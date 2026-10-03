package mux

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Persistence keeps dedicated-client offline queues, the companion identity
// they belong to, and the received-message deduplication history across
// process restarts. Multi-client inboxes are connection-scoped and are never
// persisted.
//
// Durability contract:
//   - A graceful shutdown (SIGTERM/SIGINT) saves the final state; nothing is lost.
//   - After a crash, at most the changes of the last flush interval are lost:
//     messages fetched from the companion during that window may be missing,
//     and messages delivered during that window may be delivered again.
//   - Messages still on the companion are unaffected; they are only fetched
//     when a client syncs.

const stateFileVersion = 1

// persistedState is the on-disk JSON layout. Payloads are hex-encoded native
// inbox frames exactly as received from the companion.
type persistedState struct {
	Version                int                 `json:"version"`
	SavedAt                time.Time           `json:"saved_at"`
	CompanionKey           string              `json:"companion_key"`
	DedicatedQueues        map[string][]string `json:"dedicated_queues"`
	ReceivedMessageHistory []string            `json:"received_message_history,omitempty"`
}

// stateSnapshot is an immutable copy of the persistable state. It is built by
// the coordinator goroutine and handed to the writer goroutine; queued payloads
// are never mutated after being stored, so sharing their bytes is safe.
type stateSnapshot struct {
	key    []byte
	queues map[int][][]byte
	dedup  []string
}

func (s *stateSnapshot) entries() int {
	n := 0
	for _, q := range s.queues {
		n += len(q)
	}
	return n
}

func encodeState(s *stateSnapshot) ([]byte, error) {
	ps := persistedState{
		Version:                stateFileVersion,
		SavedAt:                time.Now().UTC(),
		CompanionKey:           hex.EncodeToString(s.key),
		DedicatedQueues:        map[string][]string{},
		ReceivedMessageHistory: s.dedup,
	}
	for port, q := range s.queues {
		items := make([]string, len(q))
		for i, p := range q {
			items[i] = hex.EncodeToString(p)
		}
		ps.DedicatedQueues[strconv.Itoa(port)] = items
	}
	return json.MarshalIndent(&ps, "", "  ")
}

func decodeState(data []byte) (*stateSnapshot, error) {
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, err
	}
	if ps.Version != stateFileVersion {
		return nil, fmt.Errorf("unsupported state file version %d (expected %d)", ps.Version, stateFileVersion)
	}
	key, err := hex.DecodeString(ps.CompanionKey)
	if err != nil {
		return nil, fmt.Errorf("companion_key: %w", err)
	}
	s := &stateSnapshot{key: key, queues: map[int][][]byte{}, dedup: ps.ReceivedMessageHistory}
	for portText, items := range ps.DedicatedQueues {
		port, err := strconv.Atoi(portText)
		if err != nil {
			return nil, fmt.Errorf("dedicated_queues: invalid port %q", portText)
		}
		q := make([][]byte, 0, len(items))
		for i, item := range items {
			p, err := hex.DecodeString(item)
			if err != nil {
				return nil, fmt.Errorf("dedicated_queues[%s][%d]: %w", portText, i, err)
			}
			q = append(q, p)
		}
		s.queues[port] = q
	}
	return s, nil
}

// writeStateFile replaces path atomically: write a private temporary file in
// the same directory, fsync it, rename it over the old file, then fsync the
// directory so the rename itself survives a power loss.
func writeStateFile(path string, s *stateSnapshot) error {
	data, err := encodeState(s)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp") // created with mode 0600
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

func loadStateFile(path string) (*stateSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeState(data)
}

// statePersister writes snapshots on its own goroutine so slow storage (for
// example an SD card) never blocks the coordinator. Only the newest pending
// snapshot is kept: an older one is obsolete once a newer one exists.
type statePersister struct {
	path   string
	ch     chan *stateSnapshot
	done   chan struct{}
	errLog logLimiter
}

func newStatePersister(path string) *statePersister {
	p := &statePersister{path: path, ch: make(chan *stateSnapshot, 1), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for s := range p.ch {
			p.write(s)
		}
	}()
	return p
}

func (p *statePersister) write(s *stateSnapshot) {
	if err := writeStateFile(p.path, s); err != nil {
		if ok, suppressed := p.errLog.allow(); ok {
			Log.Errorf("event=persistence.save_failed path=%q error=%q suppressed_since_last=%d (dedicated queues will be lost on restart until this is fixed)",
				p.path, err.Error(), suppressed)
		}
		return
	}
	Log.Debugf("event=persistence.saved path=%q entries=%d", p.path, s.entries())
}

// submit hands a snapshot to the writer, replacing a pending older one. It
// never blocks; only the coordinator goroutine calls it.
func (p *statePersister) submit(s *stateSnapshot) {
	select {
	case <-p.ch:
	default:
	}
	p.ch <- s
}

// close stops the writer after it has finished any pending snapshot.
func (p *statePersister) close() {
	close(p.ch)
	<-p.done
}

// persistableInbox reports whether a payload is a valid native inbox frame
// (DM, channel message, either version, or channel data) that may be restored
// into a dedicated queue. Anything else in the file is ignored.
func persistableInbox(p []byte) bool {
	if ValidateResponseShape(p) != nil {
		return false
	}
	switch p[0] {
	case RespContactMessage, RespChannelMessage, RespContactMessageV3, RespChannelMessageV3, RespChannelData:
		return true
	}
	return false
}

// stateGeneration changes whenever persistable state changes.
func (r *Runtime) stateGeneration() uint64 {
	g := r.radio.Deduplicator.changes
	for _, s := range r.slots {
		g += s.Changes
	}
	return g
}

func (r *Runtime) snapshotState() *stateSnapshot {
	s := &stateSnapshot{key: dup(r.slotsKey), queues: map[int][][]byte{}, dedup: r.radio.Deduplicator.History()}
	for _, slot := range r.slots {
		s.queues[slot.ID] = append([][]byte(nil), slot.OfflineQueue...)
	}
	return s
}

// restoreState loads the state file at startup. A missing file is normal on
// first start; an unreadable one is moved aside so it is not overwritten and
// can be inspected, and the mux starts with empty queues.
func (r *Runtime) restoreState() {
	path := r.cfg.StateFile
	s, err := loadStateFile(path)
	if errors.Is(err, os.ErrNotExist) {
		Log.Infof("event=persistence.no_state_file path=%q (starting with empty dedicated queues)", path)
		return
	}
	if err != nil {
		aside := path + ".corrupt-" + time.Now().UTC().Format("20060102T150405Z")
		if rerr := os.Rename(path, aside); rerr != nil {
			aside = "(could not move: " + rerr.Error() + ")"
		}
		Log.Warnf("event=persistence.load_failed path=%q error=%q moved_to=%q (starting with empty dedicated queues)", path, err.Error(), aside)
		return
	}
	if len(s.key) > 0 {
		// Queues and history belong to this companion identity. A different
		// companion on the next connect clears them (prepareDedicatedSlots,
		// radioStateFor), exactly as for an in-process reconnect.
		r.slotsKey = s.key
		r.radioKey = s.key
	}
	byPort := map[int]*DedicatedClientSlot{}
	for _, slot := range r.slots {
		byPort[slot.ID] = slot
	}
	invalid, unconfigured, overflow := 0, 0, 0
	ports := make([]int, 0, len(s.queues))
	for port := range s.queues {
		ports = append(ports, port)
	}
	sort.Ints(ports)
	for _, port := range ports {
		q := s.queues[port]
		slot := byPort[port]
		if slot == nil {
			unconfigured += len(q)
			if len(q) > 0 {
				Log.Warnf("event=persistence.queue_dropped port=%d entries=%d reason=port_no_longer_configured", port, len(q))
			}
			continue
		}
		for _, p := range q {
			if !persistableInbox(p) {
				invalid++
				continue
			}
			if slot.EnqueueOffline(p, r.cfg.OfflineQueueSize) != EnqueueAdded {
				overflow++
			}
		}
	}
	if invalid > 0 {
		Log.Warnf("event=persistence.entries_ignored path=%q count=%d reason=invalid_inbox_frame", path, invalid)
	}
	if overflow > 0 {
		Log.Warnf("event=persistence.entries_dropped path=%q count=%d reason=offline_queue_size_reduced", path, overflow)
	}
	r.radio.Deduplicator.Restore(s.dedup)
	keyLabel := "unknown"
	if len(s.key) >= 6 {
		keyLabel = Hex(s.key[:6])
	}
	restored := 0
	for _, slot := range r.slots {
		restored += len(slot.OfflineQueue)
	}
	Log.Infof("event=persistence.loaded path=%q entries=%d dropped_unconfigured=%d dedup_history=%d companion_key_prefix=%s",
		path, restored, unconfigured, len(s.dedup), keyLabel)
	r.persistedGen = r.stateGeneration()
}

// maybePersist submits a snapshot when state changed and the flush interval elapsed.
func (r *Runtime) maybePersist() {
	if r.persister == nil {
		return
	}
	now := Now()
	if gen := r.stateGeneration(); (gen != r.persistedGen || r.forcePersist) && now-r.lastPersist >= r.cfg.StateFlushInterval {
		r.persister.submit(r.snapshotState())
		r.persistedGen = gen
		r.forcePersist = false
		r.lastPersist = now
	}
}

// finalPersist stops the writer and saves the final state synchronously.
func (r *Runtime) finalPersist() {
	if r.persister == nil {
		return
	}
	r.persister.close()
	s := r.snapshotState()
	if err := writeStateFile(r.cfg.StateFile, s); err != nil {
		Log.Errorf("event=persistence.save_failed path=%q error=%q (undelivered dedicated messages are lost)", r.cfg.StateFile, err.Error())
		return
	}
	Log.Infof("event=persistence.saved_on_shutdown path=%q entries=%d", r.cfg.StateFile, s.entries())
}
