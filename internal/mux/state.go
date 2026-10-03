package mux

import "time"

type queuedCommand struct {
	payload  []byte
	queuedAt time.Duration
}

type pendingSync struct {
	minimumPop int64
	deadline   time.Duration
}

// defaultScope is SET_FLOOD_SCOPE_KEY mode 0 without a key: the companion's default scope.
var defaultScope = []byte{CmdSetFloodScopeKey, 0}

// Session holds one downstream connection's command FIFO, independent inbox,
// requested protocol version, temporary flood scope, and outstanding write
// budgets. The broker owns and mutates this state.
type Session struct {
	ID int64
	// DedicatedSlotID is the dedicated listener port, or 0 for a multi-client session.
	DedicatedSlotID  int
	commands         []queuedCommand
	inbox            [][]byte
	writes           map[int64]time.Duration
	targetVersion    byte
	scope            []byte
	sync             *pendingSync
	availabilityHint int64 // write ID of the outstanding MSG_WAITING, 0 = none
}

func newSession(id int64, slotID int) *Session {
	return &Session{ID: id, DedicatedSlotID: slotID, writes: map[int64]time.Duration{}, scope: dup(defaultScope)}
}

// Dedicated reports whether this session consumes a stable dedicated slot queue.
func (s *Session) Dedicated() bool { return s.DedicatedSlotID != 0 }

// EnqueueResult describes how a dedicated offline queue absorbed a message.
type EnqueueResult int

const (
	EnqueueAdded EnqueueResult = iota
	EnqueueChannelEvicted
	EnqueueNewMessageDiscarded
)

// DedicatedClientSlot represents one port-identified client across socket
// replacements. Its offline queue survives matching-companion epochs but is
// volatile across process restart.
type DedicatedClientSlot struct {
	ID                int
	ListenPort        int
	OfflineQueue      [][]byte
	AttachedSessionID int64
	Enqueued          uint64
	Delivered         uint64
	ChannelEvictions  uint64
	NewMessageDiscard uint64
	HighWater         int
	// Changes counts every mutation of OfflineQueue so the runtime can tell
	// cheaply whether the persisted state is out of date.
	Changes uint64
}

// NewDedicatedClientSlot creates the slot for a dedicated listener port.
func NewDedicatedClientSlot(port int) *DedicatedClientSlot {
	return &DedicatedClientSlot{ID: port, ListenPort: port}
}

// EnqueueOffline mirrors companion overflow priority: at capacity, sacrifice
// the oldest channel-class frame; if none exists, discard the new arrival.
func (s *DedicatedClientSlot) EnqueueOffline(payload []byte, limit int) EnqueueResult {
	result := EnqueueAdded
	if len(s.OfflineQueue) >= limit {
		idx := -1
		for i, item := range s.OfflineQueue {
			if channelMessage(item) {
				idx = i
				break
			}
		}
		if idx < 0 {
			s.NewMessageDiscard++
			return EnqueueNewMessageDiscarded
		}
		s.OfflineQueue = append(s.OfflineQueue[:idx], s.OfflineQueue[idx+1:]...)
		s.ChannelEvictions++
		result = EnqueueChannelEvicted
	}
	s.OfflineQueue = append(s.OfflineQueue, payload)
	s.Enqueued++
	s.Changes++
	s.HighWater = max(s.HighWater, len(s.OfflineQueue))
	return result
}

// Clear discards the queue and returns the discarded count.
func (s *DedicatedClientSlot) Clear() int {
	n := len(s.OfflineQueue)
	s.OfflineQueue = nil
	s.AttachedSessionID = 0
	s.Changes++
	return n
}

// PopFront removes the oldest queued item after it was accepted for delivery.
func (s *DedicatedClientSlot) PopFront() {
	if len(s.OfflineQueue) == 0 {
		return
	}
	s.OfflineQueue = s.OfflineQueue[1:]
	s.Delivered++
	s.Changes++
}

func channelMessage(p []byte) bool {
	// CHANNEL_MSG_RECV, CHANNEL_MSG_RECV_V3, and CHANNEL_DATA_RECV are channel-class queue entries.
	return len(p) > 0 && (p[0] == RespChannelMessage || p[0] == RespChannelMessageV3 || p[0] == RespChannelData)
}

// ReceivedMessageDeduplicator remembers recently delivered logical text
// messages. The companion strips the radio retry attempt, so retries share
// sender/channel, type, timestamp, and body.
type ReceivedMessageDeduplicator struct {
	seen    map[string]struct{}
	order   []string
	changes uint64
}

const dedupCapacity = 1024

// NewReceivedMessageDeduplicator creates an empty bounded history.
func NewReceivedMessageDeduplicator() *ReceivedMessageDeduplicator {
	return &ReceivedMessageDeduplicator{seen: map[string]struct{}{}}
}

// Duplicate returns false for non-text results and first occurrences.
func (d *ReceivedMessageDeduplicator) Duplicate(payload []byte) bool {
	id, ok := ReceivedTextMessageIdentity(payload)
	if !ok {
		return false
	}
	if _, dupe := d.seen[id]; dupe {
		return true
	}
	if len(d.order) >= dedupCapacity {
		delete(d.seen, d.order[0])
		d.order = d.order[1:]
	}
	d.order = append(d.order, id)
	d.seen[id] = struct{}{}
	d.changes++
	return false
}

// History returns the remembered identities, oldest first, for persistence.
func (d *ReceivedMessageDeduplicator) History() []string {
	return append([]string(nil), d.order...)
}

// Restore replaces the history with persisted identities, keeping the newest
// entries when there are more than the capacity.
func (d *ReceivedMessageDeduplicator) Restore(ids []string) {
	d.seen = map[string]struct{}{}
	d.order = nil
	if len(ids) > dedupCapacity {
		ids = ids[len(ids)-dedupCapacity:]
	}
	for _, id := range ids {
		if _, dupe := d.seen[id]; !dupe {
			d.seen[id] = struct{}{}
			d.order = append(d.order, id)
		}
	}
}
