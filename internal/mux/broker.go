package mux

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Category selects how the runtime logs a Diagnostic.
type Category int

const (
	CatNormal Category = iota
	CatDebug
	CatTrace
	CatWarn
	CatError
	CatMalformed
	CatInfo
)

// Action is a side effect produced by the broker. The runtime executes
// actions after each broker event, never while ownership is changing.
type Action interface{ isAction() }

// SendFrame asks the runtime to write a payload to a session (0 = upstream).
type SendFrame struct {
	Session, Epoch, WriteID int64
	Payload                 []byte
}

// CloseSession asks the runtime to close a downstream connection.
type CloseSession struct {
	Session int64
	Reason  string
}

// EndEpoch tells the runtime that the current upstream epoch is over.
type EndEpoch struct {
	Epoch  int64
	Reason string
}

// Diagnostic is a log record.
type Diagnostic struct {
	Message  string
	Category Category
}

func (SendFrame) isAction()    {}
func (CloseSession) isAction() {}
func (EndEpoch) isAction()     {}
func (Diagnostic) isAction()   {}

// Step distinguishes hidden scope setup/restoration and maintenance result
// delivery from the client's ordinary command reply.
type Step int

const (
	StepCommand Step = iota
	StepSetup
	StepRestore
	StepMaintenanceResult
)

func (s Step) String() string {
	return [...]string{"command", "setup", "restore", "maintenance_result"}[s]
}

// Transaction tracks the sole in-flight upstream command, its grammar, and its owner.
type Transaction struct {
	Owner                  int64
	Command                []byte
	Descriptor             *CommandDescriptor
	Started, Progress      time.Duration
	ContactsStarted        bool
	PopSequence            int64
	NotificationGeneration int64
	HadMultiClient         bool
	Step                   Step
	Scoped                 bool
	Maintenance            bool
	UpstreamWriteID        int64 // 0 = no write pending
	MaintenanceWriteID     int64
	JobID                  int64
	ResponseFrames         int
}

type counters struct {
	names  []string
	values map[string]uint64
}

func (c *counters) add(name string, n uint64) {
	if c.values == nil {
		c.values = map[string]uint64{}
	}
	if _, ok := c.values[name]; !ok {
		c.names = append(c.names, name)
	}
	c.values[name] += n
}

func (c *counters) inc(name string)        { c.add(name, 1) }
func (c *counters) get(name string) uint64 { return c.values[name] }
func (c *counters) summary() string {
	parts := make([]string, 0, len(c.names))
	for _, n := range c.names {
		parts = append(parts, fmt.Sprintf("%s=%d", n, c.values[n]))
	}
	return strings.Join(parts, " ")
}

// Broker owns all mutable protocol state and schedules clients onto one
// companion connection. It performs no I/O; the runtime delivers socket events
// and executes the returned actions. It must only be used from one goroutine.
type Broker struct {
	Epoch        int64
	selfKey      []byte
	cfg          Config
	sessions     map[int64]*Session
	sessionIDs   []int64 // insertion order, for deterministic iteration
	active       *Transaction
	responseDebt *Transaction
	actions      []Action
	failed       bool
	orphan       []byte
	slots        map[int]*DedicatedClientSlot
	slotOrder    []*DedicatedClientSlot
	radio        *CompanionRadioState
	signing      *SigningLease

	order                  []int64 // round-robin order; 0 is the internal inbox consumer
	nextWrite              int64
	upstreamWrites         map[int64]time.Duration
	popSequence            int64
	notificationGeneration int64
	drainRequested         bool
	drainAuthorized        bool
	lastPoll               time.Duration
	now                    time.Duration
	counters               counters
	lastUnknownLog         *time.Duration
	lastOrphanLog          *time.Duration
	lastOverflowLog        map[int]time.Duration
	suppressedOverflow     map[int]uint64
}

// NewBroker creates the broker for one synchronized upstream epoch.
func NewBroker(epoch int64, selfKey []byte, cfg Config, now time.Duration, orphan []byte,
	slots []*DedicatedClientSlot, radio *CompanionRadioState) *Broker {
	if radio == nil {
		radio = NewCompanionRadioState()
	}
	b := &Broker{
		Epoch: epoch, selfKey: selfKey, cfg: cfg, sessions: map[int64]*Session{},
		orphan: orphan, slots: map[int]*DedicatedClientSlot{}, slotOrder: slots, radio: radio,
		signing: NewSigningLease(cfg.SigningTimeout), order: []int64{0},
		upstreamWrites: map[int64]time.Duration{}, lastPoll: now,
		lastOverflowLog: map[int]time.Duration{}, suppressedOverflow: map[int]uint64{},
	}
	for _, s := range slots {
		b.slots[s.ID] = s
	}
	return b
}

// Failed reports whether the epoch has ended.
func (b *Broker) Failed() bool { return b.failed }

// Orphan returns an unfanned inbox item retained for the next same-identity epoch.
func (b *Broker) Orphan() []byte { return b.orphan }

// ResponseDebt returns the transaction still awaiting a reply when the epoch failed.
func (b *Broker) ResponseDebt() *Transaction { return b.responseDebt }

// Active returns the in-flight upstream transaction, if any.
func (b *Broker) Active() *Transaction { return b.active }

// Session returns a live session by ID.
func (b *Broker) Session(id int64) *Session { return b.sessions[id] }

// TakeActions transfers pending effects to the runtime.
func (b *Broker) TakeActions() []Action {
	a := b.actions
	b.actions = nil
	return a
}

func (b *Broker) act(a Action) { b.actions = append(b.actions, a) }

func (b *Broker) diag(cat Category, format string, args ...any) {
	b.act(Diagnostic{fmt.Sprintf(format, args...), cat})
}

func (b *Broker) liveSessions() []*Session {
	out := make([]*Session, 0, len(b.sessionIDs))
	for _, id := range b.sessionIDs {
		out = append(out, b.sessions[id])
	}
	return out
}

func slotLabel(id int) string {
	if id == 0 {
		return "none"
	}
	return strconv.Itoa(id)
}

func ownerLabel(id int64) string {
	if id == 0 {
		return "none"
	}
	return strconv.FormatInt(id, 10)
}

func ms(d time.Duration) string {
	v := math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// Admit creates an independent client view and includes it in round-robin
// scheduling. Downstream IDs must be positive; slotID 0 means multi-client.
func (b *Broker) Admit(id int64, now time.Duration, slotID int) {
	b.now = now
	if b.failed || (b.active != nil && b.active.Maintenance) {
		b.act(CloseSession{id, "upstream unavailable"})
		return
	}
	if slotID != 0 {
		slot := b.slots[slotID]
		if slot == nil {
			panic("unknown dedicated client slot")
		}
		if old := slot.AttachedSessionID; old != 0 {
			b.remove(old, "replaced by new dedicated client connection", CatInfo)
		}
		slot.AttachedSessionID = id
	}
	s := newSession(id, slotID)
	b.sessions[id] = s
	b.sessionIDs = append(b.sessionIDs, id)
	b.counters.inc("connections")
	b.order = append(b.order, id)
	b.diag(CatNormal, "event=session.admitted epoch=%d session=%d dedicated_slot_id=%s sessions=%d",
		b.Epoch, id, slotLabel(slotID), len(b.sessions))
	if slotID != 0 {
		b.diag(CatInfo, "event=dedicated_client.attached epoch=%d session=%d dedicated_slot_id=%d queue_depth=%d",
			b.Epoch, id, slotID, len(b.slots[slotID].OfflineQueue))
	}
	if slotID == 0 && b.orphan != nil {
		item := b.orphan
		b.orphan = nil
		b.enqueueMultiClient(s, item)
	}
	// Admission never destructively reads the companion. A false-positive
	// MSG_WAITING is compatible with native clients and prompts an explicit sync.
	if s := b.sessions[id]; s != nil {
		b.emitAvailabilityHint(s)
	}
	b.schedule(now)
}

// ClientFrame queues a complete downstream command with a bounded backlog.
func (b *Broker) ClientFrame(id int64, payload []byte, now time.Duration) {
	b.now = now
	if b.failed {
		return
	}
	s := b.sessions[id]
	if s == nil {
		return
	}
	b.counters.inc("commands")
	b.diag(CatNormal, "event=command.received epoch=%d session=%d %s queued_commands=%d",
		b.Epoch, id, DescribeCommand(payload, false), len(s.commands))
	b.diag(CatDebug, "event=command.payload epoch=%d session=%d %s", b.Epoch, id, DescribeCommand(payload, true))
	inflight := len(s.commands)
	if s.sync != nil {
		inflight++
	}
	if b.active != nil && b.active.Owner == id {
		inflight++
	}
	if inflight >= b.cfg.CommandLimit {
		// A native client waits for each reply, so this indicates a broken or
		// hostile client and must be visible at the default log level.
		b.remove(id, fmt.Sprintf("command queue overflow limit=%d", b.cfg.CommandLimit), CatError)
	} else {
		s.commands = append(s.commands, queuedCommand{dup(payload), now})
	}
	b.schedule(now)
}

// ClientClosed removes a session. An active command remains owned until its
// real terminator so no subsequent command can inherit its replies.
func (b *Broker) ClientClosed(id int64, now time.Duration, reason string, cat Category) {
	b.now = now
	b.remove(id, reason, cat)
	b.schedule(now)
}

// Written releases output budgets for completions from this epoch only.
func (b *Broker) Written(id, epoch, writeID int64, now time.Duration) {
	if epoch != b.Epoch || b.failed {
		return
	}
	if id == 0 {
		delete(b.upstreamWrites, writeID)
		if t := b.active; t != nil && t.UpstreamWriteID == writeID {
			// Response time starts when the matching write actually completes.
			t.UpstreamWriteID = 0
			t.Progress = now
			if t.Step == StepCommand && t.Descriptor.Grammar == GrammarContacts && !t.ContactsStarted {
				t.Started = now
			}
		}
	} else if s := b.sessions[id]; s != nil {
		delete(s.writes, writeID)
		if s.availabilityHint == writeID {
			s.availabilityHint = 0
		}
		if t := b.active; t != nil && t.Maintenance && t.Step == StepMaintenanceResult &&
			t.Owner == id && t.MaintenanceWriteID == writeID {
			b.finishTransaction(t)
		}
	}
	b.schedule(now)
}

// WriteFailed handles a writer failure; an upstream failure ends the epoch.
func (b *Broker) WriteFailed(id, epoch int64, reason string, now time.Duration) {
	if epoch != b.Epoch || b.failed {
		return
	}
	b.now = now
	if id == 0 {
		b.FailEpoch("upstream write failed: " + reason)
		return
	}
	b.remove(id, "writer failed: "+reason, CatError)
	b.schedule(now)
}

// UpstreamFrame routes one companion payload.
func (b *Broker) UpstreamFrame(payload []byte, now time.Duration) {
	b.now = now
	if b.failed || !b.checkActiveDeadline(now) {
		return
	}
	b.counters.inc("upstream_frames")
	err := ValidateResponseShape(payload)
	if err == nil {
		if payload[0] >= PushAdvert {
			err = b.push(payload, now)
		} else {
			err = b.transactionResponse(payload, now)
		}
	}
	if err != nil {
		b.FailEpoch(err.Error())
		return
	}
	b.schedule(now)
}

// DrainUncertainFrame consumes frames on the old connection after the epoch
// failed with an ordinary response still outstanding, so that a late reply
// cannot acquire an owner in a new epoch. Returns true once the debt is clear.
func (b *Broker) DrainUncertainFrame(payload []byte, now time.Duration) (bool, error) {
	t := b.responseDebt
	if t == nil {
		return true, nil
	}
	if err := ValidateResponseShape(payload); err != nil {
		return false, err
	}
	if payload[0] >= PushAdvert {
		return false, b.push(payload, now)
	}
	if t.Step == StepSetup || t.Step == StepRestore {
		if !isOkOrErr(payload) {
			return false, protoErr("unexpected poisoned scope response")
		}
		b.logResponse(t, payload, now)
		b.responseDebt = nil
		return true, nil
	}
	complete, err := ValidateResponse(t.Descriptor, payload, t.Command, phase(t))
	if err != nil {
		return false, err
	}
	t.ResponseFrames++
	b.logResponse(t, payload, now)
	if t.Descriptor.Grammar == GrammarContacts && payload[0] != RespErr {
		if payload[0] == RespContactsStart {
			if t.ContactsStarted {
				return false, protoErr("duplicate contacts start in poisoned drain")
			}
			t.ContactsStarted = true
		} else if payload[0] != RespEndOfContacts && !t.ContactsStarted {
			return false, protoErr("contacts record before start in poisoned drain")
		}
	}
	if complete {
		// A hidden physical inbox pop is destructive: preserve its result.
		if t.Owner == 0 && payload[0] != RespErr {
			b.popResult(t, payload)
		}
		b.responseDebt = nil
		return true, nil
	}
	return false, nil
}

func phase(t *Transaction) int {
	if t.ContactsStarted {
		return 1
	}
	return 0
}

func isOkOrErr(p []byte) bool {
	return (len(p) == 1 && p[0] == RespOk) || (len(p) == 2 && p[0] == RespErr)
}

// Tick expires client waits and radio reservations. Polling only reminds
// clients to sync; it never authorizes a destructive physical inbox pop.
func (b *Broker) Tick(now time.Duration) {
	b.now = now
	if b.failed || !b.checkActiveDeadline(now) {
		return
	}
	for _, d := range b.upstreamWrites {
		if now >= d {
			b.FailEpoch("upstream write deadline")
			return
		}
	}
	for _, s := range b.liveSessions() {
		expired := false
		for _, d := range s.writes {
			if now >= d {
				expired = true
				break
			}
		}
		if expired {
			b.remove(s.ID, fmt.Sprintf("output write deadline: client did not accept data within %s", b.cfg.WriteTimeout), CatError)
			continue
		}
		if s.sync != nil && now >= s.sync.deadline {
			s.sync = nil
			b.reject(s.ID, []byte{CmdSyncNextMessage}, ErrBadState, "no inbox result within the virtual sync timeout")
		}
	}
	remote := &b.radio.Remote
	if remote.Command != nil {
		cmd, owner, kind := remote.Command, remote.Owner, remote.Kind
		if remote.Expire(now) {
			b.diag(CatInfo, "event=remote_lease.expired epoch=%d session=%s kind=%s %s",
				b.Epoch, ownerLabel(owner), kind, DescribeCommand(cmd, false))
		}
	}
	if b.active == nil || !b.active.Descriptor.Has(FlagSigning) {
		owner := b.signing.Owner
		if b.signing.Expire(now) {
			b.diag(CatInfo, "event=signing_lease.expired epoch=%d session=%s", b.Epoch, ownerLabel(owner))
		}
	}
	if len(b.sessions) > 0 && now-b.lastPoll >= b.cfg.PollInterval {
		b.lastPoll = now
		b.notifyAllSessions()
	}
	b.schedule(now)
}

// FailEpoch disconnects every client and never replays a possibly executed command.
func (b *Broker) FailEpoch(reason string) {
	if b.failed {
		return
	}
	b.preserveRadioUncertainty()
	if t := b.active; t != nil && t.Step != StepMaintenanceResult && b.responseDebt == nil {
		b.responseDebt = t
	}
	b.failed = true
	b.diag(epochEndCategory(reason), "event=upstream.epoch_ended epoch=%d reason=%s", b.Epoch, strconv.Quote(reason))
	for _, id := range slices.Clone(b.sessionIDs) {
		b.remove(id, "upstream epoch failed", CatNormal)
	}
	b.active = nil
	clear(b.upstreamWrites)
	b.diag(CatInfo, "event=upstream.epoch_summary epoch=%d %s", b.Epoch, b.counters.summary())
	b.act(EndEpoch{b.Epoch, reason})
}

func (b *Broker) authorizeDrain() {
	// Only a downstream sync against an empty local queue authorizes physical inbox custody.
	b.drainAuthorized = true
	b.drainRequested = true
	b.notificationGeneration++
	b.diag(CatDebug, "event=inbox.drain_requested epoch=%d generation=%d", b.Epoch, b.notificationGeneration)
}

func (b *Broker) preserveRadioUncertainty() {
	// A command-step transport failure may occur after firmware executed the
	// write but before its reply arrived.
	t := b.active
	if t == nil {
		return
	}
	remote := t.Descriptor.Has(FlagRemoteLease)
	if t.Step == StepSetup {
		if remote {
			b.radio.Remote.Rejected()
		}
		return
	}
	if t.Step != StepCommand {
		return
	}
	plain := PlainDM(t.Command)
	if !remote && !plain {
		return
	}
	deadline := b.radio.Quarantine(b.now, b.cfg.RadioUncertaintyTimeout, plain)
	if remote && b.radio.Remote.Tentative() {
		b.radio.Remote.AcceptanceUnknown(deadline)
	}
	b.diag(CatWarn, "event=radio_state.quarantined epoch=%d minimum_until_ms=%d dm_cursor_uncertain=%t %s",
		b.Epoch, deadline.Milliseconds(), plain, DescribeCommand(t.Command, false))
}

func (b *Broker) recordAvailabilityHint() {
	b.notificationGeneration++
	if b.drainAuthorized {
		b.drainRequested = true
	}
	b.notifyAllSessions()
}

func (b *Broker) notifyAllSessions() {
	for _, s := range b.liveSessions() {
		if b.sessions[s.ID] != nil {
			b.emitAvailabilityHint(s)
		}
	}
}

func (b *Broker) emitAvailabilityHint(s *Session) {
	// Coalesce hints while one MSG_WAITING write is outstanding.
	if s.availabilityHint != 0 {
		return
	}
	id, _ := b.emit(s.ID, []byte{PushMsgWaiting})
	s.availabilityHint = id
}

func (b *Broker) checkActiveDeadline(now time.Duration) bool {
	t := b.active
	if t == nil || t.Step == StepMaintenanceResult || t.UpstreamWriteID != 0 {
		return true
	}
	if now-t.Progress >= b.cfg.ResponseTimeout ||
		(t.Descriptor.Grammar == GrammarContacts && now-t.Started >= b.cfg.ContactsTimeout) {
		b.responseDebt = t
		b.FailEpoch(fmt.Sprintf("uncertain response timeout command=%s opcode=%d owner=%d step=%s "+
			"progress_elapsed_ms=%s response_timeout_ms=%s total_elapsed_ms=%s contacts_timeout_ms=%s "+
			"response_frames=%d contacts_started=%t write_pending=false",
			t.Descriptor.Name, t.Command[0], t.Owner, t.Step, ms(now-t.Progress), ms(b.cfg.ResponseTimeout),
			ms(now-t.Started), ms(b.cfg.ContactsTimeout), t.ResponseFrames, t.ContactsStarted))
		return false
	}
	return true
}

func (b *Broker) remove(id int64, reason string, cat Category) {
	s := b.sessions[id]
	if s == nil {
		return
	}
	delete(b.sessions, id)
	b.sessionIDs = slices.DeleteFunc(b.sessionIDs, func(x int64) bool { return x == id })
	b.counters.inc("disconnections")
	if s.Dedicated() {
		if slot := b.slots[s.DedicatedSlotID]; slot != nil {
			if slot.AttachedSessionID == id {
				slot.AttachedSessionID = 0
			}
			b.diag(CatInfo, "event=dedicated_client.detached epoch=%d session=%d dedicated_slot_id=%d queue_depth=%d",
				b.Epoch, id, s.DedicatedSlotID, len(slot.OfflineQueue))
		}
	} else {
		b.counters.add("discarded_inbox_items", uint64(len(s.inbox)))
	}
	b.radio.Remote.OwnerGone(id)
	// Delay abandonment of signing state until an in-flight reply has been classified.
	if b.active == nil || b.active.Owner != id {
		b.signing.OwnerGone(id)
	}
	b.order = slices.DeleteFunc(b.order, func(x int64) bool { return x == id })
	b.diag(cat, "event=session.closed epoch=%d session=%d dedicated_slot_id=%s reason=%s inbox_items=%d queued_commands=%d",
		b.Epoch, id, slotLabel(s.DedicatedSlotID), strconv.Quote(reason), len(s.inbox), len(s.commands))
	b.act(CloseSession{id, reason})
	if len(b.sessions) == 0 {
		b.drainAuthorized = false
		b.drainRequested = false
	}
	if t := b.active; t != nil && t.Maintenance && t.Step == StepMaintenanceResult && t.Owner == id {
		b.finishTransaction(t)
	}
}

// emit requests a bounded downstream write and returns its ID, or false when
// the client is gone or too slow.
func (b *Broker) emit(id int64, payload []byte) (int64, bool) {
	s := b.sessions[id]
	if s == nil {
		return 0, false
	}
	if len(s.writes) >= b.cfg.OutputFrames {
		b.remove(id, fmt.Sprintf("output queue overflow limit=%d: client is not reading its responses", b.cfg.OutputFrames), CatError)
		return 0, false
	}
	b.nextWrite++
	s.writes[b.nextWrite] = b.now + b.cfg.WriteTimeout
	b.act(SendFrame{id, b.Epoch, b.nextWrite, payload})
	return b.nextWrite, true
}

// errorName renders a native ERR reason for logs.
func errorName(reason byte) string {
	switch reason {
	case ErrUnsupportedCmd:
		return "UNSUPPORTED_CMD"
	case ErrTableFull:
		return "TABLE_FULL"
	case ErrBadState:
		return "BAD_STATE"
	case ErrIllegalArg:
		return "ILLEGAL_ARG"
	}
	return strconv.Itoa(int(reason))
}

// epochEndCategory keeps planned shutdowns quiet and makes real failures visible.
// Upstream EOF and write failures are already reported by the runtime.
func epochEndCategory(reason string) Category {
	switch {
	case reason == "process shutdown", reason == "maintenance operation completed",
		strings.HasPrefix(reason, "upstream closed"), strings.HasPrefix(reason, "upstream write failed"):
		return CatInfo
	}
	return CatError
}

func (b *Broker) logRejection(id int64, command []byte, result, detail string) {
	b.counters.inc("rejections")
	slot := 0
	if s := b.sessions[id]; s != nil {
		slot = s.DedicatedSlotID
	}
	b.diag(CatWarn, "event=command.rejected epoch=%d session=%d dedicated_slot_id=%s command=%s result=%s reason=%s",
		b.Epoch, id, slotLabel(slot), descriptorName(command), result, strconv.Quote(detail))
}

func (b *Broker) reject(id int64, command []byte, reason byte, detail string) {
	// Native ERR plus its reason byte; this rejection never goes upstream.
	b.logRejection(id, command, "ERR_"+errorName(reason), detail)
	b.emit(id, []byte{RespErr, reason})
}

func (b *Broker) schedule(now time.Duration) {
	// Resolve locally virtualized commands first, respecting each client's FIFO,
	// then rotate among clients and the internal inbox consumer.
	if b.failed {
		return
	}
	b.now = now
	if b.active != nil && b.active.Maintenance {
		return
	}
	for _, s := range b.liveSessions() {
		if b.sessions[s.ID] == nil || s.sync != nil || (b.active != nil && b.active.Owner == s.ID) {
			continue
		}
		for len(s.commands) > 0 {
			c := s.commands[0]
			_, reason := ValidateCommand(c.payload)
			op := c.payload[0]
			handled := true
			switch {
			case reason != 0:
				s.commands = s.commands[1:]
				b.reject(s.ID, c.payload, reason, fmt.Sprintf("invalid or unsupported command opcode=%d bytes=%d", op, len(c.payload)))
			case now-c.queuedAt >= b.cfg.CommandAge:
				s.commands = s.commands[1:]
				b.reject(s.ID, c.payload, ErrBadState, fmt.Sprintf("command waited longer than %s for the companion", b.cfg.CommandAge))
			case op == CmdSyncNextMessage:
				s.commands = s.commands[1:]
				if len(b.inboxFor(s)) == 0 {
					s.sync = &pendingSync{b.popSequence + 1, now + b.cfg.VirtualSyncTimeout}
					b.authorizeDrain()
					handled = false
				} else {
					b.deliverItem(s)
				}
			case op == CmdSetFloodScopeKey:
				s.commands = s.commands[1:]
				s.scope = c.payload
				b.emit(s.ID, []byte{RespOk})
			case op == CmdExportPrivateKey && !b.cfg.PrivateKeyExport:
				s.commands = s.commands[1:]
				b.logRejection(s.ID, c.payload, "DISABLED", "private-key export disabled by configuration")
				b.emit(s.ID, []byte{RespDisabled})
			case op == CmdImportPrivateKey && !b.cfg.PrivateKeyImport:
				s.commands = s.commands[1:]
				b.reject(s.ID, c.payload, ErrUnsupportedCmd, "private-key import disabled by configuration")
			case op == CmdFactoryReset && !b.cfg.FactoryReset:
				s.commands = s.commands[1:]
				b.reject(s.ID, c.payload, ErrUnsupportedCmd, "factory reset disabled by configuration")
			default:
				handled = false
			}
			if !handled || b.sessions[s.ID] == nil {
				break
			}
		}
	}
	if b.active != nil {
		return
	}
	for range len(b.order) {
		id := b.order[0]
		b.order = append(b.order[1:], id)
		if id == 0 {
			if b.drainRequested && len(b.sessions) > 0 {
				b.drainRequested = false
				b.popSequence++
				b.dispatch(0, []byte{CmdSyncNextMessage}, now, b.popSequence, b.notificationGeneration)
				return
			}
			continue
		}
		s := b.sessions[id]
		if s == nil || s.sync != nil || len(s.commands) == 0 {
			continue
		}
		c := s.commands[0]
		s.commands = s.commands[1:]
		if b.admitCommand(id, c.payload, now) {
			b.dispatch(id, c.payload, now, 0, 0)
			return
		}
	}
}

func (b *Broker) dispatch(owner int64, command []byte, now time.Duration, popSeq, generation int64) {
	// Claim upstream ownership before sending anything. A nondefault client
	// scope wraps the command in hidden SET_FLOOD_SCOPE_KEY setup and restoration.
	d := Descriptor(command)
	t := &Transaction{Owner: owner, Command: command, Descriptor: d, Started: now, Progress: now,
		PopSequence: popSeq, NotificationGeneration: generation}
	if owner == 0 {
		for _, s := range b.sessions {
			if !s.Dedicated() {
				t.HadMultiClient = true
				break
			}
		}
	}
	t.JobID = b.nextWrite + 1
	t.Maintenance = d.Has(FlagMaintenance)
	b.active = t
	b.diag(CatNormal, "event=command.dispatched epoch=%d session=%d job=%d %s scoped=%t",
		b.Epoch, owner, t.JobID, DescribeCommand(command, false), d.Has(FlagScopeSend))
	b.diag(CatDebug, "event=command.dispatched_payload epoch=%d session=%d job=%d %s",
		b.Epoch, owner, t.JobID, DescribeCommand(command, true))
	if s := b.sessions[owner]; s != nil && d.Has(FlagScopeSend) && !bytes.Equal(s.scope, defaultScope) {
		t.Step = StepSetup
		t.Scoped = true
		t.UpstreamWriteID = b.sendUpstream(s.scope, now)
	} else {
		b.sendCommand(t, now)
	}
}

func (b *Broker) sendCommand(t *Transaction, now time.Duration) {
	// DEVICE_QUERY is forwarded at the mux-owned app target; the client target stays in the transaction.
	t.Step = StepCommand
	forwarded := t.Command
	if forwarded[0] == CmdDeviceQuery {
		forwarded = NormalizeDeviceQuery(forwarded)
	}
	t.UpstreamWriteID = b.sendUpstream(forwarded, now)
}

func (b *Broker) sendUpstream(payload []byte, now time.Duration) int64 {
	b.nextWrite++
	b.upstreamWrites[b.nextWrite] = now + b.cfg.WriteTimeout
	b.act(SendFrame{0, b.Epoch, b.nextWrite, payload})
	b.counters.inc("upstream_commands")
	return b.nextWrite
}

func (b *Broker) admitCommand(owner int64, command []byte, now time.Duration) bool {
	// Apply permission and shared-radio-resource checks just before dispatch.
	d := Descriptor(command)
	var reason byte
	detail := ""
	remote := &b.radio.Remote
	switch {
	case d.Has(FlagMaintenance) && command[0] != CmdReboot:
		// Reboot keeps the disruptive lifecycle but has no single-client or idle-radio prerequisite.
		if len(b.sessions) != 1 || b.radio.Quarantined(now) || remote.Occupied(now) ||
			b.radio.DmRing.PendingCount(now) > 0 || b.signing.Occupied(now) {
			reason = ErrBadState
			detail = "maintenance requires exactly one connected client and no pending radio or signing work"
		}
	case PlainDM(command) && b.radio.Quarantined(now):
		reason, detail = ErrBadState, "radio quarantined after an uncertain send; retry later"
	case PlainDM(command) && !b.radio.DmRing.Available(now):
		reason, detail = ErrBadState, "all 8 direct-message acknowledgement slots are in use; retry later"
	case d.Has(FlagRemoteLease):
		ok := false
		if !b.radio.Quarantined(now) {
			var err error
			ok, err = remote.Reserve(owner, command, now)
			if err != nil {
				ok = false
			}
		}
		if ok {
			b.diag(CatNormal, "event=remote_lease.reserved epoch=%d session=%d kind=%s %s",
				b.Epoch, owner, remote.Kind, DescribeCommand(command, false))
		} else {
			reason = ErrBadState
			detail = "another remote request (login/status/telemetry/trace/...) is pending"
			if b.radio.Quarantined(now) {
				detail = "radio quarantined after an uncertain send; retry later"
			}
		}
	case command[0] == CmdSignStart:
		if !b.signing.Start(owner, now) {
			reason, detail = ErrBadState, "another client owns the signing operation"
		}
	case command[0] == CmdSignData:
		switch b.signing.BeginData(owner, command, now) {
		case SigningTableFull:
			reason, detail = ErrTableFull, "signing data exceeds the companion's limit"
		case SigningBadState:
			reason, detail = ErrBadState, "no signing operation started by this client"
		}
	case command[0] == CmdSignFinish:
		if b.signing.BeginFinish(owner, now) != SigningAllowed {
			reason, detail = ErrBadState, "no signing operation started by this client"
		}
	}
	if reason != 0 {
		b.reject(owner, command, reason, detail)
		return false
	}
	return true
}

func (b *Broker) scopeResponse(t *Transaction, payload []byte, now time.Duration) error {
	// Consume internal scope replies instead of exposing them to the client.
	if !isOkOrErr(payload) {
		return protoErr("unexpected internal scope response")
	}
	b.logResponse(t, payload, now)
	t.Progress = now
	if t.Step == StepSetup {
		if payload[0] == RespErr {
			if t.Descriptor.Has(FlagRemoteLease) {
				b.radio.Remote.Rejected()
			}
			b.emit(t.Owner, payload)
			b.finishTransaction(t)
		} else {
			b.sendCommand(t, now)
		}
		return nil
	}
	if payload[0] != RespOk {
		return protoErr("scope restoration rejected after command response")
	}
	b.finishTransaction(t)
	return nil
}

func (b *Broker) recordAcceptance(t *Transaction, payload []byte, now time.Duration) error {
	// SENT accepts a radio operation but does not prove delivery.
	c := t.Command
	remote := &b.radio.Remote
	switch {
	case PlainDM(c) && payload[0] == RespSent:
		return b.radio.DmRing.Accepted(payload, now)
	case t.Descriptor.Has(FlagRemoteLease):
		if payload[0] == RespSent {
			if err := remote.Accepted(payload, now); err != nil {
				return err
			}
			b.diag(CatNormal, "event=remote_lease.accepted epoch=%d session=%d kind=%s %s",
				b.Epoch, t.Owner, remote.Kind, DescribeResponse(payload, false))
		} else {
			remote.Rejected()
			b.diag(CatInfo, "event=remote_lease.rejected epoch=%d session=%d %s",
				b.Epoch, t.Owner, DescribeResponse(payload, false))
		}
	case c[0] == CmdSignStart:
		if payload[0] == RespSignStart {
			return b.signing.AcceptedStart(payload, now)
		}
		b.signing.RejectedStart()
	case c[0] == CmdSignData:
		return b.signing.DataResponse(payload, now)
	case c[0] == CmdSignFinish:
		return b.signing.FinishResponse(payload, now)
	}
	return nil
}

func (b *Broker) finishTransaction(t *Transaction) {
	// Release response ownership. Disruptive operations end the whole epoch.
	b.active = nil
	if b.sessions[t.Owner] == nil {
		b.signing.OwnerGone(t.Owner)
	}
	if t.Maintenance {
		b.FailEpoch("maintenance operation completed")
	}
}

func (b *Broker) transactionResponse(payload []byte, now time.Duration) error {
	// Keep each ordinary reply with its active owner, even if that client has disconnected.
	t := b.active
	if t == nil {
		return protoErr("ordinary response without owner")
	}
	if t.UpstreamWriteID != 0 {
		// A response proves the corresponding write completed even if the
		// writer's completion event has not reached the broker yet.
		delete(b.upstreamWrites, t.UpstreamWriteID)
		t.UpstreamWriteID = 0
		if t.Step == StepCommand && t.Descriptor.Grammar == GrammarContacts && !t.ContactsStarted {
			t.Started = now
		}
	}
	switch t.Step {
	case StepSetup, StepRestore:
		return b.scopeResponse(t, payload, now)
	case StepMaintenanceResult:
		return protoErr("ordinary response after maintenance result")
	}
	complete, err := ValidateResponse(t.Descriptor, payload, t.Command, phase(t))
	if err != nil {
		return err
	}
	t.ResponseFrames++
	if t.Owner != 0 || payload[0] != RespNoMoreMessages {
		b.logResponse(t, payload, now)
	}
	if t.Owner == 0 {
		if payload[0] == RespErr {
			return protoErr("internal inbox pop rejected")
		}
		b.active = nil
		b.popResult(t, payload)
		return nil
	}
	if t.Descriptor.Grammar == GrammarContacts && payload[0] != RespErr {
		if payload[0] == RespContactsStart {
			if t.ContactsStarted {
				return protoErr("duplicate contacts start")
			}
			t.ContactsStarted = true
		} else if !t.ContactsStarted {
			return protoErr("contacts record before start")
		}
	}
	var responseWrite int64
	if s := b.sessions[t.Owner]; s != nil {
		visible := payload
		if payload[0] == RespDeviceInfo {
			if t.Command[0] == CmdDeviceQuery {
				s.targetVersion = t.Command[1]
			}
			// DEVICE_INFO is a capability view of the mux.
			if visible, err = DownstreamDeviceInfo(payload); err != nil {
				return err
			}
		}
		responseWrite, _ = b.emit(s.ID, visible)
	}
	t.Progress = now
	if complete {
		if err := b.recordAcceptance(t, payload, now); err != nil {
			return err
		}
		switch {
		case t.Scoped:
			t.Step = StepRestore
			t.UpstreamWriteID = b.sendUpstream(dup(defaultScope), now)
		case t.Maintenance && responseWrite != 0:
			// End the epoch only after the writer confirms the real firmware result.
			t.Step = StepMaintenanceResult
			t.MaintenanceWriteID = responseWrite
		default:
			b.finishTransaction(t)
		}
	}
	return nil
}

func (b *Broker) logResponse(t *Transaction, payload []byte, now time.Duration) {
	queued := 0
	if s := b.sessions[t.Owner]; s != nil {
		queued = len(s.commands)
	}
	b.diag(CatNormal, "event=command.response epoch=%d session=%d job=%d command=%s command_bytes=%d step=%s %s response_frames=%d queued_commands=%d elapsed_ms=%s",
		b.Epoch, t.Owner, t.JobID, t.Descriptor.Name, len(t.Command), t.Step, DescribeResponse(payload, false),
		t.ResponseFrames, queued, ms(now-t.Started))
	b.diag(CatDebug, "event=response.payload epoch=%d session=%d job=%d %s",
		b.Epoch, t.Owner, t.JobID, DescribeResponse(payload, true))
}

func (b *Broker) rateLimited(last **time.Duration, now time.Duration) bool {
	if *last != nil && now-**last < time.Second {
		return true
	}
	n := now
	*last = &n
	return false
}

func (b *Broker) broadcast(payload []byte) {
	for _, id := range slices.Clone(b.sessionIDs) {
		b.emit(id, payload)
	}
}

func (b *Broker) push(payload []byte, now time.Duration) error {
	// MSG_WAITING prompts downstream sync without taking custody; SEND_CONFIRMED
	// settles the DM ring and is broadcast; remote results go to their lease owner.
	switch payload[0] {
	case PushMsgWaiting:
		b.diag(CatNormal, "event=push.received epoch=%d route=inbox_hint %s", b.Epoch, DescribeResponse(payload, false))
		b.recordAvailabilityHint()
	case PushSendConfirmed:
		matched, err := b.radio.DmRing.Confirm(payload)
		if err != nil {
			return err
		}
		b.diag(CatNormal, "event=push.received epoch=%d route=broadcast matched=%t sessions=%d %s",
			b.Epoch, matched, len(b.sessions), DescribeResponse(payload, false))
		b.diag(CatDebug, "event=push.payload epoch=%d %s", b.Epoch, DescribeResponse(payload, true))
		b.broadcast(payload)
	case PushLoginSuccess, PushLoginFailure, PushStatusResponse, PushTraceData,
		PushTelemetryResponse, PushBinaryResponse, PushPathDiscoveryResponse:
		// Self telemetry is exceptional: its push-shaped reply completes the
		// active four-byte SEND_TELEMETRY_REQ, matched by the self-key prefix.
		if t := b.active; payload[0] == PushTelemetryResponse && t != nil && t.Command[0] == CmdSendTelemetryReq &&
			len(t.Command) == 4 && len(b.selfKey) >= 6 && bytes.Equal(payload[2:8], b.selfKey[:6]) {
			return b.transactionResponse(payload, now)
		}
		remote := &b.radio.Remote
		leaseOwner, leaseKind := remote.Owner, remote.Kind
		owner, _ := remote.Match(payload, now)
		if owner != 0 {
			b.diag(CatNormal, "event=push.received epoch=%d route=lease session=%d kind=%s %s",
				b.Epoch, owner, leaseKind, DescribeResponse(payload, false))
			b.diag(CatDebug, "event=push.payload epoch=%d session=%d %s", b.Epoch, owner, DescribeResponse(payload, true))
			b.emit(owner, payload)
		} else {
			b.counters.inc("orphan_remote")
			if !b.rateLimited(&b.lastOrphanLog, now) {
				b.diag(CatInfo, "event=push.orphan_remote epoch=%d expected_session=%s expected_kind=%s %s count=%d",
					b.Epoch, ownerLabel(leaseOwner), leaseKind, DescribeResponse(payload, false), b.counters.get("orphan_remote"))
			}
		}
	default:
		switch payload[0] {
		case PushAdvert, PushPathUpdated, PushRawData, PushLogRxData, PushNewAdvert,
			PushControlData, PushContactDeleted, PushContactsFull:
			b.diag(CatNormal, "event=push.received epoch=%d route=broadcast sessions=%d %s",
				b.Epoch, len(b.sessions), DescribeResponse(payload, false))
			b.diag(CatDebug, "event=push.payload epoch=%d %s", b.Epoch, DescribeResponse(payload, true))
		default:
			b.counters.inc("unknown_pushes")
			if !b.rateLimited(&b.lastUnknownLog, now) {
				b.diag(CatWarn, "event=push.received epoch=%d route=broadcast unknown_push code=%d sessions=%d count=%d",
					b.Epoch, payload[0], len(b.sessions), b.counters.get("unknown_pushes"))
			}
		}
		b.broadcast(payload)
	}
	return nil
}

func (b *Broker) popResult(t *Transaction, payload []byte) {
	// Turn one physical inbox result into independent client copies.
	// NO_MORE_MESSAGES only satisfies waits old enough to be covered by this pop.
	if payload[0] == RespNoMoreMessages {
		newerWork := b.notificationGeneration > t.NotificationGeneration
		for _, s := range b.liveSessions() {
			if s.sync != nil && s.sync.minimumPop <= t.PopSequence {
				s.sync = nil
				b.emit(s.ID, []byte{RespNoMoreMessages})
			}
		}
		for _, s := range b.sessions {
			if s.sync != nil {
				newerWork = true
			}
		}
		if newerWork && len(b.sessions) > 0 {
			b.drainAuthorized = true
			b.drainRequested = true
		} else {
			b.drainAuthorized = false
			b.drainRequested = false
		}
		return
	}
	b.counters.inc("inbox_pops")
	if b.cfg.DeduplicateReceivedMessages && b.radio.Deduplicator.Duplicate(payload) {
		// The duplicate has still been removed from the physical inbox; keep
		// draining for the pending client sync.
		b.counters.inc("duplicate_received_messages")
		b.diag(CatDebug, "event=inbox.duplicate_discarded epoch=%d count=%d %s",
			b.Epoch, b.counters.get("duplicate_received_messages"), DescribeResponse(payload, false))
		b.drainRequested = b.drainAuthorized && len(b.sessions) > 0
		return
	}
	b.fanOutDedicated(payload)
	var multi []*Session
	for _, s := range b.liveSessions() {
		if !s.Dedicated() {
			multi = append(multi, s)
		}
	}
	if len(multi) == 0 && t.HadMultiClient {
		b.orphan = payload
	} else {
		for _, s := range multi {
			if b.sessions[s.ID] != nil {
				b.enqueueMultiClient(s, payload)
			}
		}
	}
	b.drainRequested = b.drainAuthorized && len(b.sessions) > 0
}

func (b *Broker) fanOutDedicated(payload []byte) {
	// Every configured dedicated client receives a copy, including detached ones.
	for _, slot := range b.slotOrder {
		wasEmpty := len(slot.OfflineQueue) == 0
		prevHigh := slot.HighWater
		switch slot.EnqueueOffline(payload, b.cfg.OfflineQueueSize) {
		case EnqueueChannelEvicted:
			b.warnDedicatedOverflow(slot, "oldest_channel_message_evicted")
		case EnqueueNewMessageDiscarded:
			b.warnDedicatedOverflow(slot, "new_direct_message_discarded")
			continue
		}
		if slot.HighWater > prevHigh {
			b.diag(CatDebug, "event=dedicated_queue.high_water epoch=%d dedicated_slot_id=%d depth=%d",
				b.Epoch, slot.ID, slot.HighWater)
		}
		if s := b.sessions[slot.AttachedSessionID]; slot.AttachedSessionID != 0 && s != nil {
			if s.sync != nil {
				s.sync = nil
				b.deliverItem(s)
			} else if wasEmpty {
				b.emitAvailabilityHint(s)
			}
		}
	}
}

func (b *Broker) warnDedicatedOverflow(slot *DedicatedClientSlot, event string) {
	// Rate-limit metadata-only warnings per slot.
	if last, ok := b.lastOverflowLog[slot.ID]; ok && b.now-last < time.Second {
		b.suppressedOverflow[slot.ID]++
		return
	}
	suppressed := b.suppressedOverflow[slot.ID]
	b.suppressedOverflow[slot.ID] = 0
	b.lastOverflowLog[slot.ID] = b.now
	b.diag(CatWarn, "event=dedicated_queue.%s epoch=%d dedicated_slot_id=%d depth=%d suppressed_since_last=%d",
		event, b.Epoch, slot.ID, len(slot.OfflineQueue), suppressed)
}

func (b *Broker) enqueueMultiClient(s *Session, payload []byte) {
	// Multi-client inboxes are connection-scoped; slow clients are disconnected at their bound.
	if len(s.inbox) >= b.cfg.InboxEntries {
		b.remove(s.ID, fmt.Sprintf("inbox overflow limit=%d: client is not fetching its messages", b.cfg.InboxEntries), CatError)
		return
	}
	wasEmpty := len(s.inbox) == 0
	s.inbox = append(s.inbox, payload)
	if s.sync != nil {
		s.sync = nil
		b.deliverItem(s)
	} else if wasEmpty {
		b.emitAvailabilityHint(s)
	}
}

func (b *Broker) inboxFor(s *Session) [][]byte {
	if s.Dedicated() {
		return b.slots[s.DedicatedSlotID].OfflineQueue
	}
	return s.inbox
}

func (b *Broker) deliverItem(s *Session) {
	// Remove the oldest inbox item only after its version-adjusted reply is enqueued.
	inbox := b.inboxFor(s)
	if len(inbox) == 0 {
		return
	}
	out, err := DowngradeInbox(inbox[0], s.targetVersion)
	if err != nil {
		out = inbox[0]
	}
	if _, ok := b.emit(s.ID, out); !ok {
		return
	}
	if s.Dedicated() {
		b.slots[s.DedicatedSlotID].PopFront()
	} else {
		s.inbox = s.inbox[1:]
	}
}
