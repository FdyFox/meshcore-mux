package mux

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidState means a lease operation was attempted without the reservation or phase it requires.
var ErrInvalidState = errors.New("invalid lease state")

func requirePayload(p []byte, opcode byte, size int) error {
	if len(p) != size || p[0] != opcode {
		return fmt.Errorf("expected %d-byte payload %d", size, opcode)
	}
	return nil
}

// leaseDeadline keeps a lease for 25% plus one second beyond the suggested
// radio timeout, with a five-second floor.
func leaseDeadline(now time.Duration, suggestedTimeoutMs uint32) time.Duration {
	retained := int64(suggestedTimeoutMs)*5/4 + 1000
	return now + time.Duration(max(5000, retained))*time.Millisecond
}

// DmRing mirrors the companion's outstanding plain-DM acknowledgements so the
// broker cannot overwrite a live slot. The firmware table is a ring, not a
// pool, so the insertion position matters as much as the entries.
type DmRing struct {
	slots    [dmRingCapacity]*dmEntry
	nextSlot int
}

const dmRingCapacity = 8

type dmEntry struct {
	token    uint32
	deadline time.Duration
	settled  bool
}

// Available reports whether the next ring slot can be used.
func (r *DmRing) Available(now time.Duration) bool {
	e := r.slots[r.nextSlot]
	return e == nil || e.settled || now >= e.deadline
}

// Accepted records an actual firmware SENT response. A zero token consumes no ring position.
func (r *DmRing) Accepted(sent []byte, now time.Duration) error {
	if err := requirePayload(sent, RespSent, 10); err != nil {
		return err
	}
	token := readU32(sent, 2)
	if token == 0 {
		return nil
	}
	if !r.Available(now) {
		return fmt.Errorf("%w: next DM acknowledgement slot is occupied", ErrInvalidState)
	}
	r.slots[r.nextSlot] = &dmEntry{token: token, deadline: leaseDeadline(now, readU32(sent, 6))}
	r.nextSlot = (r.nextSlot + 1) % dmRingCapacity
	return nil
}

// Confirm settles every equal token; four-byte hashes are not unique.
func (r *DmRing) Confirm(push []byte) (bool, error) {
	if err := requirePayload(push, PushSendConfirmed, 9); err != nil {
		return false, err
	}
	token := readU32(push, 1)
	matched := false
	for _, e := range r.slots {
		if e != nil && !e.settled && e.token == token {
			e.settled = true
			matched = true
		}
	}
	return matched, nil
}

// PendingCount returns the number of live, unsettled reservations.
func (r *DmRing) PendingCount(now time.Duration) int {
	n := 0
	for _, e := range r.slots {
		if e != nil && !e.settled && now < e.deadline {
			n++
		}
	}
	return n
}

// RemoteKind identifies the kind of remote request holding the lease.
type RemoteKind int

const (
	RemoteNone RemoteKind = iota
	RemoteLogin
	RemoteStatus
	RemoteTrace
	RemoteTelemetry
	RemoteBinary
	RemotePathDiscovery
	RemoteAnonymous
)

func (k RemoteKind) String() string {
	return [...]string{"none", "login", "status", "trace", "telemetry", "binary", "path_discovery", "anonymous"}[k]
}

// RemoteLease reserves the companion's shared remote-request state for one
// downstream client and matches later radio pushes to that request.
type RemoteLease struct {
	Owner       int64 // 0 = no live owner
	Kind        RemoteKind
	Command     []byte
	deadline    time.Duration
	hasDeadline bool
	peer        []byte
	tag         uint32
	traceAuth   uint32
	tentative   bool
}

// Occupied expires the lease if due and reports whether it is still held.
func (l *RemoteLease) Occupied(now time.Duration) bool {
	l.Expire(now)
	return l.Kind != RemoteNone
}

// Tentative reports whether the reservation awaits its SENT/ERR reply.
func (l *RemoteLease) Tentative() bool { return l.tentative }

// Reserve returns false for a resource conflict.
func (l *RemoteLease) Reserve(owner int64, command []byte, now time.Duration) (bool, error) {
	if l.Occupied(now) {
		return false, nil
	}
	kind, peer, tag, auth, err := classifyRemote(command)
	if err != nil {
		return false, err
	}
	*l = RemoteLease{Owner: owner, Kind: kind, Command: dup(command), peer: peer, tag: tag, traceAuth: auth, tentative: true}
	return true, nil
}

// Accepted records the SENT response that starts the asynchronous operation.
func (l *RemoteLease) Accepted(sent []byte, now time.Duration) error {
	if l.Kind == RemoteNone || !l.tentative {
		return fmt.Errorf("%w: no tentative remote reservation", ErrInvalidState)
	}
	if err := requirePayload(sent, RespSent, 10); err != nil {
		return err
	}
	if l.Kind != RemoteTrace {
		l.tag = readU32(sent, 2)
	}
	l.deadline = leaseDeadline(now, readU32(sent, 6))
	l.hasDeadline = true
	l.tentative = false
	return nil
}

// Rejected clears the lease: an immediate firmware error means nothing was accepted.
func (l *RemoteLease) Rejected() { l.clear() }

// AcceptanceUnknown converts a tentative reservation into an ownerless lease
// after TCP failed before SENT reached the mux.
func (l *RemoteLease) AcceptanceUnknown(deadline time.Duration) {
	if l.Kind == RemoteNone || !l.tentative {
		return
	}
	l.Owner = 0
	l.deadline = deadline
	l.hasDeadline = true
	l.tentative = false
}

// Match returns the live owner for a matching result and releases the lease.
// matched is true even when the owner has disconnected (owner == 0).
func (l *RemoteLease) Match(push []byte, now time.Duration) (owner int64, matched bool) {
	l.Expire(now)
	if l.Kind == RemoteNone || l.tentative || !l.matches(push) {
		return 0, false
	}
	owner = l.Owner
	l.clear()
	return owner, true
}

// OwnerGone preserves the reservation but makes a later result undeliverable.
func (l *RemoteLease) OwnerGone(owner int64) {
	if l.Owner == owner {
		l.Owner = 0
	}
}

// Expire clears the lease once its deadline has elapsed.
func (l *RemoteLease) Expire(now time.Duration) bool {
	if !l.hasDeadline || now < l.deadline {
		return false
	}
	l.clear()
	return true
}

func classifyRemote(c []byte) (RemoteKind, []byte, uint32, uint32, error) {
	if len(c) == 0 {
		return 0, nil, 0, 0, errors.New("empty remote command")
	}
	peerAt := func(off int) ([]byte, error) {
		if len(c) < off+6 {
			return nil, errors.New("short remote command")
		}
		// Remote result pushes identify peers by a six-byte public-key prefix.
		return dup(c[off : off+6]), nil
	}
	var kind RemoteKind
	off := 1
	switch c[0] {
	case CmdSendLogin:
		kind = RemoteLogin
	case CmdSendStatusReq:
		kind = RemoteStatus
	case CmdSendTracePath:
		if len(c) < 11 {
			return 0, nil, 0, 0, errors.New("short trace command")
		}
		return RemoteTrace, nil, readU32(c, 1), readU32(c, 5), nil
	case CmdSendTelemetryReq:
		if len(c) == 4 {
			return 0, nil, 0, 0, errors.New("self telemetry does not use a remote lease")
		}
		kind, off = RemoteTelemetry, 4
	case CmdSendBinaryReq:
		kind = RemoteBinary
	case CmdSendPathDiscoveryReq:
		kind, off = RemotePathDiscovery, 2
	case CmdSendAnonReq:
		kind = RemoteAnonymous
	default:
		return 0, nil, 0, 0, errors.New("command does not use the remote lease")
	}
	peer, err := peerAt(off)
	return kind, peer, 0, 0, err
}

func (l *RemoteLease) matches(push []byte) bool {
	if len(push) == 0 {
		return false
	}
	peerMatch := func() bool { return len(push) >= 8 && bytes.Equal(push[2:8], l.peer) }
	switch l.Kind {
	case RemoteLogin:
		return (push[0] == PushLoginSuccess || push[0] == PushLoginFailure) && peerMatch()
	case RemoteStatus:
		return push[0] == PushStatusResponse && peerMatch()
	case RemoteTelemetry:
		return push[0] == PushTelemetryResponse && peerMatch()
	case RemoteBinary, RemoteAnonymous:
		return push[0] == PushBinaryResponse && len(push) >= 6 && readU32(push, 2) == l.tag
	case RemotePathDiscovery:
		return push[0] == PushPathDiscoveryResponse && peerMatch()
	case RemoteTrace:
		return push[0] == PushTraceData && len(push) >= 12 && readU32(push, 4) == l.tag && readU32(push, 8) == l.traceAuth
	}
	return false
}

func (l *RemoteLease) clear() { *l = RemoteLease{} }

// CompanionRadioState owns radio work and receive history that survive
// replacement of the companion's TCP socket (for the same public key).
type CompanionRadioState struct {
	DmRing         DmRing
	Remote         RemoteLease
	Deduplicator   *ReceivedMessageDeduplicator
	uncertainTill  time.Duration
	uncertain      bool
	dmCursorUnsure bool
}

// NewCompanionRadioState returns empty radio state.
func NewCompanionRadioState() *CompanionRadioState {
	return &CompanionRadioState{Deduplicator: NewReceivedMessageDeduplicator()}
}

// Quarantined reports whether new radio work must wait out an unknown outcome.
// DM uncertainty additionally waits out every known ring reservation, because
// an unseen insertion may have moved the physical cursor.
func (s *CompanionRadioState) Quarantined(now time.Duration) bool {
	if !s.uncertain {
		return false
	}
	if now < s.uncertainTill {
		return true
	}
	if s.dmCursorUnsure && s.DmRing.PendingCount(now) > 0 {
		return true
	}
	s.uncertain = false
	s.dmCursorUnsure = false
	return false
}

// Quarantine closes radio admission for at least the given duration.
func (s *CompanionRadioState) Quarantine(now, duration time.Duration, dmCursorUncertain bool) time.Duration {
	deadline := now + duration
	if !s.uncertain || deadline > s.uncertainTill {
		s.uncertainTill = deadline
	}
	s.uncertain = true
	s.dmCursorUnsure = s.dmCursorUnsure || dmCursorUncertain
	return s.uncertainTill
}

// SigningAdmission is the result of a signing-phase admission check.
type SigningAdmission int

const (
	SigningAllowed SigningAdmission = iota
	SigningBadState
	SigningTableFull
)

// SigningLease protects the companion's single incremental signing operation
// from interleaved clients.
type SigningLease struct {
	Owner          int64
	limit          uint64
	hasLimit       bool
	acceptedBytes  uint64
	tentative      bool
	lastActivity   time.Duration
	pendingData    uint64
	hasPendingData bool
	finishPending  bool
	timeout        time.Duration
}

// NewSigningLease creates a lease with the given inactivity timeout.
func NewSigningLease(timeout time.Duration) *SigningLease { return &SigningLease{timeout: timeout} }

// Occupied expires an idle lease and reports whether it is held.
func (s *SigningLease) Occupied(now time.Duration) bool {
	s.Expire(now)
	return s.Owner != 0
}

// Start reserves the signing operation; a start from the current owner restarts it.
func (s *SigningLease) Start(owner int64, now time.Duration) bool {
	s.Expire(now)
	if s.Owner != 0 && s.Owner != owner {
		return false
	}
	*s = SigningLease{Owner: owner, tentative: true, lastActivity: now, timeout: s.timeout}
	return true
}

// AcceptedStart records the SIGN_START reply with its byte limit.
func (s *SigningLease) AcceptedStart(resp []byte, now time.Duration) error {
	if s.Owner == 0 || !s.tentative {
		return fmt.Errorf("%w: no tentative signing start", ErrInvalidState)
	}
	if err := requirePayload(resp, RespSignStart, 6); err != nil {
		return err
	}
	s.limit = uint64(readU32(resp, 2))
	s.hasLimit = true
	s.tentative = false
	s.lastActivity = now
	return nil
}

// RejectedStart clears the lease.
func (s *SigningLease) RejectedStart() { s.clear() }

// BeginData admits a SIGN_DATA chunk.
func (s *SigningLease) BeginData(owner int64, command []byte, now time.Duration) SigningAdmission {
	if !s.usableBy(owner, now) {
		return SigningBadState
	}
	n := uint64(len(command) - 1)
	if n > s.limit-s.acceptedBytes {
		return SigningTableFull
	}
	s.pendingData = n
	s.hasPendingData = true
	s.lastActivity = now
	return SigningAllowed
}

// DataResponse applies the firmware reply to a SIGN_DATA chunk.
func (s *SigningLease) DataResponse(resp []byte, now time.Duration) error {
	if !s.hasPendingData {
		return fmt.Errorf("%w: no pending signing data", ErrInvalidState)
	}
	if err := validateOkOrErr(resp, true); err != nil {
		return err
	}
	n := s.pendingData
	s.hasPendingData = false
	switch {
	case resp[0] == RespOk:
		s.acceptedBytes += n
		s.lastActivity = now
	case resp[1] == ErrBadState:
		s.clear()
	default:
		s.lastActivity = now
	}
	return nil
}

// BeginFinish admits a SIGN_FINISH.
func (s *SigningLease) BeginFinish(owner int64, now time.Duration) SigningAdmission {
	if !s.usableBy(owner, now) {
		return SigningBadState
	}
	s.finishPending = true
	s.lastActivity = now
	return SigningAllowed
}

// FinishResponse applies the firmware reply to SIGN_FINISH.
func (s *SigningLease) FinishResponse(resp []byte, now time.Duration) error {
	if !s.finishPending {
		return fmt.Errorf("%w: no pending signing finish", ErrInvalidState)
	}
	if len(resp) > 0 && resp[0] == RespSignature {
		if err := requirePayload(resp, RespSignature, 65); err != nil {
			return err
		}
		s.clear()
		return nil
	}
	if err := validateOkOrErr(resp, false); err != nil {
		return err
	}
	s.finishPending = false
	if resp[1] == ErrBadState {
		s.clear()
	} else {
		s.lastActivity = now
	}
	return nil
}

// OwnerGone abandons the lease when its owner disconnects.
func (s *SigningLease) OwnerGone(owner int64) {
	if s.Owner == owner {
		s.clear()
	}
}

// Expire clears an inactive lease.
func (s *SigningLease) Expire(now time.Duration) bool {
	if s.Owner == 0 || now < s.lastActivity+s.timeout {
		return false
	}
	s.clear()
	return true
}

func (s *SigningLease) usableBy(owner int64, now time.Duration) bool {
	s.Expire(now)
	return s.Owner == owner && !s.tentative && s.hasLimit && !s.hasPendingData && !s.finishPending
}

func validateOkOrErr(resp []byte, allowOk bool) error {
	if allowOk && len(resp) == 1 && resp[0] == RespOk {
		return nil
	}
	if len(resp) == 2 && resp[0] == RespErr {
		return nil
	}
	return errors.New("unexpected signing response")
}

func (s *SigningLease) clear() { *s = SigningLease{timeout: s.timeout} }
