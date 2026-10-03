package mux

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// Synthetic firmware fixtures; none of the keys or fields come from a real companion.

func selfInfo(identity byte) []byte {
	p := make([]byte, 58, 64)
	p[0] = RespSelfInfo
	for i := 4; i < 36; i++ {
		p[i] = identity
	}
	return append(p, "fake"...)
}

func deviceInfo(level byte) []byte {
	p := make([]byte, DeviceInfoSize)
	p[0] = RespDeviceInfo
	p[1] = level
	copy(p[20:], "FakeModel")
	copy(p[60:], "v1.0")
	return p
}

func channelMsg(text string) []byte {
	return append([]byte{RespChannelMessageV3, 0x10, 0, 0, 0, 0xff, 0, 1, 2, 3, 4}, text...)
}

type harness struct {
	t     *testing.T
	b     *Broker
	now   time.Duration
	out   map[int64][][]byte // frames sent per session (0 = upstream)
	diags []Diagnostic
}

// logged reports whether a diagnostic at cat containing substr was produced.
func (h *harness) logged(cat Category, substr string) bool {
	for _, d := range h.diags {
		if d.Category == cat && strings.Contains(d.Message, substr) {
			return true
		}
	}
	return false
}

func newHarness(t *testing.T, cfg Config, slots ...*DedicatedClientSlot) *harness {
	key := selfInfo(0xa5)[4:36]
	h := &harness{t: t, out: map[int64][][]byte{}}
	h.b = NewBroker(1, key, cfg, 0, nil, slots, nil)
	return h
}

// drain collects actions and immediately acknowledges every write, like a healthy socket.
func (h *harness) drain() {
	for {
		actions := h.b.TakeActions()
		if len(actions) == 0 {
			return
		}
		for _, a := range actions {
			switch a := a.(type) {
			case SendFrame:
				h.out[a.Session] = append(h.out[a.Session], a.Payload)
				h.b.Written(a.Session, a.Epoch, a.WriteID, h.now)
			case Diagnostic:
				h.diags = append(h.diags, a)
			}
		}
	}
}

func (h *harness) take(id int64) [][]byte {
	f := h.out[id]
	delete(h.out, id)
	return f
}

func (h *harness) admit(id int64, slot int) {
	h.b.Admit(id, h.now, slot)
	h.drain()
}

func (h *harness) client(id int64, p ...byte) {
	h.b.ClientFrame(id, p, h.now)
	h.drain()
}

func (h *harness) upstream(p ...byte) {
	h.b.UpstreamFrame(p, h.now)
	h.drain()
}

func (h *harness) expectUpstream(want ...byte) {
	h.t.Helper()
	got := h.take(0)
	if len(got) != 1 || !bytes.Equal(got[0], want) {
		h.t.Fatalf("upstream got %x, want one frame %x", got, want)
	}
}

func (h *harness) expectClient(id int64, want ...[]byte) {
	h.t.Helper()
	got := h.take(id)
	if len(got) != len(want) {
		h.t.Fatalf("session %d got %x, want %x", id, got, want)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			h.t.Fatalf("session %d frame %d got %x, want %x", id, i, got[i], want[i])
		}
	}
}

func TestResponsesGoOnlyToTheirOwnerInFIFOOrder(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.admit(1, 0)
	h.admit(2, 0)
	h.take(1)
	h.take(2) // admission MSG_WAITING hints

	h.client(1, CmdGetDeviceTime)
	h.client(2, CmdGetBattAndStorage)
	h.expectUpstream(CmdGetDeviceTime) // only one transaction at a time

	h.upstream(RespCurrentTime, 1, 2, 3, 4)
	h.expectClient(1, []byte{RespCurrentTime, 1, 2, 3, 4})
	h.expectClient(2)
	h.expectUpstream(CmdGetBattAndStorage)

	batt := append([]byte{RespBatteryAndStorage}, make([]byte, 10)...)
	h.upstream(batt...)
	h.expectClient(2, batt)
	h.expectClient(1)
}

func TestInboxIsFetchedOnceAndCopiedToEveryClient(t *testing.T) {
	slot := NewDedicatedClientSlot(5002)
	h := newHarness(t, DefaultConfig(), slot)
	h.admit(1, 0)
	h.admit(2, 0)
	h.take(1)
	h.take(2)

	h.client(1, CmdSyncNextMessage) // empty local inbox authorizes a physical drain
	h.expectUpstream(CmdSyncNextMessage)
	raw := channelMsg("hello")
	msg, _ := DowngradeInbox(raw, 0) // clients without DEVICE_QUERY use the legacy dialect
	h.upstream(raw...)
	h.expectClient(1, msg)                    // satisfies the pending sync
	h.expectClient(2, []byte{PushMsgWaiting}) // queued, client is prompted
	h.expectUpstream(CmdSyncNextMessage)      // keep draining to empty
	h.upstream(RespNoMoreMessages)
	h.expectUpstreamNone()

	h.client(2, CmdSyncNextMessage)
	h.expectClient(2, msg) // served locally, no upstream pop
	if len(slot.OfflineQueue) != 1 {
		t.Fatalf("detached dedicated slot should retain the message, has %d", len(slot.OfflineQueue))
	}

	// The dedicated client connects later and receives its backlog.
	h.admit(3, 5002)
	h.expectClient(3, []byte{PushMsgWaiting})
	h.client(3, CmdSyncNextMessage)
	h.expectClient(3, msg)
	h.client(3, CmdSyncNextMessage)
	h.expectUpstream(CmdSyncNextMessage)
	h.upstream(RespNoMoreMessages)
	h.expectClient(3, []byte{RespNoMoreMessages})
}

func (h *harness) expectUpstreamNone() {
	h.t.Helper()
	if got := h.take(0); len(got) != 0 {
		h.t.Fatalf("unexpected upstream frames %x", got)
	}
}

func TestLegacyClientReceivesDowngradedInbox(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.admit(1, 0)
	h.take(1)
	h.client(1, CmdDeviceQuery, 2)
	h.expectUpstream(CmdDeviceQuery, UpstreamAppTarget)
	h.upstream(deviceInfo(13)...)
	h.take(1)
	h.client(1, CmdSyncNextMessage)
	h.expectUpstream(CmdSyncNextMessage)
	h.upstream(channelMsg("x")...)
	got := h.take(1)
	if len(got) != 1 || got[0][0] != RespChannelMessage || len(got[0]) != len(channelMsg("x"))-3 {
		t.Fatalf("expected downgraded channel message, got %x", got)
	}
}

func TestScopedSendIsWrappedInHiddenSetupAndRestore(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.admit(1, 0)
	h.take(1)
	scope := append([]byte{CmdSetFloodScopeKey, 0}, bytes.Repeat([]byte{7}, 16)...)
	h.client(1, scope...)
	h.expectClient(1, []byte{RespOk}) // virtualized locally
	h.expectUpstreamNone()

	send := []byte{CmdSendChannelTxtMsg, 0, 0, 1, 2, 3, 4, 'h', 'i'}
	h.client(1, send...)
	h.expectUpstream(scope...)
	h.upstream(RespOk)
	h.expectClient(1) // setup reply is hidden
	h.expectUpstream(send...)
	h.upstream(RespOk)
	h.expectClient(1, []byte{RespOk})
	h.expectUpstream(CmdSetFloodScopeKey, 0)
	h.upstream(RespOk)
	h.expectClient(1)
	if h.b.Active() != nil {
		t.Fatal("transaction should be complete after restoration")
	}
}

func TestRemoteLeaseRoutesResultToOwnerAndRejectsConcurrentRequest(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.admit(1, 0)
	h.admit(2, 0)
	h.take(1)
	h.take(2)
	peer := bytes.Repeat([]byte{0x42}, 32)
	login := append([]byte{CmdSendLogin}, peer...)
	h.client(1, login...)
	h.expectUpstream(login...)
	sent := []byte{RespSent, 0, 1, 0, 0, 0, 0x10, 0x27, 0, 0}
	h.upstream(sent...)
	h.expectClient(1, sent)

	status := append([]byte{CmdSendStatusReq}, peer...)
	h.client(2, status...)
	h.expectClient(2, []byte{RespErr, ErrBadState})
	h.expectUpstreamNone()

	result := append([]byte{PushLoginSuccess, 0}, peer[:6]...)
	h.upstream(result...)
	h.expectClient(1, result)
	h.expectClient(2)
}

func TestBroadcastPushesAndUnexpectedResponseEndsEpoch(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.admit(1, 0)
	h.admit(2, 0)
	h.take(1)
	h.take(2)
	advert := append([]byte{PushAdvert}, bytes.Repeat([]byte{1}, 32)...)
	h.upstream(advert...)
	h.expectClient(1, advert)
	h.expectClient(2, advert)

	h.b.UpstreamFrame([]byte{RespOk}, h.now) // ordinary response without owner
	if !h.b.Failed() {
		t.Fatal("unowned ordinary response must end the epoch")
	}
}

func TestResponseTimeoutEndsEpochAndRecordsDebt(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, cfg)
	h.admit(1, 0)
	h.client(1, CmdGetDeviceTime)
	h.take(0)
	h.now = cfg.ResponseTimeout
	h.b.Tick(h.now)
	if !h.b.Failed() || h.b.ResponseDebt() == nil {
		t.Fatal("timeout should fail the epoch and keep the response debt")
	}
	done, err := h.b.DrainUncertainFrame([]byte{RespCurrentTime, 0, 0, 0, 0}, h.now)
	if err != nil || !done {
		t.Fatalf("late reply should clear the debt, done=%v err=%v", done, err)
	}
}

func TestDedicatedReplacementAndPolicyRejections(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PrivateKeyExport = false
	slot := NewDedicatedClientSlot(5002)
	h := newHarness(t, cfg, slot)
	h.admit(1, 5002)
	h.admit(2, 5002)
	if h.b.Session(1) != nil || slot.AttachedSessionID != 2 {
		t.Fatal("new dedicated connection should replace the old one")
	}
	h.take(2)
	h.client(2, CmdExportPrivateKey)
	h.expectClient(2, []byte{RespDisabled})
	h.expectUpstreamNone()
}

func TestCommandQueueOverflowClosesSessionWithErrorLog(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CommandLimit = 2
	h := newHarness(t, cfg)
	h.admit(1, 0)
	h.b.ClientFrame(1, []byte{CmdGetDeviceTime}, h.now) // active upstream
	h.b.ClientFrame(1, []byte{CmdGetDeviceTime}, h.now) // queued
	h.b.ClientFrame(1, []byte{CmdGetDeviceTime}, h.now) // over the limit
	var closed, logged bool
	for _, a := range h.b.TakeActions() {
		switch a := a.(type) {
		case CloseSession:
			closed = a.Session == 1
		case Diagnostic:
			if a.Category == CatError && strings.Contains(a.Message, "command queue overflow") {
				logged = true
			}
		}
	}
	if !closed || !logged || h.b.Session(1) != nil {
		t.Fatalf("closed=%v errorLogged=%v", closed, logged)
	}
}

func TestDedicatedOverflowEvictsOldestChannelMessage(t *testing.T) {
	s := NewDedicatedClientSlot(1)
	dm := []byte{RespContactMessage, 1, 2, 3, 4, 5, 6, 0, 0, 0, 0, 0, 0, 'a'}
	s.EnqueueOffline(dm, 2)
	s.EnqueueOffline(channelMsg("old"), 2)
	if r := s.EnqueueOffline(channelMsg("new"), 2); r != EnqueueChannelEvicted {
		t.Fatalf("got %v", r)
	}
	if !bytes.Equal(s.OfflineQueue[0], dm) || !bytes.Equal(s.OfflineQueue[1], channelMsg("new")) {
		t.Fatal("oldest channel message should have been evicted")
	}
	s2 := NewDedicatedClientSlot(1)
	s2.EnqueueOffline(dm, 1)
	if r := s2.EnqueueOffline(dm, 1); r != EnqueueNewMessageDiscarded {
		t.Fatalf("got %v", r)
	}
}

func TestProblemsAreLoggedAtVisibleLevels(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InboxEntries = 1
	h := newHarness(t, cfg)
	h.admit(1, 0)
	h.admit(2, 0)

	// Unsupported command: client gets ERR, operator gets a warning.
	h.client(1, 0xee)
	if !h.logged(CatWarn, "event=command.rejected") || !h.logged(CatWarn, "result=ERR_UNSUPPORTED_CMD") {
		t.Fatal("rejection must be logged as warning")
	}

	// Remote-lease conflict explains why.
	peer := bytes.Repeat([]byte{0x42}, 32)
	h.client(1, append([]byte{CmdSendLogin}, peer...)...)
	h.upstream(RespSent, 0, 1, 0, 0, 0, 0x10, 0x27, 0, 0)
	h.client(2, append([]byte{CmdSendStatusReq}, peer...)...)
	if !h.logged(CatWarn, "another remote request") {
		t.Fatal("lease conflict must explain itself")
	}

	// A multi-client that never fetches its messages is disconnected with an error.
	h.client(1, CmdSyncNextMessage)
	h.upstream(channelMsg("a")...)
	h.upstream(channelMsg("b")...)
	if !h.logged(CatError, "inbox overflow limit=1") || h.b.Session(2) != nil {
		t.Fatal("inbox overflow must be an error and close the session")
	}
}

func TestSlowClientWriteDeadlineIsError(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, cfg)
	h.b.Admit(1, 0, 0) // MSG_WAITING emitted but never acknowledged
	h.b.Tick(cfg.WriteTimeout)
	for _, a := range h.b.TakeActions() {
		if d, ok := a.(Diagnostic); ok {
			h.diags = append(h.diags, d)
		}
	}
	if !h.logged(CatError, "output write deadline") {
		t.Fatal("write deadline must be an error")
	}
}

func TestEpochEndLevels(t *testing.T) {
	if epochEndCategory("process shutdown") != CatInfo || epochEndCategory("upstream closed: EOF") != CatInfo {
		t.Fatal("planned or already reported endings should be info")
	}
	cfg := DefaultConfig()
	h := newHarness(t, cfg)
	h.admit(1, 0)
	h.client(1, CmdGetDeviceTime)
	h.now = cfg.ResponseTimeout
	h.b.Tick(h.now)
	h.drain()
	if !h.logged(CatError, "event=upstream.epoch_ended") {
		t.Fatal("companion timeout must be an error")
	}
}
