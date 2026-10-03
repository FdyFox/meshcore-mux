package mux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type listenerKind int

const (
	kindMultiClient listenerKind = iota
	kindDedicatedClient
)

func (k listenerKind) String() string {
	if k == kindDedicatedClient {
		return "dedicated_client"
	}
	return "multi_client"
}

type listenerBinding struct {
	ln     net.Listener
	kind   listenerKind
	slotID int
}

type acceptedSocket struct {
	conn   net.Conn
	kind   listenerKind
	slotID int
}

type clientRoute struct {
	kind   listenerKind
	slotID int
}

const tickInterval = 100 * time.Millisecond

// Runtime owns the listeners, upstream epochs, socket goroutines, and broker
// side effects. Only the goroutine running Run calls the Broker.
type Runtime struct {
	host string
	port int
	cfg  Config

	// Watchdog is touched after every coordinator iteration of a running epoch.
	Watchdog *Watchdog

	stopping   chan struct{}
	stopOnce   sync.Once
	finished   chan struct{}
	accepted   chan acceptedSocket
	acceptWG   sync.WaitGroup
	acceptMu   sync.Mutex
	acceptErr  error
	ready      chan struct{} // closed once all listeners are bound
	readyOnce  sync.Once
	hasRun     bool
	listeners  []listenerBinding
	upstreamOK bool

	clients     map[int64]*Endpoint
	routes      map[int64]clientRoute
	nextSession int64
	nextEpoch   int64
	orphan      []byte
	orphanKey   []byte
	slots       []*DedicatedClientSlot
	slotsKey    []byte
	radio       *CompanionRadioState
	radioKey    []byte

	malformedLog logLimiter
	refusedLog   logLimiter

	persister    *statePersister // nil when persistence is disabled
	persistedGen uint64
	lastPersist  time.Duration
	forcePersist bool
}

// logLimiter allows one record per second and counts what it suppressed, so
// a misbehaving peer is visible without flooding the log.
type logLimiter struct {
	last       time.Duration
	has        bool
	suppressed uint64
}

func (l *logLimiter) allow() (bool, uint64) {
	now := Now()
	if l.has && now-l.last < time.Second {
		l.suppressed++
		return false, 0
	}
	l.has, l.last = true, now
	n := l.suppressed
	l.suppressed = 0
	return true, n
}

// NewRuntime creates a runtime for one upstream companion.
func NewRuntime(host string, port int, cfg Config) *Runtime {
	r := &Runtime{
		host: host, port: port, cfg: cfg,
		stopping: make(chan struct{}), finished: make(chan struct{}),
		accepted: make(chan acceptedSocket), ready: make(chan struct{}),
		clients: map[int64]*Endpoint{}, routes: map[int64]clientRoute{},
		radio: NewCompanionRadioState(),
	}
	for _, p := range cfg.ListenDedicatedClientPorts {
		r.slots = append(r.slots, NewDedicatedClientSlot(p))
	}
	return r
}

func (r *Runtime) stopRequested() bool {
	select {
	case <-r.stopping:
		return true
	default:
		return false
	}
}

// Stop requests cancellation and waits for Run to return. Safe from any goroutine.
func (r *Runtime) Stop() {
	r.requestStop()
	<-r.finished
}

// Ready is closed once every listener is bound (or Run has failed).
func (r *Runtime) Ready() <-chan struct{} { return r.ready }

// ListenerAddrs returns the bound listener addresses (valid after Ready).
func (r *Runtime) ListenerAddrs() []net.Addr {
	out := make([]net.Addr, 0, len(r.listeners))
	for _, l := range r.listeners {
		out = append(out, l.ln.Addr())
	}
	return out
}

func (r *Runtime) requestStop() {
	r.stopOnce.Do(func() {
		Log.Debugf("event=runtime.stop_requested")
		close(r.stopping)
	})
}

// Run owns the listeners and every upstream epoch until shutdown. An
// unexpected acceptor failure is returned so the process exits nonzero.
func (r *Runtime) Run() (err error) {
	if r.hasRun {
		return errors.New("runtime is single-use")
	}
	r.hasRun = true
	defer close(r.finished)
	defer func() {
		r.requestStop()
		for _, l := range r.listeners {
			_ = l.ln.Close()
		}
		r.closeAllClients()
		r.acceptWG.Wait()
		if r.persister != nil {
			r.finalPersist()
		} else {
			unread := 0
			for _, s := range r.slots {
				unread += len(s.OfflineQueue)
			}
			if unread > 0 {
				Log.Warnf("event=dedicated_queues.volatile_discard process_stopping=true entries=%d (undelivered messages are lost on shutdown; enable persistence to keep them)", unread)
			}
		}
		Log.Debugf("event=runtime.stopped")
		r.readyOnce.Do(func() { close(r.ready) })
	}()

	if r.cfg.PersistenceEnabled {
		r.restoreState()
		r.persister = newStatePersister(r.cfg.StateFile)
	}
	if r.listeners, err = r.bindListeners(); err != nil {
		return err
	}
	r.readyOnce.Do(func() { close(r.ready) })
	for _, l := range r.listeners {
		Log.Infof("event=listener.started kind=%s dedicated_slot_id=%s address=%s upstream=%s",
			l.kind, slotLabel(l.slotID), l.ln.Addr(), r.upstreamAddr())
		r.startAcceptor(l)
	}
	backoff := 500 * time.Millisecond
	for !r.stopRequested() {
		conn := r.connectUpstream()
		if conn != nil {
			if r.runConnection(conn) {
				backoff = 500 * time.Millisecond
			}
		}
		if r.stopRequested() {
			break
		}
		delay := jitter(backoff)
		Log.Infof("event=upstream.reconnect_scheduled remote=%s delay_ms=%d", r.upstreamAddr(), delay.Milliseconds())
		r.waitWithRefusal(delay)
		backoff = min(backoff*2, 30*time.Second)
	}
	r.acceptMu.Lock()
	defer r.acceptMu.Unlock()
	return r.acceptErr
}

func (r *Runtime) upstreamAddr() string { return net.JoinHostPort(r.host, strconv.Itoa(r.port)) }

// runConnection runs one upstream epoch. It returns true when the epoch was
// healthy for long enough to reset the reconnect backoff.
func (r *Runtime) runConnection(conn net.Conn) (resetBackoff bool) {
	r.nextEpoch++
	epoch := r.nextEpoch
	events := make(chan Event)
	upstream := NewEndpoint(conn, 0, CompanionToClientMarker, ClientToCompanionMarker, events,
		r.cfg.FrameTimeout, r.cfg.WriteTimeout, 16)
	defer func() {
		if p := recover(); p != nil {
			Log.Errorf("event=upstream.epoch_failed epoch=%d error=%q", epoch, fmt.Sprint(p))
		}
		upstream.Stop()
		r.closeAllClients()
	}()

	startup, err := r.synchronize(upstream, events, epoch)
	if err != nil {
		if !r.stopRequested() {
			Log.Errorf("event=upstream.epoch_failed epoch=%d error=%q", epoch, err.Error())
		}
		return false
	}
	if r.stopRequested() {
		return false
	}
	key := startup.SelfKey
	orphan := r.orphanFor(key)
	r.prepareDedicatedSlots(key)
	radio := r.radioStateFor(key)
	broker := NewBroker(epoch, key, r.cfg, Now(), orphan, r.slots, radio)
	readyAt := Now()
	r.upstreamOK = true
	if startup.UpstreamProtocolLevel > MaxExposedProtocolLevel {
		Log.Warnf("upstream firmware protocol %d is newer than mux-supported protocol %d; using compatibility mode and exposing protocol %d downstream",
			startup.UpstreamProtocolLevel, MaxExposedProtocolLevel, startup.ExposedProtocolLevel)
	}
	Log.Infof("event=upstream.ready epoch=%d remote=%s %s", epoch, upstream.RemoteAddr(), startup.Identification())
	r.runEpoch(broker, upstream, events)
	if broker.ResponseDebt() != nil && !r.stopRequested() {
		r.drainResponseDebt(broker, upstream, events)
	}
	r.orphan = dup(broker.Orphan())
	r.orphanKey = nil
	if r.orphan != nil {
		r.orphanKey = key
	}
	return Now()-readyAt >= 30*time.Second
}

func (r *Runtime) bindListeners() ([]listenerBinding, error) {
	// Bind the complete configured set before starting any acceptor: partial
	// availability is unsafe because clients could reach the wrong mode.
	var out []listenerBinding
	listen := func(port int, kind listenerKind, slot int) error {
		ln, err := net.Listen("tcp", net.JoinHostPort(r.cfg.ListenHost, strconv.Itoa(port)))
		if err != nil {
			return err
		}
		out = append(out, listenerBinding{ln, kind, slot})
		return nil
	}
	err := listen(r.cfg.ListenMultiClientPort, kindMultiClient, 0)
	for _, p := range r.cfg.ListenDedicatedClientPorts {
		if err != nil {
			break
		}
		err = listen(p, kindDedicatedClient, p)
	}
	if err != nil {
		for _, l := range out {
			_ = l.ln.Close()
		}
		return nil, err
	}
	return out, nil
}

func (r *Runtime) startAcceptor(l listenerBinding) {
	// A failed accept must stop the process, not leave a live daemon with a dead listener.
	r.acceptWG.Add(1)
	go func() {
		defer r.acceptWG.Done()
		for {
			conn, err := l.ln.Accept()
			if err != nil {
				if !r.stopRequested() {
					r.acceptMu.Lock()
					r.acceptErr = err
					r.acceptMu.Unlock()
					Log.Errorf("event=listener.failed error=%q", err.Error())
					r.requestStop()
				}
				return
			}
			select {
			case r.accepted <- acceptedSocket{conn, l.kind, l.slotID}:
			case <-r.stopping:
				_ = conn.Close()
				return
			}
		}
	}()
	go func() {
		<-r.stopping
		_ = l.ln.Close()
	}()
}

func (r *Runtime) refuse(a acceptedSocket, reason string, epoch int64) {
	if ok, suppressed := r.refusedLog.allow(); ok {
		Log.Warnf("event=client.refused remote=%s reason=%s epoch=%d suppressed_since_last=%d (companion not ready; client must reconnect)",
			a.conn.RemoteAddr(), reason, epoch, suppressed)
	}
	_ = a.conn.Close()
}

func (r *Runtime) connectUpstream() net.Conn {
	// Refuse downstream sockets during the bounded connect attempt.
	Log.Infof("event=upstream.connecting remote=%s", r.upstreamAddr())
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.ConnectTimeout)
	defer cancel()
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", r.upstreamAddr())
		done <- result{c, err}
	}()
	for {
		select {
		case res := <-done:
			if r.stopRequested() {
				if res.conn != nil {
					_ = res.conn.Close()
				}
				return nil
			}
			if res.err != nil {
				Log.Warnf("event=upstream.connect_failed remote=%s error=%q", r.upstreamAddr(), res.err.Error())
				return nil
			}
			Log.Infof("event=upstream.connected local=%s remote=%s", res.conn.LocalAddr(), res.conn.RemoteAddr())
			return res.conn
		case a := <-r.accepted:
			r.refuse(a, "upstream_connecting", r.nextEpoch)
		case <-r.stopping:
			cancel()
			if res := <-done; res.conn != nil {
				_ = res.conn.Close()
			}
			return nil
		}
	}
}

func (r *Runtime) synchronize(up *Endpoint, events chan Event, epoch int64) (*Startup, error) {
	// Keep startup responses private and refuse clients until the fence and scope reset succeed.
	up.Start()
	startup := NewStartup(Now(), r.cfg.StartupTimeout)
	for i, p := range StartupProbes() {
		r.wireUp(epoch, dirTx, p)
		if !up.Enqueue(Write{epoch, -int64(i + 1), p}) {
			return nil, &StartupError{"startup writer queue full"}
		}
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for !startup.Ready() {
		select {
		case ev := <-events:
			switch ev := ev.(type) {
			case FrameEvent:
				r.wireUp(epoch, dirRx, ev.Payload)
				cmd, err := startup.Receive(ev.Payload, Now())
				if err != nil {
					return nil, err
				}
				if cmd != nil {
					r.wireUp(epoch, dirTx, cmd)
					if !up.Enqueue(Write{epoch, -7, cmd}) {
						return nil, &StartupError{"startup writer queue full"}
					}
				}
			case ClosedEvent:
				return nil, &StartupError{"upstream closed: " + ev.Reason}
			case WriteFailedEvent:
				return nil, &StartupError{"upstream write failed: " + ev.Reason}
			}
		case a := <-r.accepted:
			r.refuse(a, "upstream_starting", epoch)
		case <-ticker.C:
		case <-r.stopping:
			return nil, &StartupError{"runtime stopping"}
		}
		if err := startup.CheckDeadline(Now()); err != nil {
			return nil, err
		}
	}
	return startup, nil
}

func (r *Runtime) runEpoch(b *Broker, up *Endpoint, upEvents chan Event) {
	// This goroutine alone calls Broker. Apply actions after each event, then
	// tick deadlines even during continuous traffic.
	down := make(chan Event)
	defer func() {
		if p := recover(); p != nil && !r.stopRequested() {
			b.FailEpoch(fmt.Sprint(p))
			r.applyActions(b, up)
		}
	}()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	ended := r.applyActions(b, up)
	for !ended && !r.stopRequested() {
		select {
		case a := <-r.accepted:
			r.admit(a, b, down)
		case ev := <-upEvents:
			r.handleUpstream(ev, b)
		case ev := <-down:
			r.handleDownstream(ev, b)
		case <-ticker.C:
		case <-r.stopping:
			b.FailEpoch("process shutdown")
			r.applyActions(b, up)
			return
		}
		if ended = r.applyActions(b, up); ended {
			break
		}
		b.Tick(Now())
		ended = r.applyActions(b, up)
		// The sole recurring liveness proof: the coordinator processed an event
		// or timer, applied effects, and advanced deadlines.
		r.Watchdog.Touch()
		r.maybePersist()
	}
}

func (r *Runtime) drainResponseDebt(b *Broker, up *Endpoint, events chan Event) {
	// A timed-out command may still be running inside an asynchronous
	// companion. Keep its TCP generation open and refuse downstream work until
	// its response grammar terminates, so an old handler cannot write an
	// untagged reply into the replacement connection.
	t := b.ResponseDebt()
	if !r.upstreamOK {
		up.Stop()
		r.quarantineUnresolved(b.Epoch, "the upstream connection ended with an ordinary response still outstanding")
		return
	}
	Log.Warnf("event=upstream.response_debt_draining epoch=%d command=%s opcode=%d owner=%d step=%s deadline_ms=%d",
		b.Epoch, t.Descriptor.Name, t.Command[0], t.Owner, t.Step, r.cfg.ResponseTimeout.Milliseconds())
	deadline := Now() + r.cfg.ResponseTimeout
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for b.ResponseDebt() != nil && !r.stopRequested() && Now() < deadline {
		select {
		case ev := <-events:
			switch ev := ev.(type) {
			case FrameEvent:
				r.wireUp(b.Epoch, dirRx, ev.Payload)
				done, err := b.DrainUncertainFrame(ev.Payload, Now())
				r.applyActions(b, up)
				if err != nil {
					up.Stop()
					r.quarantineUnresolved(b.Epoch, "response debt received an invalid or mismatched ordinary frame: "+err.Error())
					return
				}
				if done {
					Log.Debugf("event=upstream.response_debt_drained epoch=%d", b.Epoch)
				}
			case ClosedEvent:
				up.Stop()
				r.quarantineUnresolved(b.Epoch, "upstream closed while an old command could still complete: "+ev.Reason)
				return
			case WriteFailedEvent:
				up.Stop()
				r.quarantineUnresolved(b.Epoch, "upstream writer failed while an old command could still complete: "+ev.Reason)
				return
			}
		case a := <-r.accepted:
			r.refuse(a, "upstream_response_debt", b.Epoch)
		case <-r.stopping:
			return
		case <-ticker.C:
		}
	}
	if b.ResponseDebt() != nil && !r.stopRequested() {
		up.Stop()
		r.quarantineUnresolved(b.Epoch, "no terminal response arrived during the bounded poisoned drain")
	}
}

func (r *Runtime) quarantineUnresolved(epoch int64, reason string) {
	// Keep the replacement connection absent for one full response horizon so
	// a late handler writes into the poisoned old generation.
	Log.Errorf("event=upstream.response_debt_unresolved epoch=%d recovery=bounded_quarantine quarantine_ms=%d reason=%q",
		epoch, r.cfg.ResponseTimeout.Milliseconds(), reason)
	r.waitWithRefusal(r.cfg.ResponseTimeout)
	if !r.stopRequested() {
		Log.Warnf("event=upstream.response_debt_quarantine_complete epoch=%d recovery=reconnect", epoch)
	}
}

func (r *Runtime) admit(a acceptedSocket, b *Broker, events chan Event) {
	r.nextSession++
	id := r.nextSession
	Log.Infof("event=client.connected epoch=%d session=%d kind=%s dedicated_slot_id=%s local=%s remote=%s",
		b.Epoch, id, a.kind, slotLabel(a.slotID), a.conn.LocalAddr(), a.conn.RemoteAddr())
	ep := NewEndpoint(a.conn, id, ClientToCompanionMarker, CompanionToClientMarker, events,
		r.cfg.FrameTimeout, r.cfg.WriteTimeout, r.cfg.OutputFrames)
	r.clients[id] = ep
	r.routes[id] = clientRoute{a.kind, a.slotID}
	ep.Start()
	b.Admit(id, Now(), a.slotID)
}

func (r *Runtime) handleUpstream(ev Event, b *Broker) {
	now := Now()
	switch ev := ev.(type) {
	case FrameEvent:
		r.wireUp(b.Epoch, dirRx, ev.Payload)
		b.UpstreamFrame(ev.Payload, now)
		if b.Failed() {
			r.upstreamOK = false
		}
	case ClosedEvent:
		r.upstreamOK = false
		Log.Warnf("event=upstream.closed epoch=%d reason=%q", b.Epoch, ev.Reason)
		b.FailEpoch("upstream closed: " + ev.Reason)
	case WrittenEvent:
		Log.Debugf("event=upstream.write_completed epoch=%d write_id=%d", ev.Epoch, ev.WriteID)
		b.Written(0, ev.Epoch, ev.WriteID, now)
	case WriteFailedEvent:
		r.upstreamOK = false
		Log.Errorf("event=upstream.write_failed epoch=%d write_id=%d reason=%q", ev.Epoch, ev.WriteID, ev.Reason)
		b.WriteFailed(0, ev.Epoch, ev.Reason, now)
	}
}

func (r *Runtime) handleDownstream(ev Event, b *Broker) {
	now := Now()
	switch ev := ev.(type) {
	case FrameEvent:
		r.wireDown(ev.Endpoint, dirRx, ev.Payload)
		b.ClientFrame(ev.Endpoint, ev.Payload, now)
	case ClosedEvent:
		cat := CatNormal
		if ev.Malformed {
			// Malformed peers use the broker's rate-limited diagnostic instead.
			cat = CatMalformed
		} else {
			remote := "unknown"
			if ep := r.clients[ev.Endpoint]; ep != nil {
				remote = ep.RemoteAddr()
			}
			Log.Infof("event=client.disconnected epoch=%d session=%d remote=%s reason=%q category=normal",
				b.Epoch, ev.Endpoint, remote, ev.Reason)
		}
		b.ClientClosed(ev.Endpoint, now, ev.Reason, cat)
	case WrittenEvent:
		Log.Debugf("event=client.write_completed epoch=%d session=%d write_id=%d", ev.Epoch, ev.Endpoint, ev.WriteID)
		b.Written(ev.Endpoint, ev.Epoch, ev.WriteID, now)
	case WriteFailedEvent:
		// The broker reports the resulting disconnect as an error.
		Log.Debugf("event=client.write_failed epoch=%d session=%d write_id=%d reason=%q", ev.Epoch, ev.Endpoint, ev.WriteID, ev.Reason)
		b.WriteFailed(ev.Endpoint, ev.Epoch, ev.Reason, now)
	}
}

func (r *Runtime) applyActions(b *Broker, up *Endpoint) bool {
	// Execute effects only after ownership decisions finish. Queue failures can
	// generate more actions, so drain those before accepting another event.
	ended := false
	for {
		actions := b.TakeActions()
		if len(actions) == 0 {
			return ended
		}
		for _, a := range actions {
			switch a := a.(type) {
			case SendFrame:
				ep := up
				if a.Session != 0 {
					ep = r.clients[a.Session]
				}
				if ep != nil && ep.Enqueue(Write{a.Epoch, a.WriteID, a.Payload}) {
					if a.Session == 0 {
						r.wireUp(a.Epoch, dirTx, a.Payload)
					} else {
						r.wireDown(a.Session, dirTx, a.Payload)
					}
				} else {
					b.WriteFailed(a.Session, a.Epoch, "writer queue unavailable", Now())
				}
			case CloseSession:
				if ep := r.clients[a.Session]; ep != nil {
					delete(r.clients, a.Session)
					ep.Stop()
				}
				delete(r.routes, a.Session)
			case EndEpoch:
				if a.Epoch == b.Epoch {
					ended = true
				}
			case Diagnostic:
				r.logDiagnostic(a)
			}
		}
	}
}

func (r *Runtime) logDiagnostic(d Diagnostic) {
	switch d.Category {
	case CatMalformed:
		if ok, suppressed := r.malformedLog.allow(); ok {
			Log.Warnf("%s suppressed_since_last=%d", d.Message, suppressed)
		}
	case CatInfo:
		Log.Logf(LevelInfo, "%s", d.Message)
	case CatTrace:
		Log.Logf(LevelTrace, "%s", d.Message)
	case CatWarn:
		Log.Logf(LevelWarn, "%s", d.Message)
	case CatError:
		Log.Logf(LevelError, "%s", d.Message)
	default:
		Log.Logf(LevelDebug, "%s", d.Message)
	}
}

func (r *Runtime) closeAllClients() {
	// Detach the client map before joining endpoints so later actions from the
	// old epoch cannot reach a stopped socket.
	clients := r.clients
	r.clients = map[int64]*Endpoint{}
	clear(r.routes)
	for id, ep := range clients {
		Log.Infof("event=client.closed epoch=%d session=%d remote=%s reason=upstream_epoch_ended", r.nextEpoch, id, ep.RemoteAddr())
		ep.Stop()
	}
}

func (r *Runtime) prepareDedicatedSlots(key []byte) {
	// Dedicated history crosses epochs only for the same companion public key.
	if !bytes.Equal(r.slotsKey, key) {
		r.forcePersist = true // the identity in the state file must follow
	}
	if r.slotsKey != nil {
		if bytes.Equal(r.slotsKey, key) {
			n := 0
			for _, s := range r.slots {
				n += len(s.OfflineQueue)
			}
			if n > 0 {
				Log.Debugf("event=dedicated_queues.preserved entries=%d", n)
			}
		} else {
			n := 0
			for _, s := range r.slots {
				n += s.Clear()
			}
			Log.Warnf("event=dedicated_queues.cleared reason=upstream_identity_changed entries=%d", n)
		}
	}
	r.slotsKey = dup(key)
}

func (r *Runtime) orphanFor(key []byte) []byte {
	orphan := r.orphan
	if orphan == nil {
		return nil
	}
	r.orphan = nil
	same := r.orphanKey != nil && bytes.Equal(r.orphanKey, key)
	r.orphanKey = nil
	if same {
		return orphan
	}
	Log.Warnf("event=inbox.orphan_discarded reason=upstream_identity_changed")
	return nil
}

func (r *Runtime) radioStateFor(key []byte) *CompanionRadioState {
	// A key match cannot prove the node did not reboot, but a mismatch proves
	// that old reservations belong to another identity.
	if r.radioKey != nil && !bytes.Equal(r.radioKey, key) {
		r.radio = NewCompanionRadioState()
		Log.Warnf("event=radio_state.cleared reason=upstream_identity_changed")
	}
	r.radioKey = dup(key)
	return r.radio
}

func (r *Runtime) waitWithRefusal(d time.Duration) {
	// Keep listeners responsive during backoff without admitting sessions.
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case a := <-r.accepted:
			r.refuse(a, "upstream_unavailable", r.nextEpoch)
		case <-timer.C:
			return
		case <-r.stopping:
			return
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + rand.Float64()*0.4))
}

func (r *Runtime) wireUp(epoch int64, dir wireDir, p []byte) {
	if Log.Enabled(LevelDebug) {
		Log.Logf(LevelDebug, "%s", wireUpstream(epoch, dir, p))
	}
}

func (r *Runtime) wireDown(session int64, dir wireDir, p []byte) {
	// Dedicated connections are named by their stable listener port.
	if !Log.Enabled(LevelDebug) {
		return
	}
	if route, ok := r.routes[session]; ok && route.kind == kindDedicatedClient {
		Log.Logf(LevelDebug, "%s", wireDedicatedClient(route.slotID, dir, p))
	} else {
		Log.Logf(LevelDebug, "%s", wireMultiClient(session, dir, p))
	}
}

// Probe synchronizes with the companion, returns its identification, and disconnects.
func Probe(host string, port int, cfg Config) (string, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), cfg.ConnectTimeout)
	if err != nil {
		return "", err
	}
	events := make(chan Event, 32)
	ep := NewEndpoint(conn, 0, CompanionToClientMarker, ClientToCompanionMarker, events, cfg.FrameTimeout, cfg.WriteTimeout, 16)
	defer ep.Stop()
	ep.Start()
	startup := NewStartup(Now(), cfg.StartupTimeout)
	for i, p := range StartupProbes() {
		if !ep.Enqueue(Write{0, -int64(i + 1), p}) {
			return "", &StartupError{"startup writer queue full"}
		}
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for !startup.Ready() {
		select {
		case ev := <-events:
			switch ev := ev.(type) {
			case FrameEvent:
				cmd, err := startup.Receive(ev.Payload, Now())
				if err != nil {
					return "", err
				}
				if cmd != nil && !ep.Enqueue(Write{0, -7, cmd}) {
					return "", &StartupError{"startup writer queue full"}
				}
			case ClosedEvent:
				return "", &StartupError{"upstream closed: " + ev.Reason}
			case WriteFailedEvent:
				return "", &StartupError{"upstream write failed: " + ev.Reason}
			}
		case <-ticker.C:
		}
		if err := startup.CheckDeadline(Now()); err != nil {
			return "", err
		}
	}
	return startup.Identification(), nil
}

// Watchdog is a last-resort liveness guard: if the coordinator fails to reach
// a running epoch for five minutes, the process exits with status 142 so its
// supervisor can replace it. A nil Watchdog is inert.
type Watchdog struct {
	timer *time.Timer
}

// WatchdogTimeout is the process-wide liveness deadline.
const WatchdogTimeout = 5 * time.Minute

// StartWatchdog arms the process watchdog.
func StartWatchdog() *Watchdog {
	return &Watchdog{timer: time.AfterFunc(WatchdogTimeout, func() {
		Log.Errorf("Watchdog: no runtime progress for %s. Exiting with status 142...", WatchdogTimeout)
		os.Exit(142)
	})}
}

// Touch resets the countdown.
func (w *Watchdog) Touch() {
	if w != nil {
		w.timer.Reset(WatchdogTimeout)
	}
}
