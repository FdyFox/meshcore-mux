package mux

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStateFileRoundTripIsPrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "state.json")
	key := selfInfo(0xa5)[4:36]
	in := &stateSnapshot{key: key, queues: map[int][][]byte{5002: {channelMsg("a"), channelMsg("b")}, 5003: nil}, dedup: []string{"x"}}
	if err := writeStateFile(path, in); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state file holds message text and must be 0600, is %o", info.Mode().Perm())
	}
	out, err := loadStateFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.key, key) || len(out.queues[5002]) != 2 || !bytes.Equal(out.queues[5002][1], channelMsg("b")) || out.dedup[0] != "x" {
		t.Fatalf("round trip lost data: %+v", out)
	}
	if entries, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".*.tmp")); len(entries) != 0 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
}

func persistenceRuntime(t *testing.T, path string, ports ...int) *Runtime {
	cfg := DefaultConfig()
	cfg.ListenDedicatedClientPorts = ports
	cfg.PersistenceEnabled = true
	cfg.StateFile = path
	return NewRuntime("127.0.0.1", 1, cfg)
}

func TestRestoreFiltersInvalidAndUnconfiguredEntries(t *testing.T) {
	Log.SetOutput(io.Discard)
	path := filepath.Join(t.TempDir(), "state.json")
	key := selfInfo(0xa5)[4:36]
	snap := &stateSnapshot{key: key, queues: map[int][][]byte{
		5002: {channelMsg("keep"), {RespOk}}, // RESP_OK is not an inbox frame and must be ignored
		6000: {channelMsg("gone")},           // port no longer configured
	}}
	if err := writeStateFile(path, snap); err != nil {
		t.Fatal(err)
	}
	r := persistenceRuntime(t, path, 5002)
	r.restoreState()
	q := r.slots[0].OfflineQueue
	if len(q) != 1 || !bytes.Equal(q[0], channelMsg("keep")) {
		t.Fatalf("unexpected restored queue %x", q)
	}
	if !bytes.Equal(r.slotsKey, key) {
		t.Fatal("restored queues must remember their companion identity")
	}

	// A different companion on the next connect must not inherit the queue.
	r.prepareDedicatedSlots(selfInfo(0x5a)[4:36])
	if len(r.slots[0].OfflineQueue) != 0 {
		t.Fatal("queue of another companion identity must be cleared")
	}
}

func TestCorruptStateFileIsMovedAside(t *testing.T) {
	Log.SetOutput(io.Discard)
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := persistenceRuntime(t, path, 5002)
	r.restoreState()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("corrupt file should have been moved aside")
	}
	if m, _ := filepath.Glob(path + ".corrupt-*"); len(m) != 1 {
		t.Fatalf("expected one preserved corrupt file, got %v", m)
	}
}

// TestDedicatedQueueSurvivesRestart: a message fetched while the dedicated
// client is offline is still delivered after the mux process restarts.
func TestDedicatedQueueSurvivesRestart(t *testing.T) {
	Log.SetOutput(io.Discard)
	companion := startFakeCompanion(t)
	port := companion.ln.Addr().(*net.TCPAddr).Port
	path := filepath.Join(t.TempDir(), "state.json")
	dedicatedPort := freePort(t)

	start := func() (*Runtime, chan error, net.Conn, []net.Addr) {
		cfg := DefaultConfig()
		cfg.ListenMultiClientPort = 0
		cfg.ListenDedicatedClientPorts = []int{dedicatedPort}
		cfg.PersistenceEnabled = true
		cfg.StateFile = path
		cfg.StateFlushInterval = 50 * time.Millisecond
		rt := NewRuntime("127.0.0.1", port, cfg)
		errc := make(chan error, 1)
		go func() { errc <- rt.Run() }()
		<-rt.Ready()
		select {
		case <-companion.ready:
		case <-time.After(5 * time.Second):
			t.Fatal("startup did not complete")
		}
		return rt, errc, <-companion.conn, rt.ListenerAddrs()
	}

	// First process: a multi-client sync fetches one message; the dedicated
	// client is not connected, so its copy waits in the dedicated queue.
	rt, errc, up, addrs := start()
	a := dialClient(t, addrs[0])
	a.next(false) // admission MSG_WAITING
	a.send(CmdSyncNextMessage)
	<-companion.commands
	msg := channelMsg("persist me")
	writeFrame(up, CompanionToClientMarker, msg)
	a.next(true)
	<-companion.commands // drain continues to empty
	writeFrame(up, CompanionToClientMarker, []byte{RespNoMoreMessages})

	// The periodic flush must persist the message while running, so a crash
	// (no graceful shutdown) would not lose it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if data, _ := os.ReadFile(path); strings.Contains(string(data), Hex(msg)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("message was not flushed to the state file while running")
		}
		time.Sleep(20 * time.Millisecond)
	}
	rt.Stop()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), Hex(msg)) {
		t.Fatalf("state file should contain the queued message: %v\n%s", err, data)
	}

	// Second process: the dedicated client connects and receives the message
	// without the companion being asked again.
	rt, errc, _, addrs = start()
	d := dialClient(t, addrs[1])
	if p := d.next(false); !bytes.Equal(p, []byte{PushMsgWaiting}) {
		t.Fatalf("expected backlog hint, got %x", p)
	}
	d.send(CmdSyncNextMessage)
	legacy, _ := DowngradeInbox(msg, 0)
	if p := d.next(true); !bytes.Equal(p, legacy) {
		t.Fatalf("dedicated client got %x after restart", p)
	}
	rt.Stop()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if after, _ := loadStateFile(path); len(after.queues[dedicatedPort]) != 0 {
		t.Fatal("delivered message must be removed from the state file")
	}
}
