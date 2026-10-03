package mux

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// fakeCompanion is a loopback companion: startup is automatic, then every
// command is handed to the test, which answers through reply/push.
type fakeCompanion struct {
	ln       net.Listener
	ready    chan struct{}
	commands chan []byte
	conn     chan net.Conn
}

func startFakeCompanion(t *testing.T) *fakeCompanion {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCompanion{ln: ln, ready: make(chan struct{}, 4), commands: make(chan []byte, 32), conn: make(chan net.Conn, 4)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeCompanion) serve(c net.Conn) {
	defer c.Close()
	d := NewDecoder(ClientToCompanionMarker, time.Minute)
	buf := make([]byte, 1024)
	started := false
	for {
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		payloads, _ := d.Feed(buf[:n], Now())
		for _, p := range payloads {
			if started {
				f.commands <- p
				continue
			}
			switch p[0] {
			case CmdAppStart:
				writeFrame(c, CompanionToClientMarker, selfInfo(0xa5))
			case CmdDeviceQuery:
				writeFrame(c, CompanionToClientMarker, deviceInfo(13))
			case CmdSetFloodScopeKey:
				writeFrame(c, CompanionToClientMarker, []byte{RespOk})
				started = true
				f.conn <- c
				f.ready <- struct{}{}
			}
		}
	}
}

func writeFrame(c net.Conn, marker byte, p []byte) {
	frame, _ := EncodeFrame(p, marker)
	_, _ = c.Write(frame)
}

type testClient struct {
	t    *testing.T
	conn net.Conn
	dec  *Decoder
	got  [][]byte
}

func dialClient(t *testing.T, addr net.Addr) *testClient {
	c, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &testClient{t: t, conn: c, dec: NewDecoder(CompanionToClientMarker, time.Minute)}
}

func (c *testClient) send(p ...byte) { writeFrame(c.conn, ClientToCompanionMarker, p) }

// next returns the next frame, skipping MSG_WAITING hints unless wanted.
func (c *testClient) next(skipHints bool) []byte {
	c.t.Helper()
	buf := make([]byte, 1024)
	for {
		for len(c.got) > 0 {
			p := c.got[0]
			c.got = c.got[1:]
			if skipHints && bytes.Equal(p, []byte{PushMsgWaiting}) {
				continue
			}
			return p
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, err := c.conn.Read(buf)
		if err != nil {
			c.t.Fatalf("client read: %v", err)
		}
		out, _ := c.dec.Feed(buf[:n], Now())
		c.got = append(c.got, out...)
	}
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestRuntimeEndToEnd(t *testing.T) {
	Log.SetOutput(io.Discard)
	companion := startFakeCompanion(t)
	port := companion.ln.Addr().(*net.TCPAddr).Port

	cfg := DefaultConfig()
	cfg.ListenMultiClientPort = 0 // ephemeral, tests only
	dedicatedPort := freePort(t)
	cfg.ListenDedicatedClientPorts = []int{dedicatedPort}
	rt := NewRuntime("127.0.0.1", port, cfg)
	errc := make(chan error, 1)
	go func() { errc <- rt.Run() }()
	<-rt.Ready()
	select {
	case <-companion.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not complete")
	}
	up := <-companion.conn
	addrs := rt.ListenerAddrs()

	a := dialClient(t, addrs[0])
	b := dialClient(t, addrs[0])
	if p := a.next(false); !bytes.Equal(p, []byte{PushMsgWaiting}) {
		t.Fatalf("expected admission hint, got %x", p)
	}
	b.next(false)

	// An ordinary reply goes only to the requester.
	a.send(CmdGetDeviceTime)
	if cmd := <-companion.commands; !bytes.Equal(cmd, []byte{CmdGetDeviceTime}) {
		t.Fatalf("companion got %x", cmd)
	}
	writeFrame(up, CompanionToClientMarker, []byte{RespCurrentTime, 1, 0, 0, 0})
	if p := a.next(true); !bytes.Equal(p, []byte{RespCurrentTime, 1, 0, 0, 0}) {
		t.Fatalf("client a got %x", p)
	}

	// One physical inbox pop is fanned out to both clients.
	b.send(CmdSyncNextMessage)
	if cmd := <-companion.commands; cmd[0] != CmdSyncNextMessage {
		t.Fatalf("companion got %x", cmd)
	}
	msg := channelMsg("hi")
	legacy, _ := DowngradeInbox(msg, 0)
	writeFrame(up, CompanionToClientMarker, msg)
	if p := b.next(true); !bytes.Equal(p, legacy) {
		t.Fatalf("client b got %x", p)
	}
	<-companion.commands // drain continues to empty
	writeFrame(up, CompanionToClientMarker, []byte{RespNoMoreMessages})
	a.send(CmdSyncNextMessage)
	if p := a.next(true); !bytes.Equal(p, legacy) {
		t.Fatalf("client a got %x", p)
	}

	// A dedicated client connecting later receives its retained backlog.
	d := dialClient(t, addrs[1])
	if p := d.next(false); !bytes.Equal(p, []byte{PushMsgWaiting}) {
		t.Fatalf("dedicated client expected hint, got %x", p)
	}
	d.send(CmdSyncNextMessage)
	if p := d.next(true); !bytes.Equal(p, legacy) {
		t.Fatalf("dedicated client got %x", p)
	}

	rt.Stop()
	if err := <-errc; err != nil {
		t.Fatalf("run: %v", err)
	}
}
