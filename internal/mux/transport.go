package mux

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Event is produced by an Endpoint for the runtime coordinator.
type Event interface{ endpointID() int64 }

// FrameEvent carries one complete decoded payload.
type FrameEvent struct {
	Endpoint int64
	Payload  []byte
}

// ClosedEvent reports that the peer or the reader ended the connection.
type ClosedEvent struct {
	Endpoint  int64
	Reason    string
	Malformed bool
}

// WrittenEvent confirms that a write reached the socket.
type WrittenEvent struct{ Endpoint, Epoch, WriteID int64 }

// WriteFailedEvent reports a failed socket write.
type WriteFailedEvent struct {
	Endpoint, Epoch, WriteID int64
	Reason                   string
}

func (e FrameEvent) endpointID() int64       { return e.Endpoint }
func (e ClosedEvent) endpointID() int64      { return e.Endpoint }
func (e WrittenEvent) endpointID() int64     { return e.Endpoint }
func (e WriteFailedEvent) endpointID() int64 { return e.Endpoint }

// Write is one queued outgoing payload.
type Write struct {
	Epoch, WriteID int64
	Payload        []byte
}

// Endpoint owns a socket with one reader and one writer goroutine. Bounded
// writer queues keep blocking I/O and slow clients out of the broker.
type Endpoint struct {
	ID           int64
	Conn         net.Conn
	inMarker     byte
	outMarker    byte
	events       chan<- Event
	frameTimeout time.Duration
	writeTimeout time.Duration
	writes       chan Write
	cancel       chan struct{}
	wg           sync.WaitGroup
	started      bool
	stopped      atomic.Bool
	stopOnce     sync.Once
}

// NewEndpoint wraps a connected socket.
func NewEndpoint(conn net.Conn, id int64, inMarker, outMarker byte, events chan<- Event,
	frameTimeout, writeTimeout time.Duration, outputCapacity int) *Endpoint {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return &Endpoint{
		ID: id, Conn: conn, inMarker: inMarker, outMarker: outMarker, events: events,
		frameTimeout: frameTimeout, writeTimeout: writeTimeout,
		writes: make(chan Write, outputCapacity), cancel: make(chan struct{}),
	}
}

// Start launches the reader and writer goroutines.
func (e *Endpoint) Start() {
	if e.started {
		return
	}
	e.started = true
	e.wg.Add(2)
	go e.reader()
	go e.writer()
}

// Enqueue never blocks. A full writer queue is a write failure owned by the
// runtime, not permission to accumulate unbounded bytes.
func (e *Endpoint) Enqueue(w Write) bool {
	if e.stopped.Load() {
		return false
	}
	select {
	case e.writes <- w:
		return true
	default:
		return false
	}
}

// Stop closes the socket and joins both goroutines.
func (e *Endpoint) Stop() {
	e.stopOnce.Do(func() {
		e.stopped.Store(true)
		close(e.cancel)
		_ = e.Conn.Close()
		if e.started {
			e.wg.Wait()
		}
	})
}

// RemoteAddr renders the peer address for logs.
func (e *Endpoint) RemoteAddr() string {
	if a := e.Conn.RemoteAddr(); a != nil {
		return a.String()
	}
	return "unknown"
}

func (e *Endpoint) publish(ev Event) bool {
	select {
	case e.events <- ev:
		return true
	case <-e.cancel:
		return false
	}
}

func (e *Endpoint) reader() {
	defer e.wg.Done()
	decoder := NewDecoder(e.inMarker, e.frameTimeout)
	buf := make([]byte, 1024)
	closed := func(err error) {
		if !e.stopped.Load() {
			var fe *FrameError
			e.publish(ClosedEvent{e.ID, err.Error(), errors.As(err, &fe)})
		}
	}
	for {
		// A short read deadline lets us enforce the frame assembly deadline
		// while the peer is silent.
		_ = e.Conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := e.Conn.Read(buf)
		if n > 0 {
			payloads, ferr := decoder.Feed(buf[:n], Now())
			for _, p := range payloads {
				if !e.publish(FrameEvent{e.ID, p}) {
					return
				}
			}
			if ferr != nil {
				closed(ferr)
				return
			}
		}
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if derr := decoder.CheckDeadline(Now()); derr != nil {
					closed(derr)
					return
				}
			} else {
				if errors.Is(err, io.EOF) {
					if ferr := decoder.Finish(); ferr != nil {
						err = ferr
					} else {
						err = errors.New("peer disconnected")
					}
				}
				closed(err)
				return
			}
		}
		if e.stopped.Load() {
			return
		}
	}
}

func (e *Endpoint) writer() {
	defer e.wg.Done()
	for {
		var w Write
		select {
		case w = <-e.writes:
		case <-e.cancel:
			return
		}
		frame, err := EncodeFrame(w.Payload, e.outMarker)
		if err == nil {
			_ = e.Conn.SetWriteDeadline(time.Now().Add(e.writeTimeout))
			_, err = e.Conn.Write(frame)
		}
		if err != nil {
			if !e.stopped.Load() {
				e.publish(WriteFailedEvent{e.ID, w.Epoch, w.WriteID, err.Error()})
			}
			return
		}
		if !e.publish(WrittenEvent{e.ID, w.Epoch, w.WriteID}) {
			return
		}
	}
}
