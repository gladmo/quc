package quc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type entry struct {
	plugin Plugin
	addr   string
}

// Server manages multiple protocol plugins and dispatches incoming messages to a unified Handler.
//
// Example:
//
//	srv := quc.NewServer()
//	srv.Register(tcp.New(), ":8080")
//	srv.Register(ws.New(), ":8081")
//	srv.OnMessage(func(msg *quc.Message) {
//	    msg.Conn.Send(msg.Data) // echo
//	})
//	srv.Start()
type Server struct {
	entries   []entry
	handler   atomic.Pointer[Handler]       // lock-free read in hot path
	onConnect atomic.Pointer[func(Connection)] // lock-free read per new connection
	onDisconn atomic.Pointer[func(Connection)] // lock-free read per disconnect
	closed    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
	mu        sync.RWMutex // guards entries only
}

// NewServer creates a new Server.
func NewServer() *Server {
	return &Server{
		closed: make(chan struct{}),
	}
}

// Register adds a plugin that will listen on addr when Start is called.
func (s *Server) Register(plugin Plugin, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry{plugin: plugin, addr: addr})
	return nil
}

// OnMessage sets the handler called for every incoming message.
// It is safe to call after Start; the new handler takes effect immediately.
func (s *Server) OnMessage(h Handler) {
	s.handler.Store(&h)
}

// OnConnect sets the callback invoked when a new connection is established.
// It is safe to call after Start; the new callback takes effect immediately.
func (s *Server) OnConnect(h func(Connection)) {
	s.onConnect.Store(&h)
}

// OnDisconnect sets the callback invoked when a connection is closed.
// It is safe to call after Start; the new callback takes effect immediately.
func (s *Server) OnDisconnect(h func(Connection)) {
	s.onDisconn.Store(&h)
}

// Start begins listening on all registered plugins.
func (s *Server) Start() error {
	s.mu.RLock()
	entries := make([]entry, len(s.entries))
	copy(entries, s.entries)
	s.mu.RUnlock()

	for _, e := range entries {
		if err := e.plugin.Listen(e.addr); err != nil {
			return fmt.Errorf("quc: plugin %s failed to listen on %s: %w", e.plugin.Protocol(), e.addr, err)
		}
		s.wg.Add(1)
		go s.acceptLoop(e.plugin)
	}
	return nil
}

// Stop shuts down all plugins and waits for all goroutines to finish.
// It is safe to call Stop more than once.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() { close(s.closed) })

	s.mu.RLock()
	entries := make([]entry, len(s.entries))
	copy(entries, s.entries)
	s.mu.RUnlock()

	for _, e := range entries {
		e.plugin.Close()
	}

	s.wg.Wait()
	return nil
}

// acceptBackoff is the sleep between retries when Accept returns a transient error,
// preventing a tight busy-loop from consuming 100% CPU.
const acceptBackoff = 5 * time.Millisecond

func (s *Server) acceptLoop(p Plugin) {
	defer s.wg.Done()
	for {
		conn, err := p.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
				// Transient accept error — back off briefly to avoid tight busy-loop.
				time.Sleep(acceptBackoff)
				continue
			}
		}

		if hp := s.onConnect.Load(); hp != nil {
			(*hp)(conn)
		}

		s.wg.Add(1)
		go s.readLoop(conn)
	}
}

func (s *Server) readLoop(conn Connection) {
	defer s.wg.Done()
	defer func() {
		conn.Close()
		if hp := s.onDisconn.Load(); hp != nil {
			(*hp)(conn)
		}
	}()

	// Use the zero-copy buffered receive path when the connection supports it.
	// The per-connection buffer is grown as needed and reused across messages,
	// avoiding one heap allocation per received message.
	// Because h(msg) is called synchronously (inline), the buffer is safe to
	// overwrite on the next iteration after the handler returns.
	br, hasBR := conn.(BufferedReceiver)
	var recvBuf []byte

	// msg is reused across iterations: one allocation per connection instead of
	// one per message. msg.Data is updated each iteration before the handler is
	// called. Both msg and msg.Data are only valid for the duration of the handler
	// call — handlers that need to retain them past their return must copy.
	msg := &Message{Conn: conn}

	for {
		var (
			data []byte
			err  error
		)
		if hasBR {
			data, err = br.RecvInto(&recvBuf)
		} else {
			data, err = conn.Recv()
		}
		if err != nil {
			return
		}

		if hp := s.handler.Load(); hp != nil {
			// Call the handler inline: provides natural backpressure (we do not
			// read the next message until the current one is handled) and avoids
			// spawning an unbounded number of goroutines under high load.
			// Handlers that need concurrency should use their own worker pool.
			msg.Data = data
			(*hp)(msg)
		}
	}
}
