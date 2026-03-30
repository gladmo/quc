package quc

import (
	"fmt"
	"sync"
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
	entries    []entry
	handler    Handler
	onConnect  func(Connection)
	onDisconn  func(Connection)
	closed     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
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
func (s *Server) OnMessage(h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = h
}

// OnConnect sets the callback invoked when a new connection is established.
func (s *Server) OnConnect(h func(Connection)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onConnect = h
}

// OnDisconnect sets the callback invoked when a connection is closed.
func (s *Server) OnDisconnect(h func(Connection)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDisconn = h
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
func (s *Server) Stop() error {
	close(s.closed)

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

func (s *Server) acceptLoop(p Plugin) {
	defer s.wg.Done()
	for {
		conn, err := p.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
				continue
			}
		}

		s.mu.RLock()
		onConn := s.onConnect
		s.mu.RUnlock()
		if onConn != nil {
			onConn(conn)
		}

		s.wg.Add(1)
		go s.readLoop(conn)
	}
}

func (s *Server) readLoop(conn Connection) {
	defer s.wg.Done()
	defer func() {
		conn.Close()
		s.mu.RLock()
		onDisconn := s.onDisconn
		s.mu.RUnlock()
		if onDisconn != nil {
			onDisconn(conn)
		}
	}()

	for {
		data, err := conn.Recv()
		if err != nil {
			return
		}

		s.mu.RLock()
		h := s.handler
		s.mu.RUnlock()

		if h != nil {
			msg := &Message{Conn: conn, Data: data}
			go h(msg)
		}
	}
}
