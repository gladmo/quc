package websocket

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	quc "github.com/gladmo/quc"
	gws "github.com/gorilla/websocket"
)

var idCounter uint64

var upgrader = gws.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type plugin struct {
	path       string
	connCh     chan quc.Connection
	done       chan struct{}
	closeOnce  sync.Once
	httpServer *http.Server
}

// New creates a new WebSocket plugin. The optional path argument sets the HTTP
// path (defaults to "/ws").
func New(path ...string) quc.Plugin {
	p := "/ws"
	if len(path) > 0 && path[0] != "" {
		p = path[0]
	}
	return &plugin{
		path:   p,
		connCh: make(chan quc.Connection, 128),
		done:   make(chan struct{}),
	}
}

func (p *plugin) Protocol() string { return "websocket" }

func (p *plugin) Listen(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc(p.path, p.handleWS)

	p.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	ready := make(chan error, 1)
	go func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			ready <- err
			return
		}
		ready <- nil
		p.httpServer.Serve(ln) //nolint:errcheck
	}()

	return <-ready
}

func (p *plugin) handleWS(w http.ResponseWriter, r *http.Request) {
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	id := atomic.AddUint64(&idCounter, 1)
	c := &conn{
		wsConn:   wsConn,
		id:       fmt.Sprintf("ws-%d", id),
		connDone: make(chan struct{}),
	}
	select {
	case p.connCh <- c:
	case <-p.done:
		wsConn.Close()
		return
	}
	// Keep handler alive until the connection or plugin is closed so the
	// HTTP/WebSocket upgrade remains valid for the lifetime of the session.
	select {
	case <-c.connDone:
	case <-p.done:
	}
}

func (p *plugin) Accept() (quc.Connection, error) {
	select {
	case c := <-p.connCh:
		return c, nil
	case <-p.done:
		return nil, fmt.Errorf("websocket: plugin closed")
	}
}

func (p *plugin) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	if p.httpServer != nil {
		return p.httpServer.Shutdown(context.Background())
	}
	return nil
}

type conn struct {
	wsConn    *gws.Conn
	id        string
	connDone  chan struct{}
	closeOnce sync.Once
	wmu       sync.Mutex // gorilla WriteMessage is not goroutine-safe for concurrent writes
}

func (c *conn) ID() string           { return c.id }
func (c *conn) Protocol() string     { return "websocket" }
func (c *conn) RemoteAddr() net.Addr { return c.wsConn.RemoteAddr() }

func (c *conn) Close() error {
	err := c.wsConn.Close()
	c.closeOnce.Do(func() { close(c.connDone) })
	return err
}

func (c *conn) Send(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.wsConn.WriteMessage(gws.BinaryMessage, data)
}

func (c *conn) Recv() ([]byte, error) {
	_, data, err := c.wsConn.ReadMessage()
	return data, err
}
