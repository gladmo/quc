package webtransport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	quc "github.com/gladmo/quc"
	"github.com/gladmo/quc/internal/framing"
	"github.com/quic-go/quic-go/http3"
	wt "github.com/quic-go/webtransport-go"
)

type plugin struct {
	tlsConf   *tls.Config
	path      string
	wtServer  *wt.Server
	connCh    chan quc.Connection
	done      chan struct{}
	closeOnce sync.Once
	idCounter uint64
}

// New creates a new WebTransport plugin. The optional path argument sets the
// HTTP path (defaults to "/wt").
func New(tlsConf *tls.Config, path string) quc.Plugin {
	if path == "" {
		path = "/wt"
	}
	return &plugin{
		tlsConf: tlsConf,
		path:    path,
		connCh:  make(chan quc.Connection, 128),
		done:    make(chan struct{}),
	}
}

func (p *plugin) Protocol() string { return "webtransport" }

func (p *plugin) Listen(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc(p.path, p.handleWT)

	h3srv := &http3.Server{
		Addr:      addr,
		TLSConfig: p.tlsConf,
		Handler:   mux,
	}
	// ConfigureHTTP3Server sets EnableDatagrams and the WebTransport SETTINGS
	// that must be advertised to clients before they can open sessions.
	wt.ConfigureHTTP3Server(h3srv)

	p.wtServer = &wt.Server{H3: h3srv}

	go p.wtServer.ListenAndServe() //nolint:errcheck
	return nil
}

func (p *plugin) handleWT(w http.ResponseWriter, r *http.Request) {
	sess, err := p.wtServer.Upgrade(w, r)
	if err != nil {
		return
	}

	stream, err := sess.AcceptStream(r.Context())
	if err != nil {
		sess.CloseWithError(0, "stream accept failed")
		return
	}

	id := atomic.AddUint64(&p.idCounter, 1)
	c := &conn{
		sess:   sess,
		stream: stream,
		id:     fmt.Sprintf("wt-%d", id),
	}

	select {
	case p.connCh <- c:
	case <-p.done:
		sess.CloseWithError(0, "plugin closed")
		return
	}

	// Keep the handler alive until the session ends so the HTTP/3 server
	// does not reclaim the underlying QUIC connection.
	select {
	case <-sess.Context().Done():
	case <-p.done:
	}
}

func (p *plugin) Accept() (quc.Connection, error) {
	select {
	case c := <-p.connCh:
		return c, nil
	case <-p.done:
		return nil, fmt.Errorf("webtransport: plugin closed")
	}
}

func (p *plugin) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	if p.wtServer != nil {
		return p.wtServer.Close()
	}
	return nil
}

type conn struct {
	sess   *wt.Session
	stream *wt.Stream
	id     string
	wmu    sync.Mutex // guards Send against concurrent callers
}

func (c *conn) ID() string           { return c.id }
func (c *conn) Protocol() string     { return "webtransport" }
func (c *conn) RemoteAddr() net.Addr { return c.sess.RemoteAddr() }

func (c *conn) Close() error {
	return c.sess.CloseWithError(0, "closed")
}

func (c *conn) Send(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return framing.Write(c.stream, data)
}

func (c *conn) Recv() ([]byte, error) {
	return framing.Read(c.stream)
}

func (c *conn) RecvInto(buf *[]byte) ([]byte, error) {
	return framing.ReadInto(c.stream, buf)
}

// Ensure context is used to satisfy import.
var _ = context.Background
