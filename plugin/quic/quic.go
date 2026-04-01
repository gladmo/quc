package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	quc "github.com/gladmo/quc"
	"github.com/gladmo/quc/internal/framing"
	quicgo "github.com/quic-go/quic-go"
)

type plugin struct {
	tlsConf   *tls.Config
	listener  *quicgo.Listener
	connCh    chan quc.Connection
	done      chan struct{}
	closeOnce sync.Once
	idCounter uint64
}

// New creates a new QUIC plugin. TLS configuration is required for QUIC.
func New(tlsConf *tls.Config) quc.Plugin {
	return &plugin{
		tlsConf: tlsConf,
		connCh:  make(chan quc.Connection, 128),
		done:    make(chan struct{}),
	}
}

func (p *plugin) Protocol() string { return "quic" }

func (p *plugin) Listen(addr string) error {
	ln, err := quicgo.ListenAddr(addr, p.tlsConf, nil)
	if err != nil {
		return err
	}
	p.listener = ln
	go p.listenLoop()
	return nil
}

func (p *plugin) listenLoop() {
	for {
		qconn, err := p.listener.Accept(context.Background())
		if err != nil {
			select {
			case <-p.done:
				return
			default:
				// Transient accept error — back off to avoid tight busy-loop.
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}
		go p.streamLoop(qconn)
	}
}

func (p *plugin) streamLoop(qconn *quicgo.Conn) {
	for {
		stream, err := qconn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		id := atomic.AddUint64(&p.idCounter, 1)
		c := &conn{
			stream: stream,
			qconn:  qconn,
			id:     "quic-" + strconv.FormatUint(id, 10),
		}
		select {
		case p.connCh <- c:
		case <-p.done:
			stream.Close()
			return
		}
	}
}

func (p *plugin) Accept() (quc.Connection, error) {
	select {
	case c := <-p.connCh:
		return c, nil
	case <-p.done:
		return nil, fmt.Errorf("quic: plugin closed")
	}
}

func (p *plugin) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}

type conn struct {
	stream *quicgo.Stream
	qconn  *quicgo.Conn
	id     string
	wmu    sync.Mutex // guards Send against concurrent callers
}

func (c *conn) ID() string           { return c.id }
func (c *conn) Protocol() string     { return "quic" }
func (c *conn) RemoteAddr() net.Addr { return c.qconn.RemoteAddr() }
func (c *conn) Close() error         { return c.stream.Close() }

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
