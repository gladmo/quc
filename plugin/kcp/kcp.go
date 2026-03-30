package kcp

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	quc "github.com/gladmo/quc"
	"github.com/gladmo/quc/internal/framing"
	kcp "github.com/xtaci/kcp-go/v5"
)

type plugin struct {
	listener  net.Listener
	idCounter uint64
}

// New creates a new KCP plugin.
func New() quc.Plugin {
	return &plugin{}
}

func (p *plugin) Protocol() string { return "kcp" }

func (p *plugin) Listen(addr string) error {
	ln, err := kcp.Listen(addr)
	if err != nil {
		return err
	}
	p.listener = ln
	return nil
}

func (p *plugin) Accept() (quc.Connection, error) {
	nc, err := p.listener.Accept()
	if err != nil {
		return nil, err
	}
	id := atomic.AddUint64(&p.idCounter, 1)
	return &conn{
		nc: nc,
		id: fmt.Sprintf("kcp-%d", id),
	}, nil
}

func (p *plugin) Close() error {
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}

type conn struct {
	nc  net.Conn
	id  string
	wmu sync.Mutex // guards Send against concurrent callers
}

func (c *conn) ID() string            { return c.id }
func (c *conn) Protocol() string      { return "kcp" }
func (c *conn) RemoteAddr() net.Addr  { return c.nc.RemoteAddr() }
func (c *conn) Close() error          { return c.nc.Close() }

func (c *conn) Send(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return framing.Write(c.nc, data)
}

func (c *conn) Recv() ([]byte, error) {
	return framing.Read(c.nc)
}
