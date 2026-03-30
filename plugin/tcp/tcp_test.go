package tcp_test

import (
	"net"
	"testing"
	"time"

	"github.com/gladmo/quc/plugin/tcp"
)

func TestTCPPlugin_ListenAcceptClose(t *testing.T) {
	p := tcp.New()
	if err := p.Listen(":19100"); err != nil {
		t.Fatal(err)
	}

	connCh := make(chan error, 1)
	go func() {
		conn, err := p.Accept()
		if err != nil {
			connCh <- err
			return
		}
		conn.Close() //nolint:errcheck
		connCh <- nil
	}()

	time.Sleep(20 * time.Millisecond)

	nc, err := net.Dial("tcp", ":19100")
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	defer nc.Close()

	select {
	case err := <-connCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept() timed out")
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
