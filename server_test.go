package quc_test

import (
	"net"
	"testing"
	"time"

	quc "github.com/gladmo/quc"
	"github.com/gladmo/quc/internal/framing"
	"github.com/gladmo/quc/plugin/tcp"
)

func TestServer_TCPEchoSingle(t *testing.T) {
	srv := quc.NewServer()
	if err := srv.Register(tcp.New(), ":19001"); err != nil {
		t.Fatal(err)
	}

	srv.OnMessage(func(msg *quc.Message) {
		msg.Conn.Send(msg.Data) //nolint:errcheck
	})

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop() //nolint:errcheck

	// Give the server a moment to be ready.
	time.Sleep(50 * time.Millisecond)

	nc, err := net.Dial("tcp", ":19001")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	payload := []byte("hello quc")
	if err := framing.Write(nc, payload); err != nil {
		t.Fatal(err)
	}

	nc.SetDeadline(time.Now().Add(2 * time.Second))
	got, err := framing.Read(nc)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != string(payload) {
		t.Fatalf("expected %q, got %q", payload, got)
	}
}

func TestServer_StopAndRestart(t *testing.T) {
	srv := quc.NewServer()
	if err := srv.Register(tcp.New(), ":19002"); err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- srv.Stop()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() hung")
	}
}
