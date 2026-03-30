// WebTransport client example for the quc unified protocol framework.
//
// Run the multi-protocol server first:
//
//	go run ./example/multi
//
// Then run this client:
//
//	go run ./example/client/webtransport
package main

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/gladmo/quc/internal/framing"
	wt "github.com/quic-go/webtransport-go"
)

func main() {
	// InsecureSkipVerify is used here only because the server uses a self-signed
	// certificate generated at runtime. Do not use this in production.
	dialer := &wt.Dialer{
		TLSClientConfig: &tls.Config{ //nolint:gosec
			InsecureSkipVerify: true,
			NextProtos:         []string{"h3"},
		},
	}
	defer dialer.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, sess, err := dialer.Dial(ctx, "https://localhost:9005/wt", nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer sess.CloseWithError(0, "done") //nolint:errcheck

	stream, err := sess.OpenStreamSync(ctx)
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}
	defer stream.Close() //nolint:errcheck

	msg := []byte("hello from webtransport client")
	if err := framing.Write(stream, msg); err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent:  %s", msg)

	stream.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	echo, err := framing.Read(stream)
	if err != nil {
		log.Fatalf("recv: %v", err)
	}
	log.Printf("echo:  %s", echo)
}
