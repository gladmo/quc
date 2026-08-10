// QUIC client example for the quc unified protocol framework.
//
// Run the multi-protocol server first:
//
//	go run ./example/multi
//
// Then run this client:
//
//	go run ./example/client/quic
package main

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/gladmo/quc/internal/framing"
	quicgo "github.com/quic-go/quic-go"
)

func main() {
	// InsecureSkipVerify is used here only because the server uses a self-signed
	// certificate generated at runtime. Do not use this in production.
	tlsConf := &tls.Config{ //nolint:gosec
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qconn, err := quicgo.DialAddr(ctx, "localhost:9003", tlsConf, nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer qconn.CloseWithError(0, "done") //nolint:errcheck

	stream, err := qconn.OpenStreamSync(ctx)
	if err != nil {
		log.Fatalf("open stream: %v", err)
	}
	defer stream.Close() //nolint:errcheck

	msg := []byte("hello from quic client")
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
