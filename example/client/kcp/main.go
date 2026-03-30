// KCP client example for the quc unified protocol framework.
//
// Run the multi-protocol server first:
//
//	go run ./example/multi
//
// Then run this client:
//
//	go run ./example/client/kcp
package main

import (
	"log"
	"time"

	"github.com/gladmo/quc/internal/framing"
	kcp "github.com/xtaci/kcp-go/v5"
)

func main() {
	conn, err := kcp.Dial("localhost:9004")
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	msg := []byte("hello from kcp client")
	if err := framing.Write(conn, msg); err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent:  %s", msg)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	echo, err := framing.Read(conn)
	if err != nil {
		log.Fatalf("recv: %v", err)
	}
	log.Printf("echo:  %s", echo)
}
