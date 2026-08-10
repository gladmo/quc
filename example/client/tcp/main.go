// TCP client example for the quc unified protocol framework.
//
// Run the multi-protocol server first:
//
//	go run ./example/multi
//
// Then run this client:
//
//	go run ./example/client/tcp
package main

import (
	"log"
	"net"
	"time"

	"github.com/gladmo/quc/internal/framing"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9001")
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	msg := []byte("hello from tcp client")
	if err := framing.Write(conn, msg); err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent:  %s", msg)

	conn.SetDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	echo, err := framing.Read(conn)
	if err != nil {
		log.Fatalf("recv: %v", err)
	}
	log.Printf("echo:  %s", echo)
}
