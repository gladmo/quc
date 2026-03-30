// WebSocket client example for the quc unified protocol framework.
//
// Run the multi-protocol server first:
//
//	go run ./example/multi
//
// Then run this client:
//
//	go run ./example/client/websocket
package main

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:9002/ws", nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	msg := []byte("hello from websocket client")
	if err := conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent:  %s", msg)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	_, echo, err := conn.ReadMessage()
	if err != nil {
		log.Fatalf("recv: %v", err)
	}
	log.Printf("echo:  %s", echo)
}
