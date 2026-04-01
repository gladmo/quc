package quc

// Message is the unified representation of data received from a client connection.
type Message struct {
	// Conn is the connection that received this message.
	Conn Connection
	// Data contains the raw bytes of the received message.
	//
	// When the underlying Connection implements BufferedReceiver, Data aliases
	// a per-connection receive buffer that is reused across messages. In that
	// case Data is only valid for the duration of the Handler call — copy it
	// if you need to retain it past the Handler's return (e.g. when spawning
	// a goroutine from inside the handler).
	//
	// For connections that do not implement BufferedReceiver (e.g. WebSocket),
	// Data is an independently allocated slice that is safe to retain.
	Data []byte
}

// Handler processes incoming messages.
// Implementations must be safe to call concurrently.
type Handler func(msg *Message)
