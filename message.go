package quc

// Message is the unified representation of data received from a client connection.
type Message struct {
	// Conn is the connection that received this message.
	Conn Connection
	// Data contains the raw bytes of the received message.
	Data []byte
}

// Handler processes incoming messages.
// Implementations must be safe to call concurrently.
type Handler func(msg *Message)
