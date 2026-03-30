package quc

import "net"

// Connection represents a unified client connection regardless of the underlying transport protocol.
type Connection interface {
	// ID returns a unique identifier for this connection.
	ID() string
	// Protocol returns the name of the transport protocol used by this connection.
	Protocol() string
	// RemoteAddr returns the remote network address of the peer.
	RemoteAddr() net.Addr
	// Send writes data to the connection.
	Send(data []byte) error
	// Recv reads the next message from the connection.
	// It blocks until a message is received or the connection is closed.
	Recv() ([]byte, error)
	// Close closes the connection.
	Close() error
}
