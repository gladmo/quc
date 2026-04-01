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

// BufferedReceiver is an optional interface that Connection implementations
// may satisfy to enable zero-copy message receive.
//
// RecvInto reads the next message into buf, growing the backing array when
// needed. It returns the sub-slice that contains the message payload; that
// slice aliases buf and is valid only until the next RecvInto call on the
// same buffer. Callers must copy the data if they need to retain it past the
// Handler's return.
//
// The Server automatically uses RecvInto (with a per-connection buffer) when a
// Connection implements BufferedReceiver, avoiding one heap allocation per
// received message.
type BufferedReceiver interface {
	RecvInto(buf *[]byte) ([]byte, error)
}
