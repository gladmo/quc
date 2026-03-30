package quc

// Plugin is the interface that all transport protocol plugins must implement.
type Plugin interface {
	// Protocol returns the name of the transport protocol (e.g. "tcp", "websocket").
	Protocol() string
	// Listen starts listening on addr and returns when ready to accept connections.
	Listen(addr string) error
	// Accept waits for and returns the next incoming Connection.
	Accept() (Connection, error)
	// Close shuts down the listener and releases resources.
	Close() error
}
