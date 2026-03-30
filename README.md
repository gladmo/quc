# quc

**quc** is a unified protocol framework for Go that abstracts TCP, QUIC, KCP, WebSocket, and WebTransport into a single, plugin-based API. Register any number of protocols and handle all incoming messages through one callback.

## Features

- **Unified API** — one `Handler` for all protocols
- **Plugin architecture** — swap or add transports without changing application logic
- **Concurrent** — each connection is handled in its own goroutine
- **Lifecycle hooks** — `OnConnect` / `OnDisconnect` callbacks
- **Clean shutdown** — `Stop()` drains goroutines gracefully

## Supported Transports

| Plugin | Package | Notes |
|--------|---------|-------|
| TCP | `github.com/gladmo/quc/plugin/tcp` | Length-prefixed framing |
| WebSocket | `github.com/gladmo/quc/plugin/websocket` | gorilla/websocket |
| QUIC | `github.com/gladmo/quc/plugin/quic` | TLS required |
| KCP | `github.com/gladmo/quc/plugin/kcp` | UDP-based reliable transport |
| WebTransport | `github.com/gladmo/quc/plugin/webtransport` | HTTP/3, TLS required |

## Installation

```bash
go get github.com/gladmo/quc
```

## Quick Start

```go
package main

import (
    "log"

    quc "github.com/gladmo/quc"
    "github.com/gladmo/quc/plugin/tcp"
    "github.com/gladmo/quc/plugin/websocket"
)

func main() {
    srv := quc.NewServer()

    srv.Register(tcp.New(), ":8080")
    srv.Register(websocket.New("/ws"), ":8081")

    srv.OnConnect(func(conn quc.Connection) {
        log.Printf("connected: %s %s", conn.Protocol(), conn.ID())
    })

    srv.OnDisconnect(func(conn quc.Connection) {
        log.Printf("disconnected: %s %s", conn.Protocol(), conn.ID())
    })

    srv.OnMessage(func(msg *quc.Message) {
        // Echo the message back
        msg.Conn.Send(msg.Data)
    })

    if err := srv.Start(); err != nil {
        log.Fatal(err)
    }

    select {} // block forever
}
```

## Multi-Protocol Example

See [`example/multi/main.go`](example/multi/main.go) for a complete server that listens on TCP, WebSocket, QUIC, KCP, and WebTransport simultaneously.

```bash
go run ./example/multi
```

## Architecture

### Core Interfaces

#### `Plugin`

```go
type Plugin interface {
    Protocol() string
    Listen(addr string) error
    Accept() (Connection, error)
    Close() error
}
```

#### `Connection`

```go
type Connection interface {
    ID() string
    Protocol() string
    RemoteAddr() net.Addr
    Send(data []byte) error
    Recv() ([]byte, error)
    Close() error
}
```

#### `Message`

```go
type Message struct {
    Conn Connection
    Data []byte
}
```

### Server

```go
srv := quc.NewServer()
srv.Register(plugin, addr)   // register a transport plugin
srv.OnMessage(handler)       // set the unified message handler
srv.OnConnect(handler)       // optional: connection lifecycle hook
srv.OnDisconnect(handler)    // optional: disconnection lifecycle hook
srv.Start()                  // begin accepting connections
srv.Stop()                   // graceful shutdown
```

### Message Framing

TCP, QUIC, KCP, and WebTransport plugins use a **4-byte big-endian length prefix** (provided by `internal/framing`) to delimit messages on stream-oriented transports. WebSocket uses its own native message framing.

## Writing a Custom Plugin

Implement the `quc.Plugin` and `quc.Connection` interfaces:

```go
type myPlugin struct{ /* ... */ }

func (p *myPlugin) Protocol() string              { return "my-protocol" }
func (p *myPlugin) Listen(addr string) error      { /* start listener */ }
func (p *myPlugin) Accept() (quc.Connection, error) { /* return next conn */ }
func (p *myPlugin) Close() error                  { /* shutdown */ }
```

Then register it like any built-in plugin:

```go
srv.Register(&myPlugin{}, ":9999")
```

## Module Requirements

```
go 1.21+
github.com/gorilla/websocket v1.5.3
github.com/quic-go/quic-go  v0.59.0
github.com/quic-go/webtransport-go v0.10.0
github.com/xtaci/kcp-go/v5  v5.6.72
```

## License

MIT
