// Package main demonstrates a multi-protocol echo server with a connection
// registry and 20-second idle heartbeat detection.
//
// The registry tracks every active connection (TCP, WebSocket, QUIC, KCP, and
// WebTransport) and closes any that have not sent a message within the idle
// timeout window.
//
// Usage:
//
//	go run ./example/registry
//
// Then exercise any of the five client examples (they connect to different ports
// from example/multi, so both can run at the same time):
//
//	# set the server address per client, e.g. for TCP:
//	go run ./example/client/tcp  # hardcoded to :9001 — edit addr or run separate client
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	quc "github.com/gladmo/quc"
	"github.com/gladmo/quc/plugin/kcp"
	"github.com/gladmo/quc/plugin/quic"
	"github.com/gladmo/quc/plugin/tcp"
	"github.com/gladmo/quc/plugin/websocket"
	"github.com/gladmo/quc/plugin/webtransport"
)

// heartbeatTimeout is the maximum time a connection may be idle before the
// registry closes it.
const heartbeatTimeout = 20 * time.Second

// certValidityPeriod is the lifetime of the self-signed TLS certificate
// generated for QUIC and WebTransport protocols.
const certValidityPeriod = 24 * time.Hour

// heartbeatInterval is how often the idle-check sweep runs.
// Set to one quarter of the timeout so idle connections are caught promptly.
const heartbeatInterval = heartbeatTimeout / 4

// connEntry holds a connection alongside the last time it was seen active.
type connEntry struct {
	conn         quc.Connection
	lastActivity time.Time
}

// ConnRegistry tracks all active connections across every transport protocol.
// It is safe to use from multiple goroutines.
type ConnRegistry struct {
	mu      sync.RWMutex
	entries map[string]*connEntry // keyed by Connection.ID()
}

// NewConnRegistry creates an empty ConnRegistry.
func NewConnRegistry() *ConnRegistry {
	return &ConnRegistry{
		entries: make(map[string]*connEntry),
	}
}

// Add registers a new connection. Called from the server's OnConnect hook.
func (r *ConnRegistry) Add(conn quc.Connection) {
	r.mu.Lock()
	r.entries[conn.ID()] = &connEntry{conn: conn, lastActivity: time.Now()}
	total := len(r.entries)
	r.mu.Unlock()
	log.Printf("[registry] add    proto=%-13s id=%s remote=%s total=%d",
		conn.Protocol(), conn.ID(), conn.RemoteAddr(), total)
}

// Remove unregisters a connection. Called from the server's OnDisconnect hook.
func (r *ConnRegistry) Remove(conn quc.Connection) {
	r.mu.Lock()
	delete(r.entries, conn.ID())
	total := len(r.entries)
	r.mu.Unlock()
	log.Printf("[registry] remove proto=%-13s id=%s total=%d",
		conn.Protocol(), conn.ID(), total)
}

// Touch updates the last-activity timestamp for conn.
// Called on every received message so the idle timer resets.
func (r *ConnRegistry) Touch(conn quc.Connection) {
	r.mu.Lock()
	if e, ok := r.entries[conn.ID()]; ok {
		e.lastActivity = time.Now()
	}
	r.mu.Unlock()
}

// Len returns the number of currently tracked connections.
func (r *ConnRegistry) Len() int {
	r.mu.RLock()
	n := len(r.entries)
	r.mu.RUnlock()
	return n
}

// Stats returns per-protocol connection counts.
func (r *ConnRegistry) Stats() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := make(map[string]int, 8)
	for _, e := range r.entries {
		m[e.conn.Protocol()]++
	}
	return m
}

// StartHeartbeat runs a background goroutine that closes every connection that
// has been idle for longer than timeout. The goroutine exits when ctx is done.
func (r *ConnRegistry) StartHeartbeat(ctx context.Context, timeout time.Duration) {
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				r.sweepIdle(now, timeout)
			}
		}
	}()
}

// sweepIdle closes connections that have been idle since before the deadline.
func (r *ConnRegistry) sweepIdle(now time.Time, timeout time.Duration) {
	deadline := now.Add(-timeout)

	// Collect idle connections under a read lock, then close outside the lock.
	// This prevents a deadlock if conn.Close() triggers the server's OnDisconnect
	// hook which calls Remove() (which needs the write lock).
	r.mu.RLock()
	var idle []quc.Connection
	for _, e := range r.entries {
		if e.lastActivity.Before(deadline) {
			idle = append(idle, e.conn)
		}
	}
	r.mu.RUnlock()

	for _, conn := range idle {
		log.Printf("[heartbeat] closing idle connection proto=%-13s id=%s (idle > %s)",
			conn.Protocol(), conn.ID(), timeout)
		if err := conn.Close(); err != nil {
			log.Printf("[heartbeat] close error proto=%s id=%s: %v",
				conn.Protocol(), conn.ID(), err)
		}
	}

	if r.Len() > 0 {
		log.Printf("[heartbeat] sweep done — active=%d stats=%v", r.Len(), r.Stats())
	}
}

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

func main() {
	tlsConf := selfSignedTLS()

	registry := NewConnRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the 20-second idle heartbeat sweep.
	registry.StartHeartbeat(ctx, heartbeatTimeout)

	srv := quc.NewServer()

	// Ports :9011-:9015 let this example run alongside example/multi (:9001-:9005).
	if err := srv.Register(tcp.New(), ":9011"); err != nil {
		log.Fatalf("register tcp: %v", err)
	}
	if err := srv.Register(websocket.New("/ws"), ":9012"); err != nil {
		log.Fatalf("register websocket: %v", err)
	}
	if err := srv.Register(quic.New(tlsConf), ":9013"); err != nil {
		log.Fatalf("register quic: %v", err)
	}
	if err := srv.Register(kcp.New(), ":9014"); err != nil {
		log.Fatalf("register kcp: %v", err)
	}
	if err := srv.Register(webtransport.New(tlsConf, "/wt"), ":9015"); err != nil {
		log.Fatalf("register webtransport: %v", err)
	}

	// Wire the registry into the server lifecycle hooks.
	srv.OnConnect(func(conn quc.Connection) {
		registry.Add(conn)
	})
	srv.OnDisconnect(func(conn quc.Connection) {
		registry.Remove(conn)
	})
	srv.OnMessage(func(msg *quc.Message) {
		// Reset the idle timer whenever a message arrives.
		registry.Touch(msg.Conn)

		log.Printf("[message] proto=%-13s id=%s len=%d",
			msg.Conn.Protocol(), msg.Conn.ID(), len(msg.Data))

		// Echo the payload back.
		if err := msg.Conn.Send(msg.Data); err != nil {
			log.Printf("[echo error] %v", err)
		}
	})

	if err := srv.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}

	log.Println("registry server started:")
	log.Println("  TCP          :9011")
	log.Println("  WebSocket    :9012/ws")
	log.Println("  QUIC         :9013")
	log.Println("  KCP          :9014")
	log.Println("  WebTransport :9015/wt")
	log.Printf("  idle timeout %s (sweep every %s)", heartbeatTimeout, heartbeatInterval)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	cancel() // stop the heartbeat goroutine
	if err := srv.Stop(); err != nil {
		log.Printf("stop error: %v", err)
	}
}

func selfSignedTLS() *tls.Config {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(certValidityPeriod),
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		log.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.Fatalf("key pair: %v", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"h3"},
	}
}
