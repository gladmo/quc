package main

import (
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
	"syscall"
	"time"

	quc "github.com/gladmo/quc"
	"github.com/gladmo/quc/plugin/kcp"
	"github.com/gladmo/quc/plugin/quic"
	"github.com/gladmo/quc/plugin/tcp"
	"github.com/gladmo/quc/plugin/websocket"
	"github.com/gladmo/quc/plugin/webtransport"
)

func main() {
	tlsConf := selfSignedTLS()

	srv := quc.NewServer()

	if err := srv.Register(tcp.New(), ":9001"); err != nil {
		log.Fatalf("register tcp: %v", err)
	}
	if err := srv.Register(websocket.New("/ws"), ":9002"); err != nil {
		log.Fatalf("register websocket: %v", err)
	}
	if err := srv.Register(quic.New(tlsConf), ":9003"); err != nil {
		log.Fatalf("register quic: %v", err)
	}
	if err := srv.Register(kcp.New(), ":9004"); err != nil {
		log.Fatalf("register kcp: %v", err)
	}
	if err := srv.Register(webtransport.New(tlsConf, "/wt"), ":9005"); err != nil {
		log.Fatalf("register webtransport: %v", err)
	}

	srv.OnConnect(func(conn quc.Connection) {
		log.Printf("[connect] protocol=%s id=%s remote=%s", conn.Protocol(), conn.ID(), conn.RemoteAddr())
	})

	srv.OnDisconnect(func(conn quc.Connection) {
		log.Printf("[disconnect] protocol=%s id=%s", conn.Protocol(), conn.ID())
	})

	srv.OnMessage(func(msg *quc.Message) {
		log.Printf("[message] protocol=%s id=%s len=%d", msg.Conn.Protocol(), msg.Conn.ID(), len(msg.Data))
		if err := msg.Conn.Send(msg.Data); err != nil {
			log.Printf("[echo error] %v", err)
		}
	})

	if err := srv.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}

	log.Println("server started on :9001 (tcp) :9002 (ws) :9003 (quic) :9004 (kcp) :9005 (wt)")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
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
		NotAfter:     time.Now().Add(24 * time.Hour),
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
