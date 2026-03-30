package framing

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// MaxMessageSize is the maximum allowed message payload size (64 MiB).
const MaxMessageSize = 64 << 20

// Write writes data to w with a 4-byte big-endian length prefix.
// When w supports scatter-gather I/O (e.g. *net.TCPConn), the header and
// payload are delivered in a single writev syscall to halve kernel round-trips.
// For other writers net.Buffers falls back to sequential Write calls, which is
// equivalent to the original behaviour. Atomicity on the wire is guaranteed by
// the per-connection write mutex in each plugin's conn.Send() method.
func Write(w io.Writer, data []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	bufs := net.Buffers{hdr[:], data}
	_, err := bufs.WriteTo(w)
	return err
}

// Read reads one length-prefixed message from r.
func Read(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length > MaxMessageSize {
		return nil, fmt.Errorf("framing: message too large (%d bytes, max %d)", length, MaxMessageSize)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
