// Copyright 2012 SocialCode. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package gelf

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

type Reader struct {
	mu   sync.Mutex
	conn net.Conn
}

func NewReader(addr string) (*Reader, error) {
	var err error
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("ResolveUDPAddr('%s'): %s", addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("ListenUDP: %s", err)
	}

	r := new(Reader)
	r.conn = conn
	return r, nil
}

func (r *Reader) Addr() string {
	return r.conn.LocalAddr().String()
}

// FIXME: this will discard data if p isn't big enough to hold the
// full message.
func (r *Reader) Read(p []byte) (int, error) {
	msg, err := r.ReadMessage()
	if err != nil {
		return -1, err
	}

	var data string

	if msg.Full == "" {
		data = msg.Short
	} else {
		data = msg.Full
	}

	return strings.NewReader(data).Read(p)
}

func (r *Reader) ReadMessage() (*Message, error) {
	// the data we get from the wire is compressed
	cBuf := make([]byte, ChunkSize)

	n, err := r.conn.Read(cBuf)
	if err != nil {
		return nil, fmt.Errorf("Read: %s", err)
	}
	cHead, cBuf := cBuf[:2], cBuf[:n]

	var cReader io.Reader
	if bytes.Equal(cHead, magicGzip) {
		cReader, err = gzip.NewReader(bytes.NewReader(cBuf))
	} else if cHead[0] == magicZlib[0] &&
		(int(cHead[0])*256+int(cHead[1]))%31 == 0 {
		// zlib is slightly more complicated, but correct
		cReader, err = zlib.NewReader(bytes.NewReader(cBuf))
	} else {
		return nil, fmt.Errorf("unknown magic: %x", cHead)
	}

	if err != nil {
		return nil, fmt.Errorf("NewReader: %s", err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, cReader)
	if err != nil {
		return nil, fmt.Errorf("io.Copy: %s", err)
	}

	msg := new(Message)
	if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %s", err)
	}

	return msg, nil
}
