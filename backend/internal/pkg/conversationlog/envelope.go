package conversationlog

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
)

type envelopePart struct {
	prefix, suffix []byte
	chunks         [][]byte
}

func (a *attempt) bodyChunks(request bool) [][]byte {
	if a.row.Protocol == "WebSocket" {
		if request {
			return [][]byte{a.row.Request}
		}
		return [][]byte{a.row.Response}
	}
	if request {
		return a.requestChunks
	}
	return a.responseChunks
}
func chunksLength(chunks [][]byte) int64 {
	var n int64
	for _, chunk := range chunks {
		n += int64(len(chunk))
	}
	return n
}
func (a *attempt) capturedDigest() string {
	h := sha256.New()
	number := func(n uint64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], n); _, _ = h.Write(b[:]) }
	body := func(chunks [][]byte) {
		number(uint64(chunksLength(chunks)))
		for _, chunk := range chunks {
			_, _ = h.Write(chunk)
		}
	}
	body(a.bodyChunks(true))
	body(a.bodyChunks(false))
	number(uint64(len(a.row.Frames)))
	for _, f := range a.row.Frames {
		_, _ = h.Write([]byte(f.Direction))
		number(uint64(f.At))
		b := byte(0)
		if f.Binary {
			b = 1
		}
		_, _ = h.Write([]byte{b})
		body([][]byte{f.Body})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (a *attempt) envelopeParts() ([]envelopePart, error) {
	// Shadow payload fields so encoding/json handles metadata without buffering the bodies.
	metadata, err := json.Marshal(struct {
		event
		Request  *json.RawMessage `json:"request,omitempty"`
		Response *json.RawMessage `json:"response,omitempty"`
		Frames   *json.RawMessage `json:"frames,omitempty"`
	}{event: a.row})
	if err != nil {
		return nil, err
	}
	parts := []envelopePart{{prefix: metadata[:len(metadata)-1]},
		{prefix: []byte(`,"request":"`), chunks: a.bodyChunks(true), suffix: []byte(`"`)},
		{prefix: []byte(`,"response":"`), chunks: a.bodyChunks(false), suffix: []byte(`"`)},
		{prefix: []byte(`,"frames":[`)}}
	for i, f := range a.row.Frames {
		meta, encodeErr := json.Marshal(struct {
			Direction string `json:"direction"`
			At        int64  `json:"at"`
			Binary    bool   `json:"binary"`
		}{f.Direction, f.At, f.Binary})
		if encodeErr != nil {
			return nil, encodeErr
		}
		prefix := []byte{}
		if i > 0 {
			prefix = append(prefix, ',')
		}
		prefix = append(prefix, meta[:len(meta)-1]...)
		prefix = append(prefix, `,"body":"`...)
		parts = append(parts, envelopePart{prefix: prefix, chunks: [][]byte{f.Body}, suffix: []byte(`"}`)})
	}
	return append(parts, envelopePart{prefix: []byte(`]}`)}), nil
}

func (a *attempt) streamEnvelope() (io.ReadCloser, int64, func(), error) {
	parts, err := a.envelopeParts()
	if err != nil {
		return nil, 0, nil, err
	}
	var length int64
	for _, part := range parts {
		length += int64(len(part.prefix)+len(part.suffix)) + int64(base64.StdEncoding.EncodedLen(int(chunksLength(part.chunks))))
	}
	reader, writer := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := bufio.NewWriterSize(writer, 65536)
		writeErr := writeEnvelope(buffer, parts)
		if writeErr == nil {
			writeErr = buffer.Flush()
		}
		_ = writer.CloseWithError(writeErr)
	}()
	stop := func() { _ = reader.Close(); <-done }
	return reader, length, stop, nil
}

func writeEnvelope(writer io.Writer, parts []envelopePart) error {
	for _, part := range parts {
		if _, err := writer.Write(part.prefix); err != nil {
			return err
		}
		encoder := base64.NewEncoder(base64.StdEncoding, writer)
		for _, chunk := range part.chunks {
			if _, err := encoder.Write(chunk); err != nil {
				return err
			}
		}
		if err := encoder.Close(); err != nil {
			return err
		}
		if _, err := writer.Write(part.suffix); err != nil {
			return err
		}
	}
	return nil
}
