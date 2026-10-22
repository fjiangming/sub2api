package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/tidwall/gjson"
)

// Only usage objects are persisted. The bounded observer never stores response text or credentials.
type recoveryResponseBody struct {
	io.ReadCloser
	id, requestID  string
	status         int
	buffer         []byte
	usage          map[string]json.RawMessage
	sse            bool
	once           sync.Once
	mu             sync.Mutex
	eof, truncated bool
	lastSaved      time.Time
}

func observeRecoveryJSON(data []byte, usage map[string]json.RawMessage) {
	for _, path := range []string{"usage", "usageMetadata", "response.usage", "message.usage"} {
		value := gjson.GetBytes(data, path)
		if value.IsObject() {
			for key, field := range value.Map() {
				if field.Type == gjson.Number {
					usage[key] = json.RawMessage(field.Raw)
				}
				if field.IsObject() {
					if cleaned := recoveryNumericObject(field, 0); len(cleaned) > 0 {
						usage[key] = cleaned
					}
				}
			}
		}
	}
}
func recoveryNumericObject(value gjson.Result, depth int) json.RawMessage {
	if depth > 4 {
		return nil
	}
	fields := map[string]json.RawMessage{}
	for key, item := range value.Map() {
		if item.Type == gjson.Number {
			fields[key] = json.RawMessage(item.Raw)
		} else if item.IsObject() {
			if data := recoveryNumericObject(item, depth+1); len(data) > 0 {
				fields[key] = data
			}
		}
	}
	if len(fields) == 0 {
		return nil
	}
	data, _ := json.Marshal(fields)
	return data
}
func (b *recoveryResponseBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		if len(b.buffer)+n <= 2<<20 {
			b.buffer = append(b.buffer, p[:n]...)
		} else {
			b.truncated = true
			b.buffer = nil
		}
	}
	if b.sse {
		for {
			index := bytes.IndexByte(b.buffer, '\n')
			if index < 0 {
				break
			}
			line := strings.TrimSpace(string(b.buffer[:index]))
			b.buffer = b.buffer[index+1:]
			if strings.HasPrefix(line, "data:") {
				observeRecoveryJSON([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), b.usage)
			}
		}
	}
	if len(b.usage) > 0 && time.Since(b.lastSaved) > time.Second {
		b.persist("")
		b.lastSaved = time.Now()
	}
	if err == io.EOF {
		b.eof = true
	}
	if err != nil {
		b.finish(err)
	}
	return n, err
}
func (b *recoveryResponseBody) finish(readErr error) {
	b.once.Do(func() {
		if !b.sse {
			observeRecoveryJSON(b.buffer, b.usage)
		} else if line := bytes.TrimSpace(b.buffer); bytes.HasPrefix(line, []byte("data:")) {
			observeRecoveryJSON(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:"))), b.usage)
		}
		code := ""
		if !b.eof || readErr != nil && readErr != io.EOF {
			code = "UPSTREAM_BODY_INTERRUPTED"
		}
		if b.truncated {
			code = "USAGE_OBSERVATION_TRUNCATED"
		}
		b.persist(code)
		b.buffer = nil
	})
}
func (b *recoveryResponseBody) persist(code string) {
	store := service.BillingRecovery()
	if store == nil {
		return
	}
	data, _ := json.Marshal(b.usage)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.Observe(ctx, b.id, b.status, b.requestID, data, code); err != nil {
		slog.Error("billing recovery observation persistence failed", "case_id", b.id)
	}
}
func (b *recoveryResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.finish(nil)
	return err
}
func observeBillingRecoveryResponse(id string, resp *http.Response, err error) {
	if id == "" || service.BillingRecovery() == nil {
		return
	}
	if err != nil || resp == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = service.BillingRecovery().Observe(ctx, id, 0, "", nil, "UPSTREAM_RESULT_UNKNOWN")
		return
	}
	requestID := resp.Header.Get("x-request-id")
	if requestID == "" {
		requestID = resp.Header.Get("request-id")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	observeErr := service.BillingRecovery().Observe(ctx, id, resp.StatusCode, requestID, nil, "")
	cancel()
	if observeErr != nil {
		slog.Error("billing recovery header persistence failed", "case_id", id)
	}
	if resp.Body != nil {
		resp.Body = &recoveryResponseBody{ReadCloser: resp.Body, id: id, requestID: requestID, status: resp.StatusCode,
			sse: strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream"), usage: make(map[string]json.RawMessage)}
	}
}
