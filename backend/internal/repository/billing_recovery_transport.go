package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
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
	started        time.Time
	terminal       bool
	finalUsage     bool
	protocol       string
	requestIDs     map[string]string
	frame          []byte
	ctx            context.Context
	choices        map[int]bool
	done, failed   bool
	invalidUsage   bool
}

func observeRecoveryJSON(data []byte, usage map[string]json.RawMessage) bool {
	invalid := false
	for _, path := range []string{"usage", "usageMetadata", "response.usage", "response.usageMetadata", "message.usage", "metadata.usage", "amazon-bedrock-invocationMetrics"} {
		value := gjson.GetBytes(data, path)
		if value.IsObject() {
			invalid = recoveryInvalidUsage(value, 0) || invalid
			for key, field := range value.Map() {
				if field.Type == gjson.Number {
					usage[key] = json.RawMessage(field.Raw)
				}
				if field.IsObject() {
					if cleaned := recoveryNumericObject(field, 0); len(cleaned) > 0 {
						usage[key] = mergeRecoveryNumericJSON(usage[key], cleaned)
					}
				}
			}
		}
	}
	return invalid
}

func recoveryInvalidUsage(value gjson.Result, depth int) bool {
	if depth > 4 {
		return true
	}
	for key, field := range value.Map() {
		name := strings.ToLower(key)
		if (strings.HasSuffix(name, "tokens") || strings.HasSuffix(name, "tokencount")) && !recoveryValidCount(field) {
			return true
		}
		if field.IsObject() && recoveryInvalidUsage(field, depth+1) {
			return true
		}
	}
	return false
}

func mergeRecoveryNumericJSON(previous, next json.RawMessage) json.RawMessage {
	var old, fields map[string]json.RawMessage
	_ = json.Unmarshal(previous, &old)
	_ = json.Unmarshal(next, &fields)
	if old == nil {
		old = map[string]json.RawMessage{}
	}
	for key, value := range fields {
		if len(value) > 0 && value[0] == '{' {
			old[key] = mergeRecoveryNumericJSON(old[key], value)
		} else {
			old[key] = value
		}
	}
	data, _ := json.Marshal(old)
	return data
}

func (b *recoveryResponseBody) observe(data []byte) {
	if !json.Valid(data) {
		return
	}
	b.invalidUsage = observeRecoveryJSON(data, b.usage) || b.invalidUsage
	typeName := gjson.GetBytes(data, "type").String()
	if typeName == "error" || typeName == "response.failed" || gjson.GetBytes(data, "error").Exists() {
		b.failed = true
	}
	if typeName == "message" || strings.HasPrefix(typeName, "message_") || strings.HasPrefix(typeName, "content_block_") {
		b.protocol = "anthropic"
	} else if strings.HasPrefix(typeName, "response.") || gjson.GetBytes(data, "object").String() == "response" ||
		gjson.GetBytes(data, "output").IsArray() && gjson.GetBytes(data, "usage.input_tokens").Exists() {
		b.protocol = "responses"
	} else if gjson.GetBytes(data, "usageMetadata").Exists() || gjson.GetBytes(data, "response.usageMetadata").Exists() {
		b.protocol = "gemini"
	} else if gjson.GetBytes(data, "choices").Exists() {
		b.protocol = "chat"
	}
	switch typeName {
	case "message_stop", "response.completed", "response.incomplete":
		b.terminal = true
		b.done = true
	}
	for _, path := range []string{"choices", "candidates", "response.candidates"} {
		for position, item := range gjson.GetBytes(data, path).Array() {
			index := position
			if item.Get("index").Exists() {
				index = int(item.Get("index").Int())
			}
			if b.choices == nil {
				b.choices = map[int]bool{}
			}
			if _, ok := b.choices[index]; !ok {
				b.choices[index] = false
			}
			if item.Get("finish_reason").String() != "" || item.Get("finishReason").String() != "" {
				b.choices[index] = true
			}
		}
	}
	if len(b.choices) > 0 {
		b.terminal = true
		for _, finished := range b.choices {
			b.terminal = b.terminal && finished
		}
	}
	if typeName == "message_delta" && recoveryValidCount(gjson.GetBytes(data, "usage.output_tokens")) ||
		(typeName == "response.completed" || typeName == "response.incomplete") && recoveryValidCount(gjson.GetBytes(data, "response.usage.output_tokens")) ||
		recoveryValidCount(gjson.GetBytes(data, "usage.completion_tokens")) ||
		b.protocol == "gemini" && (recoveryValidCount(gjson.GetBytes(data, "usageMetadata.candidatesTokenCount")) || recoveryValidCount(gjson.GetBytes(data, "response.usageMetadata.candidatesTokenCount"))) {
		b.finalUsage = true
	}
	for _, path := range []string{"id", "response.id", "responseId", "response.responseId"} {
		if value := gjson.GetBytes(data, path).String(); value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\r\n") {
			b.requestIDs["body_"+path] = value
		}
	}
}

func recoveryValidCount(value gjson.Result) bool {
	return value.Type == gjson.Number && value.Num >= 0 && value.Num <= 2147483647 && value.Num == math.Trunc(value.Num)
}

func (b *recoveryResponseBody) observeLine(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		if len(b.frame) > 0 {
			b.failed = true
			b.frame = nil
		}
		return
	}
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	if bytes.Equal(data, []byte("[DONE]")) {
		b.done = true
		return
	}
	if len(b.frame) > 0 {
		b.frame = append(b.frame, '\n')
	}
	b.frame = append(b.frame, data...)
	if len(b.frame) > 2<<20 {
		b.truncated, b.frame = true, nil
		return
	}
	if json.Valid(b.frame) {
		b.observe(b.frame)
		b.frame = nil
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
			line := b.buffer[:index]
			b.buffer = b.buffer[index+1:]
			b.observeLine(line)
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
			b.observe(b.buffer)
		} else {
			b.observeLine(b.buffer)
		}
		code := ""
		bodyComplete := b.eof || b.sse && b.done && b.terminal && b.finalUsage
		if !bodyComplete || readErr != nil && readErr != io.EOF {
			code = "UPSTREAM_BODY_INTERRUPTED"
		}
		if b.truncated {
			code = "USAGE_OBSERVATION_TRUNCATED"
		}
		if b.invalidUsage {
			code = "USAGE_OBSERVATION_INVALID"
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
	complete := b.status >= 200 && b.status < 300 && len(b.usage) > 0 && !b.truncated && !b.failed && !b.invalidUsage &&
		(b.sse && b.terminal && b.finalUsage && (b.eof || b.done) || !b.sse && b.eof && json.Valid(b.buffer))
	phase := "receiving"
	if code != "" {
		phase = "interrupted"
	} else if b.eof || b.sse && b.done && b.terminal && b.finalUsage {
		phase = "finished"
	}
	metadata, _ := json.Marshal(map[string]any{"complete": complete, "body_complete": b.eof, "terminal": b.terminal,
		"phase": phase, "protocol": b.protocol, "response_request_ids": b.requestIDs,
		"invalid_usage":       b.invalidUsage,
		"client_disconnected": b.ctx != nil && service.BillingRecoveryClientDisconnected(b.ctx),
		"body_duration_ms":    time.Since(b.started).Milliseconds(), "observed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	// Metadata is separate from numeric usage so missing values remain unknown.
	dataMap := make(map[string]json.RawMessage, len(b.usage)+1)
	for key, value := range b.usage {
		dataMap[key] = value
	}
	dataMap["_recovery"] = metadata
	data, _ := json.Marshal(dataMap)
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
func observeBillingRecoveryResponse(id string, resp *http.Response, err error, requestContext ...context.Context) {
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
	ids := map[string]string{}
	for _, name := range []string{"x-request-id", "request-id", "x-client-request-id", "x-goog-request-id", "x-amzn-requestid", "x-ms-request-id"} {
		if value := strings.TrimSpace(resp.Header.Get(name)); value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\r\n") {
			ids[name] = value
		}
	}
	metadata, _ := json.Marshal(map[string]any{"_recovery": map[string]any{"complete": false, "phase": "headers", "response_request_ids": ids}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	observeErr := service.BillingRecovery().Observe(ctx, id, resp.StatusCode, requestID, metadata, "")
	cancel()
	if observeErr != nil {
		slog.Error("billing recovery header persistence failed", "case_id", id)
	}
	if resp.Body != nil {
		var ctx context.Context
		if len(requestContext) > 0 {
			ctx = requestContext[0]
		}
		resp.Body = &recoveryResponseBody{ReadCloser: resp.Body, id: id, requestID: requestID, status: resp.StatusCode,
			sse: strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream"), usage: make(map[string]json.RawMessage),
			started: time.Now(), requestIDs: ids, ctx: ctx}
	}
}
