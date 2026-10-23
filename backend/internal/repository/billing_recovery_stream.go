package repository

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Keep downstream synchronous APIs unchanged while receiving upstream heartbeats
// and final usage. Only documented JSON/SSE protocols are rewritten.
func prepareRecoveryStream(req *http.Request) (*http.Request, string, error) {
	if req == nil || req.URL == nil || service.BillingRecovery() == nil || !service.HasBillingRecoveryRequest(req.Context()) ||
		req.Method != http.MethodPost || req.GetBody == nil || strings.EqualFold(os.Getenv("BILLING_RECOVERY_SYNC_STREAM"), "false") {
		return req, "", nil
	}
	path := strings.TrimRight(req.URL.Path, "/")
	for name, values := range req.Header {
		if strings.EqualFold(name, "authorization") && len(values) > 0 && strings.HasPrefix(values[0], "AWS4-") {
			return req, "", nil
		}
	}
	protocol := ""
	switch {
	case strings.HasSuffix(path, "/messages"):
		protocol = "anthropic"
	case strings.HasSuffix(path, "/chat/completions"):
		protocol = "chat"
	case strings.HasSuffix(path, "/responses"):
		protocol = "responses"
	case strings.HasSuffix(path, ":generateContent"):
		protocol = "gemini"
	default:
		return req, "", nil
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, "", err
	}
	data, err := io.ReadAll(io.LimitReader(body, (4<<20)+1))
	_ = body.Close()
	if err != nil {
		return nil, "", err
	}
	// Large requests and non-JSON protocols retain their native processing path.
	var fields map[string]json.RawMessage
	if len(data) > 4<<20 || json.Unmarshal(data, &fields) != nil || fields == nil {
		return req, "", nil
	}
	if protocol != "gemini" && string(bytes.TrimSpace(fields["stream"])) == "true" {
		return req, "", nil
	}
	if protocol == "responses" && string(bytes.TrimSpace(fields["background"])) == "true" {
		return req, "", nil
	}
	clone := req.Clone(req.Context())
	if protocol == "gemini" {
		clone.URL.Path = strings.TrimSuffix(path, ":generateContent") + ":streamGenerateContent"
		query := clone.URL.Query()
		query.Set("alt", "sse")
		clone.URL.RawQuery = query.Encode()
	} else {
		fields["stream"] = json.RawMessage("true")
		if protocol == "chat" {
			options := map[string]json.RawMessage{}
			if value := fields["stream_options"]; len(value) > 0 && string(value) != "null" {
				if err := json.Unmarshal(value, &options); err != nil {
					return nil, "", fmt.Errorf("invalid stream_options: %w", err)
				}
			}
			options["include_usage"] = json.RawMessage("true")
			fields["stream_options"], _ = json.Marshal(options)
		}
	}
	data, err = json.Marshal(fields)
	if err != nil {
		return nil, "", err
	}
	clone.Body = io.NopCloser(bytes.NewReader(data))
	clone.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }
	clone.ContentLength = int64(len(data))
	for name := range clone.Header {
		if strings.EqualFold(name, "content-length") || strings.EqualFold(name, "accept") {
			delete(clone.Header, name)
		}
	}
	clone.Header.Set("Accept", "text/event-stream")
	clone = clone.WithContext(service.WithBillingRecoveryStream(req.Context(), protocol))
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return clone, protocol, nil
}

func aggregateRecoveryStream(resp *http.Response, protocol string) {
	if protocol == "" || resp == nil || resp.Body == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 ||
		!strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		return
	}
	resp.Body = &recoveryAggregateBody{source: resp.Body, protocol: protocol}
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Del("Content-Length")
	resp.Header.Del("Content-Encoding")
	resp.ContentLength = -1
}

type recoveryAggregateBody struct {
	source   io.ReadCloser
	protocol string
	once     sync.Once
	reader   *bytes.Reader
	err      error
}

func (b *recoveryAggregateBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		data, err := collectRecoveryStream(b.source, b.protocol)
		b.err = err
		b.reader = bytes.NewReader(data)
	})
	if b.err != nil {
		return 0, b.err
	}
	return b.reader.Read(p)
}

func (b *recoveryAggregateBody) Close() error { return b.source.Close() }

func collectRecoveryStream(source io.Reader, protocol string) ([]byte, error) {
	a := recoveryStreamAccumulator{protocol: protocol, blocks: map[int]map[string]any{}, partial: map[int]*recoveryText{}, choices: map[int]map[string]any{},
		responses: apicompat.NewBufferedResponseAccumulator()}
	reader := bufio.NewReaderSize(io.LimitReader(source, (64<<20)+1), 32<<10)
	var frame bytes.Buffer
	total := 0
	dispatch := func() error {
		if frame.Len() == 0 {
			return nil
		}
		data := bytes.TrimSpace(frame.Bytes())
		if bytes.Equal(data, []byte("[DONE]")) {
			if protocol == "chat" {
				a.done = true
			}
			frame.Reset()
			return nil
		}
		var event map[string]any
		if json.Unmarshal(data, &event) != nil {
			return errors.New("invalid upstream SSE JSON; usage retained for review")
		}
		frame.Reset()
		return a.add(event)
	}
	for {
		line, err := reader.ReadBytes('\n')
		total += len(line)
		if total > 64<<20 {
			return nil, errors.New("upstream SSE response exceeds aggregation limit; usage retained for review")
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if e := dispatch(); e != nil {
				return nil, e
			}
			if a.done && a.finalUsage {
				return a.result()
			}
		} else if bytes.HasPrefix(line, []byte("data:")) {
			if frame.Len() > 0 {
				_ = frame.WriteByte('\n')
			}
			_, _ = frame.Write(bytes.TrimPrefix(bytes.TrimPrefix(line, []byte("data:")), []byte(" ")))
		}
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			if e := dispatch(); e != nil {
				return nil, e
			}
			break
		}
	}
	return a.result()
}

type recoveryStreamAccumulator struct {
	protocol   string
	response   map[string]any
	blocks     map[int]map[string]any
	partial    map[int]*recoveryText
	choices    map[int]map[string]any
	done       bool
	finalUsage bool
	responses  *apicompat.BufferedResponseAccumulator
}

type recoveryText struct{ strings.Builder }

func (t *recoveryText) MarshalJSON() ([]byte, error) { return json.Marshal(t.String()) }

func recoveryObject(value any) map[string]any { result, _ := value.(map[string]any); return result }
func recoveryString(value any) string {
	if text, ok := value.(*recoveryText); ok {
		return text.String()
	}
	result, _ := value.(string)
	return result
}

// Append token fragments without repeatedly copying an entire long response.
func appendRecoveryText(target map[string]any, key, fragment string) {
	text, ok := target[key].(*recoveryText)
	if !ok {
		text = &recoveryText{}
		_, _ = text.WriteString(recoveryString(target[key]))
		target[key] = text
	}
	_, _ = text.WriteString(fragment)
}
func recoveryIndex(value any) int {
	result, ok := value.(float64)
	if !ok || result < 0 || result > 1024 || result != float64(int(result)) {
		return -1
	}
	return int(result)
}

func (a *recoveryStreamAccumulator) add(v map[string]any) error {
	if v["error"] != nil || v["type"] == "error" || v["type"] == "response.failed" {
		return errors.New("upstream SSE reported an error; usage retained for review")
	}
	switch a.protocol {
	case "anthropic":
		index := recoveryIndex(v["index"])
		switch v["type"] {
		case "message_start":
			a.response = recoveryObject(v["message"])
		case "content_block_start":
			if index < 0 {
				return errors.New("invalid upstream content block index")
			}
			a.blocks[index] = recoveryObject(v["content_block"])
		case "content_block_delta":
			delta, block := recoveryObject(v["delta"]), a.blocks[index]
			if block == nil {
				return errors.New("upstream SSE delta has no content block")
			}
			switch delta["type"] {
			case "input_json_delta":
				if a.partial[index] == nil {
					a.partial[index] = &recoveryText{}
				}
				_, _ = a.partial[index].WriteString(recoveryString(delta["partial_json"]))
			case "citations_delta":
				items, _ := block["citations"].([]any)
				block["citations"] = append(items, delta["citation"])
			default:
				for key, value := range delta {
					if key == "type" {
						continue
					}
					if text, ok := value.(string); ok {
						appendRecoveryText(block, key, text)
					} else {
						block[key] = value
					}
				}
			}
		case "message_delta":
			if a.response == nil {
				return errors.New("upstream SSE has no message_start")
			}
			for k, value := range recoveryObject(v["delta"]) {
				a.response[k] = value
			}
			usage := recoveryObject(a.response["usage"])
			if usage == nil {
				usage = map[string]any{}
			}
			for k, value := range recoveryObject(v["usage"]) {
				usage[k] = value
			}
			if recoveryObject(v["usage"])["output_tokens"] != nil {
				a.finalUsage = true
			}
			a.response["usage"] = usage
		case "message_stop":
			a.done = true
		}
	case "responses":
		data, _ := json.Marshal(v)
		var event apicompat.ResponsesStreamEvent
		if json.Unmarshal(data, &event) == nil {
			a.responses.ProcessEvent(&event)
		}
		if v["type"] == "response.completed" || v["type"] == "response.incomplete" {
			a.response = recoveryObject(v["response"])
			a.done = a.response != nil
			if a.response != nil {
				usage := recoveryObject(a.response["usage"])
				a.finalUsage = usage["input_tokens"] != nil && usage["output_tokens"] != nil
				data, _ := json.Marshal(a.response)
				var terminal apicompat.ResponsesResponse
				if json.Unmarshal(data, &terminal) == nil {
					a.responses.SupplementResponseOutput(&terminal)
					if len(terminal.Output) > 0 {
						data, _ = json.Marshal(terminal.Output)
						var supplemented any
						if json.Unmarshal(data, &supplemented) == nil {
							a.response["output"] = mergeRecoverySupplement(a.response["output"], supplemented)
						}
					}
				}
			}
		}
	case "chat":
		if a.response == nil {
			a.response = map[string]any{"object": "chat.completion"}
		}
		for k, value := range v {
			if k != "object" && k != "choices" && value != nil {
				a.response[k] = value
			}
		}
		if usage := recoveryObject(v["usage"]); usage["prompt_tokens"] != nil && usage["completion_tokens"] != nil {
			a.finalUsage = true
		}
		choices, _ := v["choices"].([]any)
		for _, item := range choices {
			choice := recoveryObject(item)
			index := recoveryIndex(choice["index"])
			if index < 0 {
				return errors.New("invalid upstream choice index")
			}
			target := a.choices[index]
			if target == nil {
				target = map[string]any{"index": index, "message": map[string]any{"role": "assistant", "content": nil}, "finish_reason": nil}
				a.choices[index] = target
			}
			message := recoveryObject(target["message"])
			for key, value := range recoveryObject(choice["delta"]) {
				if key == "tool_calls" {
					if err := mergeRecoveryToolCalls(message, value); err != nil {
						return err
					}
				} else if key == "function_call" || key == "audio" {
					mergeRecoveryFragments(message, key, value)
				} else if key == "role" {
					message[key] = value
				} else if text, ok := value.(string); ok {
					appendRecoveryText(message, key, text)
				} else if items, ok := value.([]any); ok {
					previous, _ := message[key].([]any)
					message[key] = append(previous, items...)
				} else if value != nil {
					message[key] = value
				}
			}
			if choice["finish_reason"] != nil {
				target["finish_reason"] = choice["finish_reason"]
			}
			if incoming := recoveryObject(choice["logprobs"]); incoming != nil {
				logs := recoveryObject(target["logprobs"])
				if logs == nil {
					logs = map[string]any{}
				}
				for key, value := range incoming {
					previous, _ := logs[key].([]any)
					items, _ := value.([]any)
					logs[key] = append(previous, items...)
				}
				target["logprobs"] = logs
			}
		}
	case "gemini":
		if a.response == nil {
			a.response = map[string]any{}
		}
		for key, value := range v {
			if key != "candidates" {
				a.response[key] = value
			}
		}
		if usage := recoveryObject(v["usageMetadata"]); usage["promptTokenCount"] != nil && usage["candidatesTokenCount"] != nil {
			a.finalUsage = true
		}
		candidates, _ := v["candidates"].([]any)
		for position, value := range candidates {
			candidate := recoveryObject(value)
			index := recoveryIndex(candidate["index"])
			if candidate["index"] == nil {
				index = position
			}
			if index < 0 {
				return errors.New("invalid upstream candidate index")
			}
			target := a.choices[index]
			if target == nil {
				target = map[string]any{"index": index, "content": map[string]any{"role": "model", "parts": []any{}}}
				a.choices[index] = target
			}
			content := recoveryObject(target["content"])
			parts, _ := content["parts"].([]any)
			incoming, _ := recoveryObject(candidate["content"])["parts"].([]any)
			content["parts"] = append(parts, incoming...)
			for key, item := range candidate {
				if key != "content" {
					target[key] = item
				}
			}
		}
	}
	return nil
}

// Fill missing streamed output without discarding unknown terminal extensions.
func mergeRecoverySupplement(original, supplement any) any {
	if original == nil {
		return supplement
	}
	switch value := original.(type) {
	case map[string]any:
		for key, next := range recoveryObject(supplement) {
			value[key] = mergeRecoverySupplement(value[key], next)
		}
	case []any:
		items, _ := supplement.([]any)
		for index, next := range items {
			if index < len(value) {
				value[index] = mergeRecoverySupplement(value[index], next)
			} else {
				value = append(value, next)
			}
		}
		return value
	case string:
		if strings.TrimSpace(value) == "" {
			return supplement
		}
	}
	return original
}

func mergeRecoveryFragments(target map[string]any, key string, value any) {
	fields := recoveryObject(target[key])
	if fields == nil {
		fields = map[string]any{}
	}
	for name, fragment := range recoveryObject(value) {
		if text, ok := fragment.(string); ok {
			if name == "id" {
				fields[name] = text
			} else {
				appendRecoveryText(fields, name, text)
			}
		} else {
			fields[name] = fragment
		}
	}
	target[key] = fields
}

func mergeRecoveryToolCalls(message map[string]any, value any) error {
	existing, _ := message["tool_calls"].([]any)
	items, _ := value.([]any)
	for _, item := range items {
		delta := recoveryObject(item)
		index := recoveryIndex(delta["index"])
		if index < 0 || index > 1024 {
			return errors.New("invalid upstream tool index")
		}
		for len(existing) <= index {
			existing = append(existing, map[string]any{"function": map[string]any{}})
		}
		target := recoveryObject(existing[index])
		for key, field := range delta {
			if key == "index" {
				continue
			}
			if key == "function" {
				function := recoveryObject(target[key])
				for name, fragment := range recoveryObject(field) {
					appendRecoveryText(function, name, recoveryString(fragment))
				}
			} else {
				target[key] = field
			}
		}
	}
	message["tool_calls"] = existing
	return nil
}

func (a *recoveryStreamAccumulator) result() ([]byte, error) {
	switch a.protocol {
	case "anthropic":
		blocks := make([]any, 0, len(a.blocks))
		for _, index := range recoverySortedKeys(a.blocks) {
			block := a.blocks[index]
			if partial, ok := a.partial[index]; ok {
				var input any
				if json.Unmarshal([]byte(partial.String()), &input) != nil {
					return nil, errors.New("upstream SSE tool input is incomplete")
				}
				block["input"] = input
			}
			blocks = append(blocks, block)
		}
		if a.response != nil {
			a.response["content"] = blocks
		}
	case "chat", "gemini":
		items := make([]any, 0, len(a.choices))
		terminal := len(a.choices) > 0
		for _, index := range recoverySortedKeys(a.choices) {
			item := a.choices[index]
			field := "finish_reason"
			if a.protocol == "gemini" {
				field = "finishReason"
			}
			terminal = terminal && recoveryString(item[field]) != ""
			items = append(items, item)
		}
		if a.response != nil {
			if a.protocol == "chat" {
				a.response["choices"] = items
			} else {
				a.response["candidates"] = items
			}
		}
		a.done = terminal
	}
	if !a.done || a.response == nil || !a.finalUsage {
		return nil, errors.New("upstream SSE ended without a terminal response; usage retained for review")
	}
	return json.Marshal(a.response)
}

func recoverySortedKeys(values map[int]map[string]any) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}
