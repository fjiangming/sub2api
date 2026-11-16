package conversationlog

import (
	"bytes"
	"mime"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requestmodel"
	"github.com/tidwall/gjson"
)

var claudeModel = regexp.MustCompile(`^(?:claude-?)?(fable|opus)-?(\d+)(?:[.-](\d+))?$`)
var kimiModel = regexp.MustCompile(`^kimi-?k(\d{1,4})(?:(?:[.-]\d+)*)(?:-[a-z][a-z0-9.-]*)?$`)

func normalizeModel(model string) string {
	model = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return unicode.ToLower(r)
	}, model)
	if parts := claudeModel.FindStringSubmatch(model); parts != nil {
		model = "claude-" + parts[1] + "-" + parts[2]
		if parts[3] != "" {
			model += "-" + parts[3]
		}
	}
	if model == "geminicarbon" {
		return "gemini-carbon"
	}
	if strings.HasPrefix(model, "kimik") && len(model) > 5 && model[5] >= '0' && model[5] <= '9' {
		return "kimi-k" + model[5:]
	}
	return model
}

func (p policy) allowsModel(model string) bool {
	if !p.ModelFilterEnabled {
		return true
	}
	model = normalizeModel(model)
	if model == "" {
		return false
	}
	for _, allowed := range p.AllowedModels {
		if normalizeModel(allowed) == model {
			return true
		}
	}
	if p.KimiK3Plus {
		if match := kimiModel.FindStringSubmatch(model); match != nil {
			major, err := strconv.Atoi(match[1])
			return err == nil && major >= 3
		}
	}
	return false
}

func bodyModels(body []byte, contentType string, websocket bool) []string {
	media, _, _ := mime.ParseMediaType(contentType)
	if strings.EqualFold(media, "multipart/form-data") {
		return requestmodel.FromBodyCandidates("", contentType, body)
	}
	parsed := gjson.ParseBytes(body)
	models := jsonModels(parsed)
	if websocket {
		parsed.ForEach(func(key, value gjson.Result) bool {
			if strings.EqualFold(key.String(), "response") && value.IsObject() {
				models = append(models, jsonModels(value)...)
			}
			return true
		})
	}
	return models
}
func jsonModels(value gjson.Result) []string {
	var top, session []string
	value.ForEach(func(key, item gjson.Result) bool {
		if strings.EqualFold(key.String(), "model") && item.Type == gjson.String {
			if model := strings.TrimSpace(item.String()); model != "" {
				top = append(top, strings.Clone(model))
			}
		}
		if strings.EqualFold(key.String(), "session") {
			item.ForEach(func(k, v gjson.Result) bool {
				if strings.EqualFold(k.String(), "model") && v.Type == gjson.String {
					if model := strings.TrimSpace(v.String()); model != "" {
						session = append(session, strings.Clone(model))
					}
				}
				return true
			})
		}
		return true
	})
	if len(top) > 0 {
		return top
	}
	return session
}

func (a *attempt) prepareDelivery() {
	if a.row.Protocol == "WebSocket" {
		a.setIdentifiedModel()
		return
	}
	var body []byte
	if len(a.requestChunks) == 1 {
		body = a.requestChunks[0]
	} else {
		body = bytes.Join(a.requestChunks, nil)
	}
	a.models = append(a.models, bodyModels(body, a.contentType, false)...)
	a.setIdentifiedModel()
}

func (a *attempt) identifyLocked() {
	models := bodyModels(a.row.Request, a.contentType, a.row.Protocol == "WebSocket")
	if len(models) > 0 {
		a.models = append(a.models, models...)
	}
	a.setIdentifiedModel()
}
func (a *attempt) setIdentifiedModel() {
	if len(a.models) > 0 && modelName.MatchString(a.models[0]) {
		a.row.Model = a.models[0]
	}
}

func (a *attempt) modelAllowedLocked(p policy) bool {
	if !p.allowsModel(a.row.Model) {
		return false
	}
	for _, model := range a.models {
		if !p.allowsModel(model) {
			return false
		}
	}
	return true
}
