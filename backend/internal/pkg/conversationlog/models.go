package conversationlog

import (
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
	models := requestmodel.FromBodyCandidates("", contentType, body)
	if websocket {
		gjson.ParseBytes(body).ForEach(func(key, value gjson.Result) bool {
			if strings.EqualFold(key.String(), "response") && value.IsObject() {
				models = append(models, requestmodel.FromBodyCandidates("", "application/json", []byte(value.Raw))...)
			}
			return true
		})
	}
	return models
}

func (a *attempt) identifyLocked() {
	models := bodyModels(a.row.Request, a.contentType, a.row.Protocol == "WebSocket")
	if len(models) > 0 {
		a.models = append(a.models, models...)
	}
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
