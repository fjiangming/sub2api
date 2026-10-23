package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
)

// Pricing is frozen before dispatch. Recovery never substitutes today's price
// table or the provider's purchase cost for the user's historical selling price.
var recoveryPricingGateway atomic.Pointer[GatewayService]

func ConfigureBillingRecoveryPricing(gateway *GatewayService) { recoveryPricingGateway.Store(gateway) }

type recoveryIngress struct {
	Model       string `json:"model"`
	Stream      bool   `json:"stream"`
	ServiceTier string `json:"service_tier"`
	Effort      string `json:"reasoning_effort"`
	Reasoning   struct {
		Effort string `json:"effort"`
	} `json:"reasoning"`
	Tools []json.RawMessage `json:"tools"`
}

type recoveryIngressBody struct {
	io.ReadCloser
	state     *billingRequestState
	buffer    []byte
	truncated bool
}

func CaptureBillingRecoveryIngress(req *http.Request) *http.Request {
	if req == nil {
		return req
	}
	state, _ := req.Context().Value(billingRequestKey{}).(*billingRequestState)
	if state != nil && req.Body != nil {
		req.Body = &recoveryIngressBody{ReadCloser: req.Body, state: state}
	}
	return req
}

func (b *recoveryIngressBody) Close() error {
	b.buffer = nil
	return b.ReadCloser.Close()
}

func (b *recoveryIngressBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if !b.truncated {
		if len(b.buffer)+n <= 4<<20 {
			b.buffer = append(b.buffer, p[:n]...)
		} else {
			b.buffer, b.truncated = nil, true
		}
	}
	if err == io.EOF {
		var fields recoveryIngress
		if !b.truncated && json.Unmarshal(b.buffer, &fields) == nil {
			// Retain only billing metadata, never tool schemas or message content.
			if len(fields.Tools) > 0 {
				fields.Tools = []json.RawMessage{json.RawMessage(`true`)}
			}
			b.state.Ingress.Store(&fields)
		}
		b.buffer = nil
	}
	return n, err
}

type recoveryPricePlan struct {
	Version           int               `json:"version"`
	Model             string            `json:"model"`
	RequestedModel    string            `json:"requested_model"`
	RateMultiplier    float64           `json:"rate_multiplier"`
	BasePricing       *ModelPricing     `json:"base_pricing"`
	Intervals         []PricingInterval `json:"intervals,omitempty"`
	IntervalPrices    []*ModelPricing   `json:"interval_prices,omitempty"`
	TimeMultiplier    float64           `json:"time_multiplier"`
	PricingAt         time.Time         `json:"pricing_at"`
	LongContext       bool              `json:"long_context"`
	ServiceTier       string            `json:"service_tier"`
	ReasoningEffort   string            `json:"reasoning_effort"`
	Eligible          bool              `json:"eligible"`
	ForceCacheBilling bool              `json:"force_cache_billing"`
	CacheTTLDefault5m bool              `json:"cache_ttl_default_5m"`
}

func snapshotRecoveryPrice(ctx context.Context, state *billingRequestState, model string, req *http.Request) *recoveryPricePlan {
	s := recoveryPricingGateway.Load()
	ingress := state.Ingress.Load()
	if s == nil || s.billingService == nil || state.Key == nil || ingress == nil || strings.TrimSpace(model) == "" {
		return nil
	}
	requested := ingress.Model
	if requested == "" {
		requested = model
	}
	fields := ChannelUsageFields{}
	if s.channelService != nil {
		mapping, _ := s.channelService.ResolveChannelMappingAndRestrict(ctx, state.GroupID, requested)
		fields = mapping.ToUsageFields(requested, model)
	}
	result := &ForwardResult{Model: requested, UpstreamModel: model}
	billingModel := s.recordUsageBillingModel(ctx, state.Key, result, fields)
	multiplier, _, pricingAt := s.recordUsageMultipliers(ctx, state.Key, state.Key.User, state.StartedAt)
	resolver := s.resolver
	if resolver == nil {
		resolver = NewModelPricingResolver(s.channelService, s.billingService)
	}
	resolved := resolver.Resolve(ctx, PricingInput{Model: billingModel, GroupID: state.GroupID, Group: state.Key.Group})
	plan := &recoveryPricePlan{Version: 1, Model: billingModel, RequestedModel: requested,
		RateMultiplier: multiplier, Intervals: resolved.Intervals, PricingAt: pricingAt,
		TimeMultiplier: resolvedChannelTimeMultiplier(resolved, pricingAt),
		LongContext:    resolved.longContextPricingEnabled, ServiceTier: ingress.ServiceTier, ReasoningEffort: ingress.Effort}
	plan.ForceCacheBilling = IsForceCacheBilling(req.Context())
	plan.CacheTTLDefault5m = s.settingService != nil && s.settingService.IsAnthropicCacheTTL1hInjectionEnabled(ctx)
	freeze := func(price *ModelPricing) *ModelPricing {
		if price == nil {
			return nil
		}
		price = s.billingService.applyModelSpecificPricingPolicyEx(billingModel, price, resolved.Source == PricingSourceLiteLLM, pricingAt)
		copy := *price
		if resolved.Source == PricingSourceLiteLLM && isDeepSeekModel(billingModel) {
			mult := deepseekPeakMultiplierAt(pricingAt)
			copy.InputPricePerToken *= mult
			copy.OutputPricePerToken *= mult
			copy.CacheReadPricePerToken *= mult
		}
		return &copy
	}
	plan.BasePricing = freeze(resolved.BasePricing)
	for _, interval := range resolved.Intervals {
		price := intervalToModelPricing(&interval, resolved.BasePricing, resolved.channelPricing)
		price.SupportsCacheBreakdown = resolved.SupportsCacheBreakdown
		plan.IntervalPrices = append(plan.IntervalPrices, freeze(price))
	}
	if ingress.Reasoning.Effort != "" {
		plan.ReasoningEffort = ingress.Reasoning.Effort
	}
	if req.GetBody != nil {
		if body, err := req.GetBody(); err == nil {
			data, _ := io.ReadAll(io.LimitReader(body, 4<<20))
			_ = body.Close()
			if tier := gjson.GetBytes(data, "service_tier").String(); tier != "" {
				plan.ServiceTier = tier
			}
			if effort := gjson.GetBytes(data, "reasoning.effort").String(); effort != "" {
				plan.ReasoningEffort = effort
			}
			// Built-in tools, media and provider-specific tiers need their native
			// billing dimensions; a token-only recovery quote must not guess them.
			for _, path := range []string{"tools", "functions", "modalities", "audio", "generationConfig.responseModalities", "generationConfig.imageConfig"} {
				value := gjson.GetBytes(data, path)
				if value.Exists() && value.Raw != "null" && value.Raw != "[]" {
					ingress = nil
				}
			}
			if gjson.GetBytes(data, "speed").String() == "fast" || gjson.GetBytes(data, "output_config.effort").Exists() {
				ingress = nil
			}
		}
	}
	path := ""
	if req.URL != nil {
		path = strings.ToLower(req.URL.Path)
	}
	tokenEndpoint := strings.HasSuffix(path, "/messages") || strings.HasSuffix(path, "/responses") ||
		strings.HasSuffix(path, "/chat/completions") || strings.HasSuffix(path, ":generatecontent") || strings.HasSuffix(path, ":streamgeneratecontent")
	plan.Eligible = tokenEndpoint && resolved.Mode == BillingModeToken && resolved.BasePricing != nil &&
		ingress != nil && len(ingress.Tools) == 0 && fields.BillingModelSource != BillingModelSourceResponse &&
		plan.ServiceTier == "" && plan.ReasoningEffort == "" &&
		(s.cfg == nil || s.cfg.RunMode != config.RunModeSimple)
	return plan
}

// QuoteBillingRecoveryUsage is read-only; review and settlement remain separate,
// versioned operations in operations-center.
func (s *GatewayService) QuoteBillingRecoveryUsage(ctx context.Context, id string, version int, tokens UsageTokens) (*UsageLog, error) {
	store := BillingRecovery()
	if store == nil {
		return nil, fmt.Errorf("billing recovery is disabled")
	}
	entry, err := store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if entry.Version != version {
		return nil, ErrBillingRecoveryConflict
	}
	if s.billingService == nil || tokens.ImageInputTokens != 0 || tokens.ImageOutputTokens != 0 || tokens.ImageCacheReadTokens != 0 {
		return nil, fmt.Errorf("historical token pricing does not cover this request")
	}
	if err := validateRecoveryUsage(&UsageLog{InputTokens: tokens.InputTokens, OutputTokens: tokens.OutputTokens,
		CacheCreationTokens: tokens.CacheCreationTokens, CacheReadTokens: tokens.CacheReadTokens,
		CacheCreation5mTokens: tokens.CacheCreation5mTokens, CacheCreation1hTokens: tokens.CacheCreation1hTokens}); err != nil {
		return nil, err
	}
	var snapshot struct {
		Pricing          *recoveryPricePlan `json:"pricing"`
		AccountRate      *float64           `json:"account_rate_multiplier"`
		AccountType      string             `json:"account_type"`
		CacheTTLOverride bool               `json:"account_cache_ttl_override"`
		CacheTTLTarget   string             `json:"account_cache_ttl_target"`
		Stream           bool               `json:"downstream_stream"`
	}
	if json.Unmarshal(entry.Snapshot, &snapshot) != nil || snapshot.Pricing == nil || snapshot.Pricing.Version != 1 ||
		!snapshot.Pricing.Eligible || snapshot.AccountRate == nil {
		return nil, fmt.Errorf("verified historical token pricing is unavailable; operator review required")
	}
	plan := snapshot.Pricing
	if math.IsNaN(*snapshot.AccountRate) || math.IsInf(*snapshot.AccountRate, 0) || *snapshot.AccountRate < 0 || *snapshot.AccountRate > 1e8 {
		return nil, fmt.Errorf("historical account multiplier is invalid")
	}
	if plan.ForceCacheBilling {
		tokens.CacheReadTokens += tokens.InputTokens
		tokens.InputTokens = 0
	}
	cacheTTLOverridden := false
	if entry.Platform == PlatformAnthropic && (snapshot.AccountType == AccountTypeOAuth || snapshot.AccountType == AccountTypeSetupToken) &&
		(snapshot.CacheTTLOverride || plan.CacheTTLDefault5m) && tokens.CacheCreationTokens > 0 {
		tokens.CacheCreation5mTokens, tokens.CacheCreation1hTokens = tokens.CacheCreationTokens, 0
		if snapshot.CacheTTLOverride && snapshot.CacheTTLTarget == "1h" {
			tokens.CacheCreation5mTokens, tokens.CacheCreation1hTokens = 0, tokens.CacheCreationTokens
		}
		cacheTTLOverridden = true
	}
	if tokens.CacheCreation5mTokens+tokens.CacheCreation1hTokens > tokens.CacheCreationTokens {
		return nil, fmt.Errorf("cache TTL counts exceed creation tokens")
	}
	price := plan.BasePricing
	contextTokens := tokens.InputTokens + tokens.CacheCreationTokens + tokens.CacheReadTokens
	if !plan.LongContext {
		contextTokens = 1
	}
	if interval := FindMatchingInterval(plan.Intervals, contextTokens); interval != nil {
		for index := range plan.Intervals {
			if interval == &plan.Intervals[index] && index < len(plan.IntervalPrices) {
				price = plan.IntervalPrices[index]
			}
		}
	}
	if price == nil || plan.TimeMultiplier <= 0 {
		return nil, fmt.Errorf("historical price card is incomplete")
	}
	cost := s.billingService.computeTokenBreakdown(price, tokens, plan.RateMultiplier, plan.ServiceTier, plan.LongContext && len(plan.Intervals) == 0)
	applyCostBreakdownMultiplier(cost, plan.TimeMultiplier)
	applyCostBreakdownMultiplier(cost, reasoningEffortBillingMultiplier(plan.ReasoningEffort, price.ReasoningEffortMultipliers))
	u := &UsageLog{UserID: entry.UserID, APIKeyID: entry.APIKeyID, AccountID: entry.AccountID,
		GroupID: entry.GroupID, SubscriptionID: entry.SubscriptionID, Model: plan.Model, RequestedModel: plan.RequestedModel,
		InputTokens: tokens.InputTokens, OutputTokens: tokens.OutputTokens, CacheCreationTokens: tokens.CacheCreationTokens,
		CacheReadTokens: tokens.CacheReadTokens, CacheCreation5mTokens: tokens.CacheCreation5mTokens, CacheCreation1hTokens: tokens.CacheCreation1hTokens,
		InputCost: cost.InputCost, OutputCost: cost.OutputCost, CacheCreationCost: cost.CacheCreationCost, CacheReadCost: cost.CacheReadCost,
		TotalCost: cost.TotalCost, ActualCost: cost.ActualCost, RateMultiplier: plan.RateMultiplier, AccountRateMultiplier: snapshot.AccountRate,
		CreatedAt: entry.CreatedAt}
	u.Stream, u.RequestType = snapshot.Stream, RequestTypeFromLegacy(snapshot.Stream, false)
	u.CacheTTLOverridden, u.LongContextBillingApplied = cacheTTLOverridden, cost.LongContextBillingApplied
	if entry.Model != "" {
		model := entry.Model
		u.UpstreamModel = &model
	}
	if entry.UpstreamRequestID != "" {
		requestID := entry.UpstreamRequestID
		u.UpstreamRequestID = &requestID
	}
	if entry.SubscriptionID != nil {
		u.BillingType = BillingTypeSubscription
	}
	if err := validateRecoveryUsage(u); err != nil {
		return nil, err
	}
	if u.TotalCost <= 0 || u.RateMultiplier <= 0 {
		return nil, fmt.Errorf("zero-cost recovery requires operator review")
	}
	return u, nil
}
