package handler

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Registered inside the existing admin authentication, rate-limit and audit boundary.
func RegisterBillingRecoveryRoutes(admin *gin.RouterGroup, h *GatewayHandler) {
	if h != nil {
		service.ConfigureBillingRecoveryPricing(h.gatewayService)
	}
	group := admin.Group("/billing-recovery")
	store := func(c *gin.Context) service.BillingRecoveryStore {
		s := service.BillingRecovery()
		if s == nil {
			response.Error(c, 503, "Billing recovery is disabled")
		}
		return s
	}
	actor := func(c *gin.Context) string {
		if subject, ok := middleware2.GetAuthSubjectFromContext(c); ok {
			return "admin:" + strconv.FormatInt(subject.UserID, 10)
		}
		return "admin"
	}
	respondError := func(c *gin.Context, err error) {
		if errors.Is(err, service.ErrBillingRecoveryConflict) || errors.Is(err, service.ErrUsageBillingRequestConflict) {
			response.Error(c, http.StatusConflict, err.Error())
			return
		}
		response.ErrorFrom(c, err)
	}
	group.POST("/imports/quote", func(c *gin.Context) {
		if store(c) == nil {
			return
		}
		var input struct {
			Source service.BillingRecoveryImport `json:"source" binding:"required"`
			Tokens service.UsageTokens           `json:"tokens"`
		}
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "Verified source and tokens are required")
			return
		}
		if h == nil || h.apiKeyService == nil || h.gatewayService == nil {
			response.Error(c, http.StatusServiceUnavailable, "Import pricing is unavailable")
			return
		}
		key, err := h.apiKeyService.GetByID(c.Request.Context(), input.Source.APIKeyID)
		if err != nil {
			respondError(c, err)
			return
		}
		quote, err := h.gatewayService.QuoteBillingImportUsage(c.Request.Context(), &input.Source, key, input.Tokens)
		if err != nil {
			response.BadRequest(c, err.Error())
			return
		}
		response.Success(c, quote)
	})
	group.POST("/imports/reserve", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		imports, ok := s.(service.BillingRecoveryImportStore)
		if !ok {
			response.Error(c, http.StatusServiceUnavailable, "Historical billing import is unavailable")
			return
		}
		var input service.BillingRecoveryImport
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "A verified local source, bill identity and preview are required")
			return
		}
		id, err := imports.ReserveImport(c.Request.Context(), &input, actor(c))
		if err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"case_id": id})
	})
	group.POST("/imports/reserve-group", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		imports, ok := s.(service.BillingRecoveryImportGroupStore)
		if !ok {
			response.Error(c, http.StatusServiceUnavailable, "Group import is unavailable")
			return
		}
		var input service.BillingRecoveryImportGroup
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "A complete frozen group is required")
			return
		}
		receipt, err := imports.ReserveImportGroup(c.Request.Context(), &input, actor(c))
		if err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"claims": receipt})
	})
	group.GET("/imports/claims", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		imports, ok := s.(service.BillingRecoveryImportGroupStore)
		if !ok {
			response.Error(c, http.StatusServiceUnavailable, "Import claims are unavailable")
			return
		}
		account, err := strconv.ParseInt(c.Query("account_id"), 10, 64)
		from, fromErr := time.Parse(time.RFC3339Nano, c.Query("from"))
		to, toErr := time.Parse(time.RFC3339Nano, c.Query("to"))
		if err != nil || account <= 0 || fromErr != nil || toErr != nil {
			response.BadRequest(c, "Account and evidence window are required")
			return
		}
		claims, err := imports.ImportClaims(c.Request.Context(), account, from, to)
		if err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"claims": claims})
	})
	group.GET("", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		page, size := response.ParsePagination(c)
		filter := service.BillingRecoveryListFilter{TimeoutOnly: c.Query("timeout_only") == "true", RequestID: c.Query("request_id")}
		for name, target := range map[string]**time.Time{"from": &filter.From, "to": &filter.To} {
			if value := c.Query(name); value != "" {
				at, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					response.BadRequest(c, "Invalid "+name)
					return
				}
				*target = &at
			}
		}
		for name, target := range map[string]*int64{"user_id": &filter.UserID, "api_key_id": &filter.APIKeyID, "account_id": &filter.AccountID} {
			if value := c.Query(name); value != "" {
				id, err := strconv.ParseInt(value, 10, 64)
				if err != nil || id <= 0 {
					response.BadRequest(c, "Invalid "+name)
					return
				}
				*target = id
			}
		}
		if len(filter.RequestID) > 128 {
			response.BadRequest(c, "Request ID is too long")
			return
		}
		items, total, err := s.List(service.WithBillingRecoveryFilter(c.Request.Context(), filter), c.Query("state"), size, (page-1)*size)
		if err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"items": items, "total": total, "page": page, "page_size": size})
	})
	group.GET("/:id", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		entry, err := s.Get(c.Request.Context(), c.Param("id"))
		if err != nil {
			respondError(c, err)
			return
		}
		events, err := s.Events(c.Request.Context(), entry.ID)
		if err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"case": entry, "events": events})
	})
	group.POST("/:id/review", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		var input struct {
			Version  int    `json:"version" binding:"required,min=1"`
			State    string `json:"state" binding:"required,oneof=review ignored prepared"`
			Evidence string `json:"evidence" binding:"required,min=8,max=4000"`
		}
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "Version, state and evidence are required")
			return
		}
		if err := s.Review(c.Request.Context(), c.Param("id"), input.Version, input.State, input.Evidence, actor(c)); err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"ok": true})
	})
	group.POST("/:id/usage", func(c *gin.Context) {
		if store(c) == nil {
			return
		}
		var input struct {
			Version  int              `json:"version" binding:"required,min=1"`
			Usage    service.UsageLog `json:"usage_record"`
			Evidence string           `json:"evidence" binding:"required,min=8,max=4000"`
		}
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "A usage record and historical pricing evidence are required")
			return
		}
		entry, err := service.BillingRecovery().Get(c.Request.Context(), c.Param("id"))
		if err != nil {
			respondError(c, err)
			return
		}
		key, err := h.apiKeyService.GetByID(c.Request.Context(), entry.APIKeyID)
		if err != nil {
			respondError(c, err)
			return
		}
		restoredID, err := h.gatewayService.RestoreBillingRecoveryUsage(c.Request.Context(), entry.ID, input.Version, key, &input.Usage, input.Evidence, actor(c))
		if err != nil {
			response.BadRequest(c, err.Error())
			return
		}
		response.Success(c, gin.H{"ok": true, "case_id": restoredID})
	})
	group.POST("/:id/quote", func(c *gin.Context) {
		if store(c) == nil {
			return
		}
		var input struct {
			Version int                 `json:"version" binding:"required,min=1"`
			Tokens  service.UsageTokens `json:"tokens"`
		}
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "Version and verified tokens are required")
			return
		}
		u, err := h.gatewayService.QuoteBillingRecoveryUsage(c.Request.Context(), c.Param("id"), input.Version, input.Tokens)
		if err != nil {
			response.BadRequest(c, err.Error())
			return
		}
		response.Success(c, gin.H{"usage_record": u})
	})
	group.POST("/:id/settle", func(c *gin.Context) {
		if store(c) == nil {
			return
		}
		var input struct {
			Version int `json:"version" binding:"required,min=1"`
		}
		if err := c.ShouldBindJSON(&input); err != nil {
			response.BadRequest(c, "Version is required")
			return
		}
		if err := h.gatewayService.SettleBillingRecovery(c.Request.Context(), c.Param("id"), input.Version); err != nil {
			respondError(c, err)
			return
		}
		response.Success(c, gin.H{"ok": true})
	})
}
