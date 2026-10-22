package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Registered inside the existing admin authentication, rate-limit and audit boundary.
func RegisterBillingRecoveryRoutes(admin *gin.RouterGroup, h *GatewayHandler) {
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
	group.GET("", func(c *gin.Context) {
		s := store(c)
		if s == nil {
			return
		}
		page, size := response.ParsePagination(c)
		items, total, err := s.List(c.Request.Context(), c.Query("state"), size, (page-1)*size)
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
