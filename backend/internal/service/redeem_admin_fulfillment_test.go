//go:build unit

package service

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestAdminFulfillmentBypassesLimitAndKeepsRedeemAffiliate(t *testing.T) {
	ctx := context.Background()
	client := newPaymentConfigServiceTestClient(t)
	userID := int64(42)
	inviterID := int64(9001)
	code := &RedeemCode{
		ID: 102, Code: "ADMIN-SUCCESS", Type: RedeemTypeBalance, Value: 20, Status: StatusUnused,
	}
	redeemRepo := &paymentFulfillmentRedeemRepo{
		paymentOrderLifecycleRedeemRepo: paymentOrderLifecycleRedeemRepo{
			codesByCode: map[string]*RedeemCode{code.Code: code},
		},
	}
	userRepo := &mockUserRepo{getByIDUser: &User{ID: userID}}
	userRepo.updateBalanceFn = func(context.Context, int64, float64) error { return nil }
	cache := &paymentFulfillmentRedeemCacheStub{count: redeemMaxFailedAttempts}
	affiliateRepo := &paymentFulfillmentAffiliateRepoStub{
		inviteeSummary: &AffiliateSummary{
			UserID: userID, AffCode: "INVITEE", InviterID: &inviterID, CreatedAt: time.Now().Add(-time.Hour),
		},
		inviterSummary: &AffiliateSummary{
			UserID: inviterID, AffCode: "INVITER", CreatedAt: time.Now().Add(-2 * time.Hour),
		},
	}
	settingSvc := NewSettingService(&paymentFulfillmentSettingRepoStub{values: map[string]string{
		SettingKeyAffiliateEnabled:           "true",
		SettingKeyAffiliateRebateRate:        "10",
		SettingKeyAffiliateRebateFreezeHours: "0",
	}}, nil)
	affiliateSvc := NewAffiliateService(affiliateRepo, settingSvc, nil, nil)
	svc := NewRedeemService(redeemRepo, userRepo, nil, cache, nil, client, nil, affiliateSvc)

	result, err := svc.RedeemForAdminFulfillment(ctx, userID, code.Code)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, cache.getCalls)
	require.Zero(t, cache.incrementCalls)
	require.Equal(t, 1, cache.acquireCalls)
	require.Equal(t, 1, cache.releaseCalls)
	require.Len(t, redeemRepo.useCalls, 1)
	require.Len(t, affiliateRepo.accrueCalls, 1)
	require.Equal(t, inviterID, affiliateRepo.accrueCalls[0].inviterID)
	require.Equal(t, userID, affiliateRepo.accrueCalls[0].inviteeUserID)
	require.InDelta(t, 2, affiliateRepo.accrueCalls[0].amount, 1e-8)
	require.Nil(t, affiliateRepo.accrueCalls[0].sourceOrderID)
}

func TestAdminAffiliateFulfillmentCreditsOnceWithoutReferralRebate(t *testing.T) {
	ctx := context.Background()
	client := newPaymentConfigServiceTestClient(t)
	userID, inviterID := int64(42), int64(9001)
	code := &RedeemCode{ID: 103, Code: "ACTIVITY-REWARD", Type: RedeemTypeAffiliateBalance, Value: 0.5, Status: StatusUnused}
	repo := &paymentFulfillmentRedeemRepo{paymentOrderLifecycleRedeemRepo: paymentOrderLifecycleRedeemRepo{
		codesByCode: map[string]*RedeemCode{code.Code: code},
	}}
	userRepo := &mockUserRepo{getByIDUser: &User{ID: userID}}
	credits := 0
	userRepo.updateBalanceFn = func(txCtx context.Context, id int64, amount float64) error {
		require.NotNil(t, dbent.TxFromContext(txCtx))
		require.Equal(t, userID, id)
		require.Equal(t, 0.5, amount)
		require.Len(t, repo.useCalls, 1)
		credits++
		return nil
	}
	affiliateRepo := &paymentFulfillmentAffiliateRepoStub{
		inviteeSummary: &AffiliateSummary{UserID: userID, InviterID: &inviterID, CreatedAt: time.Now().Add(-time.Hour)},
		inviterSummary: &AffiliateSummary{UserID: inviterID, CreatedAt: time.Now().Add(-2 * time.Hour)},
	}
	settings := NewSettingService(&paymentFulfillmentSettingRepoStub{values: map[string]string{
		SettingKeyAffiliateEnabled: "true", SettingKeyAffiliateRebateRate: "10", SettingKeyAffiliateRebateFreezeHours: "0",
	}}, nil)
	authCache := &authCacheInvalidatorStub{}
	balanceCache := &balanceEligibilityCacheStub{}
	svc := NewRedeemService(repo, userRepo, nil, nil, &BillingCacheService{cache: balanceCache}, client, authCache,
		NewAffiliateService(affiliateRepo, settings, nil, nil))
	result, err := svc.RedeemForAdminFulfillment(ctx, userID, code.Code)
	require.NoError(t, err)
	require.Equal(t, RedeemTypeAffiliateBalance, result.Type)
	require.Equal(t, StatusUsed, result.Status)
	require.Equal(t, userID, *result.UsedBy)
	require.NotNil(t, result.UsedAt)
	require.Equal(t, 1, credits)
	require.Empty(t, affiliateRepo.accrueCalls)
	require.Equal(t, []int64{userID}, authCache.userIDs)
	require.Eventually(t, func() bool { return balanceCache.invalidateCalls.Load() == 1 }, time.Second, time.Millisecond)
	_, err = svc.RedeemForAdminFulfillment(ctx, userID, code.Code)
	require.ErrorIs(t, err, ErrRedeemCodeUsed)
	require.Equal(t, 1, credits)
	require.Len(t, repo.useCalls, 1)
	require.Empty(t, affiliateRepo.accrueCalls)
}

func TestAffiliateRewardRejectsPublicAndPaymentRedemption(t *testing.T) {
	for _, path := range []string{"public", "payment"} {
		t.Run(path, func(t *testing.T) {
			repo := &redeemRejectRepo{code: RedeemCode{ID: 1, Code: "ACTIVITY-REWARD", Type: RedeemTypeAffiliateBalance, Value: 0.5, Status: StatusUnused}}
			svc := NewRedeemService(repo, nil, nil, nil, nil, nil, nil, nil)
			var result *RedeemCode
			var err error
			if path == "public" {
				result, err = svc.Redeem(context.Background(), 42, repo.code.Code)
			} else {
				result, err = svc.redeemForPaymentFulfillment(context.Background(), 42, repo.code.Code)
			}
			require.Nil(t, result)
			require.Equal(t, "REDEEM_CODE_UNSUPPORTED_TYPE", infraerrors.Reason(err))
			require.False(t, repo.useCalled)
			require.Equal(t, StatusUnused, repo.code.Status)
		})
	}
}

func TestAffiliateRewardRejectsInvalidAmountsBeforeMutation(t *testing.T) {
	for _, value := range []float64{0, -0.5, math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			repo := &redeemRejectRepo{code: RedeemCode{ID: 1, Code: "ACTIVITY-REWARD", Type: RedeemTypeAffiliateBalance, Value: value, Status: StatusUnused}}
			svc := NewRedeemService(repo, nil, nil, nil, nil, nil, nil, nil)
			require.Equal(t, "REDEEM_CODE_INVALID", infraerrors.Reason(svc.CreateCode(context.Background(), &repo.code)))
			result, err := svc.RedeemForAdminFulfillment(context.Background(), 42, repo.code.Code)
			require.Nil(t, result)
			require.Equal(t, "REDEEM_CODE_INVALID", infraerrors.Reason(err))
			require.False(t, repo.useCalled)
		})
	}
}
