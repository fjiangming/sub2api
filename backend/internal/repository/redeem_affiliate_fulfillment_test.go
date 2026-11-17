package repository

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type failingRewardCreditUserRepo struct{ service.UserRepository }

func (r failingRewardCreditUserRepo) UpdateBalance(ctx context.Context, id int64, amount float64) error {
	if err := r.UserRepository.UpdateBalance(ctx, id, amount); err != nil {
		return err
	}
	return errors.New("simulated failure after balance write")
}

func TestAffiliateRewardRealTransactionAndRechargeExclusion(t *testing.T) {
	ctx := context.Background()
	users, client := newUserEntRepo(t)
	user, err := client.User.Create().SetEmail("reward@example.test").SetPasswordHash("hash").SetBalance(10).Save(ctx)
	require.NoError(t, err)
	codes := NewRedeemCodeRepository(client)
	svc := service.NewRedeemService(codes, users, nil, nil, nil, client, nil, nil)
	normal := &service.RedeemCode{Code: "NORMAL-RECHARGE", Type: service.RedeemTypeBalance, Value: 50}
	require.NoError(t, svc.CreateCode(ctx, normal))
	_, err = svc.RedeemForAdminFulfillment(ctx, user.ID, normal.Code)
	require.NoError(t, err)
	reward := &service.RedeemCode{
		Code: "AC-" + strings.Repeat("a", 48), Type: service.RedeemTypeAffiliateBalance, Value: 0.5,
		Notes: "活动奖励; activity=recharge-lottery; reward=example; batch=example",
	}
	require.NoError(t, svc.CreateCode(ctx, reward))

	// Both the used marker and the balance update must roll back together.
	failing := service.NewRedeemService(codes, failingRewardCreditUserRepo{users}, nil, nil, nil, client, nil, nil)
	_, err = failing.RedeemForAdminFulfillment(ctx, user.ID, reward.Code)
	require.ErrorContains(t, err, "simulated failure")
	afterFailure, err := client.User.Get(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, 60.0, afterFailure.Balance)
	unused, err := codes.GetByCode(ctx, reward.Code)
	require.NoError(t, err)
	require.Equal(t, service.StatusUnused, unused.Status)
	require.Nil(t, unused.UsedAt)
	require.Nil(t, unused.UsedBy)

	receipt, err := svc.RedeemForAdminFulfillment(ctx, user.ID, reward.Code)
	require.NoError(t, err)
	require.Equal(t, service.RedeemTypeAffiliateBalance, receipt.Type)
	require.Equal(t, service.StatusUsed, receipt.Status)
	require.Equal(t, user.ID, *receipt.UsedBy)
	require.NotNil(t, receipt.UsedAt)
	_, err = svc.RedeemForAdminFulfillment(ctx, user.ID, reward.Code)
	require.ErrorIs(t, err, service.ErrRedeemCodeUsed)
	storedReceipt, err := codes.GetByCode(ctx, reward.Code)
	require.NoError(t, err)
	require.Equal(t, receipt, storedReceipt)
	afterCredit, err := client.User.Get(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, 60.5, afterCredit.Balance)
	total, err := codes.SumPositiveBalanceByUser(ctx, user.ID)
	require.NoError(t, err)
	require.Equal(t, 50.0, total)
	history, page, err := codes.ListByUserPaginated(ctx, user.ID, pagination.PaginationParams{Page: 1, PageSize: 20}, service.RedeemTypeAffiliateBalance)
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Total)
	require.Len(t, history, 1)
	require.Equal(t, reward.Code, history[0].Code)
	require.Equal(t, 0.5, history[0].Value)
}
