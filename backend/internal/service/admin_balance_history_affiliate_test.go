package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/stretchr/testify/require"
)

type affiliateHistoryRedeemRepo struct {
	RedeemCodeRepository
	codes   []RedeemCode
	filters []string
}

func (r *affiliateHistoryRedeemRepo) ListByUserPaginated(_ context.Context, userID int64, _ pagination.PaginationParams, codeType string) ([]RedeemCode, *pagination.PaginationResult, error) {
	r.filters = append(r.filters, codeType)
	var result []RedeemCode
	for _, code := range r.codes {
		if code.UsedBy != nil && *code.UsedBy == userID && (codeType == "" || code.Type == codeType) {
			result = append(result, code)
		}
	}
	return result, &pagination.PaginationResult{Total: int64(len(result))}, nil
}

func (*affiliateHistoryRedeemRepo) SumPositiveBalanceByUser(context.Context, int64) (float64, error) {
	return 50, nil
}

func TestAdminBalanceHistoryIncludesAffiliateRewardsAndNativeTransfers(t *testing.T) {
	for _, tc := range []struct {
		codeType string
		page     int
		wantID   int64
		total    int64
	}{
		{RedeemTypeAffiliateBalance, 1, 10, 2}, {RedeemTypeAffiliateBalance, 2, -20, 2},
		{"", 1, 10, 3}, {"", 2, -20, 3}, {"", 3, 30, 3}, {RedeemTypeBalance, 1, 30, 1},
	} {
		t.Run(fmt.Sprintf("%s/page%d", tc.codeType, tc.page), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
			t.Cleanup(func() { _ = client.Close() })
			base := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
			userID := int64(42)
			latest, older := base.Add(3*time.Hour), base.Add(time.Hour)
			repo := &affiliateHistoryRedeemRepo{codes: []RedeemCode{
				{ID: 10, Code: "AC-reward", Type: RedeemTypeAffiliateBalance, Value: 0.5, UsedBy: &userID, UsedAt: &latest},
				{ID: 30, Code: "paid", Type: RedeemTypeBalance, Value: 50, UsedBy: &userID, UsedAt: &older},
			}}
			if tc.codeType != RedeemTypeBalance {
				mock.ExpectQuery(`SELECT id,\s+amount::double precision,\s+created_at\s+FROM user_affiliate_ledger`).
					WithArgs(userID, 0, 1000).
					WillReturnRows(sqlmock.NewRows([]string{"id", "amount", "created_at"}).AddRow(20, 3, base.Add(2*time.Hour)))
				mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM user_affiliate_ledger`).WithArgs(userID).
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			}
			svc := &adminServiceImpl{entClient: client, redeemCodeRepo: repo}
			rows, total, recharged, err := svc.GetUserBalanceHistory(context.Background(), userID, tc.page, 1, tc.codeType)
			require.NoError(t, err)
			require.Equal(t, tc.total, total)
			require.Equal(t, 50.0, recharged)
			require.Len(t, rows, 1)
			require.Equal(t, tc.wantID, rows[0].ID)
			require.Equal(t, []string{tc.codeType}, repo.filters)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
