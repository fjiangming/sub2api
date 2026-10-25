package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func recoveryImportGroupInput() *service.BillingRecoveryImportGroup {
	in := &service.BillingRecoveryImportGroup{Identity: strings.Repeat("c", 64), PayloadDigest: strings.Repeat("d", 64)}
	group := int64(7)
	for i := 0; i < 2; i++ {
		source := *recoveryImportInput()
		source.CaseID, source.ErrorID, source.EventIndex = "", int64(10+i), -1
		source.BillIdentity = fmt.Sprintf("%064x", 10+i)
		in.Members = append(in.Members, service.BillingRecoveryImportGroupMember{Source: source,
			Usage: &service.UsageLog{UserID: 1, APIKeyID: 2, AccountID: 3, GroupID: &group, Model: "test",
				InputTokens: 100 + i, OutputTokens: 200, TotalCost: 0.01, ActualCost: 0.009, RateMultiplier: 0.9}})
	}
	return in
}

func expectGroupSource(mock sqlmock.Sqlmock, in *service.BillingRecoveryImport, exists bool) {
	mock.ExpectQuery("SELECT user_id,api_key_id,account_id,group_id").WithArgs(in.ErrorID).
		WillReturnRows(sqlmock.NewRows([]string{"user", "key", "account", "group", "at", "model", "upstream", "platform", "path", "request", "client", "events", "stream"}).
			AddRow(1, 2, 3, 7, in.StartedAt.Add(125*time.Second), "test", "test", "anthropic", "/v1/messages", fmt.Sprintf("root-%d", in.ErrorID), "", nil, false))
	mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM usage_logs").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(exists))
	if !exists {
		mock.ExpectQuery("SELECT subscription_type FROM groups").WithArgs(7).WillReturnRows(sqlmock.NewRows([]string{"type"}).AddRow("standard"))
	}
}

func expectGroupStart(mock sqlmock.Sqlmock, in *service.BillingRecoveryImportGroup, quotas bool) {
	expectImportLock(mock)
	mock.ExpectQuery("SELECT payload_digest,receipt").WithArgs(3, in.Identity).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT COALESCE\\(k.rate_limit_5h").WithArgs(2, 3, 1).
		WillReturnRows(sqlmock.NewRows([]string{"quota"}).AddRow(quotas))
}

func TestBillingRecoveryImportGroupReservesAllMembersOrRollsBack(t *testing.T) {
	for _, failLast := range []bool{false, true} {
		t.Run(fmt.Sprint(failLast), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			in := recoveryImportGroupInput()
			expectGroupStart(mock, in, false)
			for i := range in.Members {
				expectGroupSource(mock, &in.Members[i].Source, false)
			}
			for i := range in.Members {
				mock.ExpectQuery("SELECT case_id,account_id").WillReturnError(sql.ErrNoRows)
				expectGroupSource(mock, &in.Members[i].Source, false)
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery").WillReturnResult(sqlmock.NewResult(0, 1))
				claim := mock.ExpectExec("INSERT INTO sub2api_custom.billing_import_claims")
				if failLast && i == len(in.Members)-1 {
					claim.WillReturnError(errors.New("unique source conflict"))
					break
				}
				claim.WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery_events").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("UPDATE sub2api_custom.billing_recovery SET request_snapshot").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			if failLast {
				mock.ExpectRollback()
			} else {
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_import_groups").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			}
			receipt, err := (&billingRecoveryRepository{db: db}).ReserveImportGroup(context.Background(), in, "admin:9")
			if failLast {
				require.Error(t, err)
				require.Nil(t, receipt)
			} else {
				require.NoError(t, err)
				require.Len(t, receipt, 2)
				require.NotEqual(t, receipt[0].CaseID, receipt[1].CaseID)
				require.Equal(t, "error:10:-1", receipt[0].LocalReference)
			}
			require.NoError(t, mock.ExpectationsWereMet(), "no usage or balance writes are allowed during reservation")
		})
	}
}

func TestBillingRecoveryImportGroupRetriesOriginalReceiptAndRejectsChangedDigest(t *testing.T) {
	for _, changed := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		in := recoveryImportGroupInput()
		receipt := []service.BillingRecoveryImportClaim{{CaseID: "original-1"}, {CaseID: "original-2"}}
		saved, err := json.Marshal(receipt)
		require.NoError(t, err)
		digest := in.PayloadDigest
		if changed {
			digest = strings.Repeat("e", 64)
		}
		expectImportLock(mock)
		mock.ExpectQuery("SELECT payload_digest,receipt").WithArgs(3, in.Identity).
			WillReturnRows(sqlmock.NewRows([]string{"digest", "receipt"}).AddRow(digest, saved))
		if changed {
			mock.ExpectRollback()
		} else {
			mock.ExpectCommit()
		}
		actual, err := (&billingRecoveryRepository{db: db}).ReserveImportGroup(context.Background(), in, "another-admin")
		if changed {
			require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
		} else {
			require.NoError(t, err)
			require.Equal(t, receipt, actual)
		}
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}

func TestBillingRecoveryImportGroupBlocksQuotasAndExistingAliasesBeforeStaging(t *testing.T) {
	for _, quotas := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		in := recoveryImportGroupInput()
		expectGroupStart(mock, in, quotas)
		if !quotas {
			expectGroupSource(mock, &in.Members[0].Source, false)
			expectGroupSource(mock, &in.Members[1].Source, true)
		}
		mock.ExpectRollback()
		_, err = (&billingRecoveryRepository{db: db}).ReserveImportGroup(context.Background(), in, "admin")
		require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}

func TestBillingRecoveryImportGroupRejectsDuplicateOrWrongIdentityWithoutDatabaseWrites(t *testing.T) {
	for _, invalid := range []string{"bill", "source", "owner", "subscription", "amount", "size"} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		in := recoveryImportGroupInput()
		in.Members[1].Source.BillIdentity = strings.Repeat("f", 64)
		switch invalid {
		case "bill":
			in.Members[1].Source.BillIdentity = in.Members[0].Source.BillIdentity
		case "source":
			in.Members[1].Source.ErrorID = in.Members[0].Source.ErrorID
		case "owner":
			in.Members[1].Usage.UserID = 9
		case "subscription":
			id := int64(9)
			in.Members[1].Usage.SubscriptionID = &id
		case "amount":
			in.Members[1].Usage.ActualCost = 100
		case "size":
			in.Members = in.Members[:1]
		}
		_, err = (&billingRecoveryRepository{db: db}).ReserveImportGroup(context.Background(), in, "admin")
		require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}
