package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func recoveryImportInput() *service.BillingRecoveryImport {
	return &service.BillingRecoveryImport{AccountID: 3, BillIdentity: strings.Repeat("a", 64), PayloadDigest: strings.Repeat("b", 64),
		CaseID: "case-1", Version: 1, UserID: 1, APIKeyID: 2, StartedAt: time.Now().Add(-4 * time.Hour), Model: "test", Evidence: "verified CSV and historical prices"}
}

func expectImportLock(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WithArgs("billing-import:3").WillReturnResult(sqlmock.NewResult(0, 1))
}

func importCaseRows(state string, userID int64, version int) *sqlmock.Rows {
	at := time.Now().Add(-4 * time.Hour)
	return sqlmock.NewRows(strings.Split(recoveryColumns, ",")).AddRow("case-1", "root", userID, 2, 7, nil, 3,
		"anthropic", "/v1/messages", "test", "", state, nil, nil, nil, nil, "", "", 0, version,
		at, at, at, false, nil, []byte(`{}`))
}

func TestBillingRecoveryImportReservationIsIdempotentAndBindsPreview(t *testing.T) {
	for _, mismatch := range []string{"", "bill", "digest", "reference", "account"} {
		t.Run(mismatch, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			in := recoveryImportInput()
			bill, digest, reference, account := in.BillIdentity, in.PayloadDigest, "case:case-1", int64(3)
			switch mismatch {
			case "bill":
				bill = strings.Repeat("c", 64)
			case "digest":
				digest = strings.Repeat("c", 64)
			case "reference":
				reference = "case:another"
			case "account":
				account = 4
			}
			expectImportLock(mock)
			mock.ExpectQuery("SELECT case_id,account_id,bill_identity,payload_digest,local_reference").
				WithArgs(3, in.BillIdentity, "case:case-1").WillReturnRows(sqlmock.NewRows([]string{"id", "account", "bill", "digest", "reference"}).AddRow("case-1", account, bill, digest, reference))
			if mismatch == "" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			id, err := (&billingRecoveryRepository{db: db}).ReserveImport(context.Background(), in, "admin:9")
			if mismatch == "" {
				require.NoError(t, err)
				require.Equal(t, "case-1", id)
			} else {
				require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBillingRecoveryImportRejectsStaleOrWrongOwnershipBeforeClaim(t *testing.T) {
	for _, fixture := range []struct {
		state string
		user  int64
		ver   int
	}{{"settled", 1, 1}, {"unknown", 9, 1}, {"unknown", 1, 2}} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		in := recoveryImportInput()
		expectImportLock(mock)
		mock.ExpectQuery("SELECT case_id,account_id").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("SELECT id,request_root").WithArgs("case-1").WillReturnRows(importCaseRows(fixture.state, fixture.user, fixture.ver))
		mock.ExpectRollback()
		_, err = (&billingRecoveryRepository{db: db}).ReserveImport(context.Background(), in, "admin:9")
		require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}

func TestBillingRecoveryImportReservesExistingCaseWithoutMoneyWrites(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	in := recoveryImportInput()
	expectImportLock(mock)
	mock.ExpectQuery("SELECT case_id,account_id").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT id,request_root").WillReturnRows(importCaseRows("unknown", 1, 1))
	mock.ExpectExec("INSERT INTO sub2api_custom.billing_import_claims").WithArgs(3, in.BillIdentity, "case:case-1", in.PayloadDigest, "case-1", "admin:9").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	caseID, err := (&billingRecoveryRepository{db: db}).ReserveImport(context.Background(), in, "admin:9")
	require.NoError(t, err)
	require.Equal(t, "case-1", caseID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBillingRecoveryImportLegacyValidatesNativeSourceAndSubscription(t *testing.T) {
	for _, scenario := range []string{"balance", "subscription", "already_billed", "deleted_key", "ambiguous_subscription"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			in := recoveryImportInput()
			in.CaseID, in.ErrorID, in.EventIndex, in.UpstreamID = "", 10, 0, "provider-1"
			expectImportLock(mock)
			mock.ExpectQuery("SELECT case_id,account_id").WillReturnError(sql.ErrNoRows)
			events, err := json.Marshal([]service.OpsUpstreamErrorEvent{{AccountID: 3, UpstreamRequestID: "provider-1"}})
			require.NoError(t, err)
			mock.ExpectQuery("SELECT user_id,api_key_id,account_id,group_id").WithArgs(10).WillReturnRows(sqlmock.NewRows([]string{
				"user", "key", "account", "group", "at", "model", "upstream", "platform", "path", "request", "client", "events", "stream",
			}).AddRow(1, 2, 3, 7, in.StartedAt.Add(125*time.Second), "test", "test", "anthropic", "/v1/messages", "root", "", events, false))
			mock.ExpectQuery("SELECT EXISTS\\(SELECT 1 FROM usage_logs").WithArgs(1, 2, 3, `{"root","provider-1"}`).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(scenario == "already_billed"))
			groupType := "standard"
			if strings.Contains(scenario, "subscription") {
				groupType = "subscription"
			}
			if scenario != "already_billed" {
				mock.ExpectQuery("SELECT subscription_type FROM groups").WithArgs(7).WillReturnRows(sqlmock.NewRows([]string{"type"}).AddRow(groupType))
				if groupType == "subscription" {
					rows := sqlmock.NewRows([]string{"id"}).AddRow(20)
					if scenario == "ambiguous_subscription" {
						rows.AddRow(21)
					}
					mock.ExpectQuery("SELECT id FROM user_subscriptions").WithArgs(1, 7, in.StartedAt).WillReturnRows(rows)
				}
				if scenario != "ambiguous_subscription" {
					count := int64(1)
					if scenario == "deleted_key" {
						count = 0
					}
					mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery").WillReturnResult(sqlmock.NewResult(0, count))
				}
			}
			if scenario == "balance" || scenario == "subscription" {
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_import_claims").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery_events").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			caseID, err := (&billingRecoveryRepository{db: db}).ReserveImport(context.Background(), in, "admin:9")
			if scenario == "balance" || scenario == "subscription" {
				require.NoError(t, err)
				require.NotEmpty(t, caseID)
			} else {
				require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBillingRecoveryImportRefusesInvalidIdentityOrActiveRequest(t *testing.T) {
	for _, invalid := range []string{"digest", "recent", "both_sources", "missing_user"} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		in := recoveryImportInput()
		switch invalid {
		case "digest":
			in.BillIdentity = "invalid"
		case "recent":
			in.StartedAt = time.Now()
		case "both_sources":
			in.ErrorID = 9
		case "missing_user":
			in.UserID = 0
		}
		_, err = (&billingRecoveryRepository{db: db}).ReserveImport(context.Background(), in, "admin:9")
		require.ErrorIs(t, err, service.ErrBillingRecoveryConflict)
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}

func TestBillingRecoveryImportDateRangeKeepsPaginationParameters(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()
	ctx := service.WithBillingRecoveryFilter(context.Background(), service.BillingRecoveryListFilter{AccountID: 356, From: &from, To: &to})
	args := []driver.Value{"", 0, 0, 356, false, "", from, to}
	mock.ExpectQuery(`(?s)SELECT count.*created_at >= \$7 AND created_at < \$8`).WithArgs(args...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`(?s)SELECT id,request_root.*LIMIT \$9 OFFSET \$10`).WithArgs(append(args, 20, 40)...).WillReturnRows(sqlmock.NewRows(strings.Split(recoveryColumns, ",")))
	rows, total, err := (&billingRecoveryRepository{db: db}).List(ctx, "", 20, 40)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Zero(t, total)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBillingRecoveryLogImportChecksArchiveAndQuotesWithoutWrites(t *testing.T) {
	for _, scenario := range []string{"inspect", "reserve", "already_billed"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			in := recoveryImportInput()
			in.CaseID, in.Version = "", 0
			in.LogSource = &service.BillingRecoveryLogSource{RequestID: "server-log-id", ClientID: "client-log-id", GroupID: 7,
				Model: in.Model, EndedAt: in.StartedAt.Add(125 * time.Second), LatencyMS: 125000, StatusCode: 499, Digest: strings.Repeat("c", 64)}
			if scenario == "reserve" {
				expectImportLock(mock)
				mock.ExpectQuery("SELECT case_id,account_id").WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectBegin()
			}
			mock.ExpectQuery("SELECT platform FROM accounts").WithArgs(3).WillReturnRows(sqlmock.NewRows([]string{"platform"}).AddRow("anthropic"))
			mock.ExpectQuery(`(?s)SELECT EXISTS.*usage_billing_dedup_archive`).
				WithArgs(1, 2, 3, `{"server-log-id","client-log-id","client:server-log-id","client:client-log-id"}`).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(scenario == "already_billed"))
			if scenario != "already_billed" {
				mock.ExpectQuery("SELECT subscription_type FROM groups").WithArgs(7).WillReturnRows(sqlmock.NewRows([]string{"type"}).AddRow("standard"))
			}
			if scenario == "reserve" {
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_import_claims").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO sub2api_custom.billing_recovery_events").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
				_, err = (&billingRecoveryRepository{db: db}).ReserveImport(context.Background(), in, "admin:9")
				require.NoError(t, err)
			} else {
				mock.ExpectRollback()
				entry, readErr := (&billingRecoveryRepository{db: db}).ImportSource(context.Background(), in)
				if scenario == "already_billed" {
					require.ErrorIs(t, readErr, service.ErrBillingRecoveryConflict)
				} else {
					require.NoError(t, readErr)
					require.Equal(t, "historical-log:server-log-id", entry.RequestRoot)
					require.EqualValues(t, 7, *entry.GroupID)
					require.Equal(t, in.StartedAt, entry.CreatedAt)
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBillingRecoveryLogImportRejectsIncompleteAndConflictingSources(t *testing.T) {
	for _, scenario := range []string{"no_id", "bad_digest", "wrong_model", "live", "bad_latency", "success", "two_sources"} {
		in := recoveryImportInput()
		in.CaseID, in.Version = "", 0
		in.LogSource = &service.BillingRecoveryLogSource{RequestID: "server-id", GroupID: 7, Model: "test", Digest: strings.Repeat("c", 64),
			EndedAt: in.StartedAt.Add(125 * time.Second), LatencyMS: 125000, StatusCode: 499}
		switch scenario {
		case "no_id":
			in.LogSource.RequestID = ""
		case "bad_digest":
			in.LogSource.Digest = "unverified"
		case "wrong_model":
			in.LogSource.Model = "other"
		case "live":
			in.LogSource.EndedAt = time.Now()
		case "bad_latency":
			in.LogSource.LatencyMS = 1
		case "success":
			in.LogSource.StatusCode = 200
		case "two_sources":
			in.ErrorID = 10
		}
		_, err := (&billingRecoveryRepository{}).ReserveImport(context.Background(), in, "admin:9")
		require.ErrorIs(t, err, service.ErrBillingRecoveryConflict, scenario)
	}
}
