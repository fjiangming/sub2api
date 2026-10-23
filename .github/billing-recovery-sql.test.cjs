'use strict';

// Runs the actual fork migration and historical SQL in embedded PostgreSQL.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const modulePath = process.env.PGLITE_MODULE_PATH || '../../sub2api-extra/operations-center/node_modules/@electric-sql/pglite';
const { PGlite } = require(modulePath.startsWith('.') ? path.resolve(__dirname, modulePath) : modulePath);
const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'backend/internal/repository/billing_recovery_settlement.go'), 'utf8');
const queries = [...source.matchAll(/`([^`]+)`/g)].map((match) => match[1]);

test('dispatch snapshots and filtered timeout lookups retain exact users, accounts and response correlation', async () => {
 const db = new PGlite();
 try {
  await db.exec(fs.readFileSync(path.join(root, 'backend/migrations/custom_20261023_billing_recovery.sql'), 'utf8'));
  const lookupMigration = fs.readFileSync(path.join(root, 'backend/migrations/custom_20261023_billing_recovery_lookup.sql'), 'utf8');
  await db.exec(lookupMigration); await db.exec(lookupMigration);
  await db.exec(`CREATE TABLE users(id bigint,email text,username text);
   CREATE TABLE api_keys(id bigint,name text); CREATE TABLE groups(id bigint,name text);
   CREATE TABLE accounts(id bigint,name text,type text,extra jsonb,rate_multiplier numeric);
   INSERT INTO users VALUES(1,'owner@example.com','owner'); INSERT INTO api_keys VALUES(2,'private-key-name');
   INSERT INTO groups VALUES(7,'group'); INSERT INTO accounts VALUES(3,'vendor','apikey','{}',0.8);`);
  const repo = fs.readFileSync(path.join(root, 'backend/internal/repository/billing_recovery_repo.go'), 'utf8');
  const sql = [...repo.matchAll(/`([^`]+)`/g)].map(match => match[1]);
  const begin = sql.find(query => query.includes("'user_email'"));
  const observe = sql.find(query => query.includes('SET http_status=$2'));
  const filter = sql.find(query => query.includes("$1='incomplete'"));
  await db.query(begin, ['case','root',1,2,7,null,3,'anthropic','/v1/messages','test',JSON.stringify({dispatch_correlation_id:'dispatch'})]);
  await db.query(observe, ['case',200,'provider-request',JSON.stringify({input_tokens:13,output_tokens:21,
   _recovery:{complete:true,client_disconnected:true,phase:'finished',response_request_ids:{'x-client-request-id':'provider-client'}}}), '']);
  const read = (args) => db.query(`SELECT id,request_snapshot,observed_usage FROM sub2api_custom.billing_recovery WHERE ${filter}`,args);
  const rows = (await read(['',1,2,3,true,'provider-client'])).rows;
  assert.equal(rows.length, 1); assert.equal(rows[0].request_snapshot.user_email,'owner@example.com');
  assert.equal(rows[0].request_snapshot.account_name,'vendor'); assert.equal(rows[0].request_snapshot.account_rate_multiplier,0.8);
  assert.equal(rows[0].observed_usage.output_tokens,21);
  assert.equal((await read(['',999,2,3,true,'provider-client'])).rows.length,0);
  await db.exec(`UPDATE sub2api_custom.billing_recovery SET state='settled',observed_usage=jsonb_set(observed_usage,'{_recovery,complete}','false')`);
  assert.equal((await read(['incomplete',0,0,0,false,''])).rows.length,1);
 } finally { await db.close(); }
});

test('historical settlement retains old usage without consuming a later quota window', async () => {
 const db = new PGlite();
 try {
  const migration = fs.readFileSync(path.join(root, 'backend/migrations/custom_20261023_billing_recovery.sql'), 'utf8');
  await db.exec(migration); await db.exec(migration);
  await db.exec(`CREATE TABLE user_subscriptions(id bigint PRIMARY KEY, deleted_at timestamptz, updated_at timestamptz,
   daily_usage_usd numeric DEFAULT 0,weekly_usage_usd numeric DEFAULT 0,monthly_usage_usd numeric DEFAULT 0,
   daily_window_start timestamptz,weekly_window_start timestamptz,monthly_window_start timestamptz);
   CREATE TABLE api_keys(id bigint PRIMARY KEY,deleted_at timestamptz,updated_at timestamptz,
   usage_5h numeric DEFAULT 0,usage_1d numeric DEFAULT 0,usage_7d numeric DEFAULT 0,
   window_5h_start timestamptz,window_1d_start timestamptz,window_7d_start timestamptz);
   CREATE TABLE accounts(id bigint PRIMARY KEY,deleted_at timestamptz,updated_at timestamptz,extra jsonb);
   INSERT INTO user_subscriptions(id,daily_window_start,weekly_window_start,monthly_window_start) VALUES(1,'2026-10-23','2026-10-20','2026-10-01');
   INSERT INTO api_keys(id,window_5h_start,window_1d_start,window_7d_start) VALUES(1,'2026-10-23','2026-10-23','2026-10-20');
   INSERT INTO accounts(id,extra) VALUES(1,'{"quota_used":0,"quota_daily_start":"2026-10-23T00:00:00Z","quota_weekly_start":"2026-10-20T00:00:00Z"}');`);
  for (const prefix of ['UPDATE user_subscriptions SET', 'UPDATE api_keys SET', 'UPDATE accounts SET']) {
   const sql = queries.find((query) => query.trim().startsWith(prefix)); assert.ok(sql, prefix);
   await db.query(sql, prefix.includes('user_subscriptions') ? [0.5, 1, false, true, true] : [0.5, 1, '2026-10-22T12:00:00Z']);
  }
  const sub = (await db.query('SELECT daily_usage_usd,weekly_usage_usd,monthly_usage_usd FROM user_subscriptions')).rows[0];
  assert.equal(Number(sub.daily_usage_usd), 0);assert.equal(Number(sub.weekly_usage_usd), 0.5);assert.equal(Number(sub.monthly_usage_usd), 0.5);
  const key = (await db.query('SELECT usage_5h,usage_1d,usage_7d FROM api_keys')).rows[0];
  assert.equal(Number(key.usage_5h), 0);assert.equal(Number(key.usage_1d), 0);assert.equal(Number(key.usage_7d), 0.5);
  const extra = (await db.query('SELECT extra FROM accounts')).rows[0].extra;
  assert.equal(extra.quota_used, 0.5);assert.equal(extra.quota_daily_used, 0);assert.equal(extra.quota_weekly_used, 0.5);
 } finally { await db.close(); }
});

test('missing prices persist usage for review in both dispatch and background preparation', async () => {
 const db = new PGlite();
 try {
  await db.exec(fs.readFileSync(path.join(root, 'backend/migrations/custom_20261023_billing_recovery.sql'), 'utf8'));
  const repo = fs.readFileSync(path.join(root, 'backend/internal/repository/billing_recovery_repo.go'), 'utf8');
  const sql = [...repo.matchAll(/`([^`]+)`/g)].map((match) => match[1]);
  const insert = sql.find((query) => query.includes("ON CONFLICT(api_key_id,native_request_id) DO NOTHING"));
  const update = sql.find((query) => query.includes('SET native_request_id=$2'));
  const usage = JSON.stringify({ Model: 'unpriced', InputTokens: 100 });
  const command = JSON.stringify({ RequestID: 'dispatch:one', RequestFingerprint: 'fingerprint' });
  await db.query(insert, ['one', 1, 2, null, null, 3, 'unpriced', 'dispatch:one', usage, command, 'review', 'PRICING_UNAVAILABLE']);
  await db.query(`INSERT INTO sub2api_custom.billing_recovery(id,request_root,user_id,api_key_id,account_id) VALUES('two','root',1,2,3)`);
  await db.query(update, ['two', 'dispatch:two', usage, command, 'unpriced', 1, 2, 3, null, null, null, 'review', 'PRICING_UNAVAILABLE']);
  const rows = (await db.query('SELECT state,last_error,usage_record FROM sub2api_custom.billing_recovery ORDER BY id')).rows;
  assert.equal(rows.length, 2);
  for (const row of rows) {
   assert.equal(row.state, 'review'); assert.equal(row.last_error, 'PRICING_UNAVAILABLE');
   assert.equal(row.usage_record.InputTokens, 100);
  }
 } finally { await db.close(); }
});

test('CSV legacy reservation stages only verified native attribution and enforces both uniqueness claims', async () => {
 const db = new PGlite();
 try {
  await db.exec(fs.readFileSync(path.join(root, 'backend/migrations/custom_20261023_billing_recovery.sql'), 'utf8'));
  const migration = fs.readFileSync(path.join(root, 'backend/migrations/custom_20261023_billing_recovery_csv_import.sql'), 'utf8');
  await db.exec(migration); await db.exec(migration);
  await db.exec(`CREATE TABLE users(id bigint,email text,deleted_at timestamptz);
   CREATE TABLE api_keys(id bigint,user_id bigint,name text,deleted_at timestamptz);
   CREATE TABLE accounts(id bigint,name text,type text,rate_multiplier numeric,extra jsonb,deleted_at timestamptz);
   CREATE TABLE usage_logs(user_id bigint,api_key_id bigint,account_id bigint,request_id text,upstream_request_id text);
   CREATE TABLE usage_billing_dedup(api_key_id bigint,request_id text);
   CREATE TABLE usage_billing_dedup_archive(api_key_id bigint,request_id text);
   INSERT INTO users VALUES(1,'owner@example.test',NULL);
   INSERT INTO api_keys VALUES(2,1,'downstream-key',NULL);
   INSERT INTO accounts VALUES(3,'supplier','apikey',0.8,'{"quota_limit":10}',NULL);`);
  const repo = fs.readFileSync(path.join(root, 'backend/internal/repository/billing_recovery_import.go'), 'utf8');
  const sql = [...repo.matchAll(/`([^`]+)`/g)].map(match => match[1]);
  const stage = sql.find(query => query.includes("'user_email'"));
  const existing = sql.find(query => query.includes('SELECT EXISTS(SELECT 1 FROM usage_logs'));
  const claim = sql.find(query => query.includes('INSERT INTO sub2api_custom.billing_import_claims'));
  const parameters = ['csv-case','historical-error:10:0',1,2,3,7,null,'anthropic','/v1/messages','test',
   'upstream-1',JSON.stringify({request_id:'root-1',csv_import:true}),'verified CSV and pricing','2026-10-22T01:08:08Z'];
  await db.query(stage, parameters);
  const row = (await db.query('SELECT * FROM sub2api_custom.billing_recovery')).rows[0];
  assert.equal(row.state,'review'); assert.equal(row.usage_record,null); assert.equal(row.billing_command,null);
  assert.equal(row.user_id,1); assert.equal(row.api_key_id,2); assert.equal(row.account_id,3);
  assert.equal(row.request_snapshot.user_email,'owner@example.test'); assert.equal(row.request_snapshot.account_quota,true);
  assert.equal(row.request_snapshot.account_rate_multiplier,0.8);
  assert.equal((await db.query(existing,[1,2,3,['root-1']])).rows[0].exists,true,'native root detects an already staged intent');
  assert.equal((await db.query(existing,[1,2,3,['not-this-request']])).rows[0].exists,false);
  await db.exec("INSERT INTO usage_logs VALUES(1,2,3,'native-billed','provider-billed')");
  assert.equal((await db.query(existing,[1,2,3,['provider-billed']])).rows[0].exists,true);
  await db.exec("INSERT INTO usage_billing_dedup_archive VALUES(2,'old-billed-request')");
  assert.equal((await db.query(existing,[1,2,3,['old-billed-request']])).rows[0].exists,true,'retained dedup protects bills after usage detail cleanup');
  await db.query(claim,[3,'bill-1','error:10:0','preview-1','csv-case','admin:1']);
  await assert.rejects(db.query(claim,[3,'bill-1','error:11:0','preview-1','csv-case','admin:2']),/duplicate key/);
  await assert.rejects(db.query(claim,[3,'bill-2','error:10:0','preview-2','csv-case','admin:2']),/duplicate key/);
  assert.equal((await db.query('SELECT COUNT(*) FROM sub2api_custom.billing_import_claims')).rows[0].count,1);
  await db.exec('UPDATE api_keys SET deleted_at=NOW()');
  assert.equal((await db.query(stage,['deleted-case',...parameters.slice(1)])).affectedRows,0);
 } finally { await db.close(); }
});
