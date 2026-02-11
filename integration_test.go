//go:build integration

package pgmcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

// --- Query Tool Integration Tests ---

func TestQuery_SelectBasic(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text, email text)")
	setupTable(t, p, "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT id, name, email FROM users ORDER BY id"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(output.Columns))
	}
	if len(output.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(output.Rows))
	}
	if output.Rows[0]["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", output.Rows[0]["name"])
	}
	if output.Rows[1]["name"] != "Bob" {
		t.Fatalf("expected Bob, got %v", output.Rows[1]["name"])
	}
}

func TestQuery_Insert(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "INSERT INTO users (name) VALUES ('Charlie') RETURNING id, name"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	if output.Rows[0]["name"] != "Charlie" {
		t.Fatalf("expected Charlie, got %v", output.Rows[0]["name"])
	}
	if output.RowsAffected != 1 {
		t.Fatalf("expected RowsAffected=1, got %d", output.RowsAffected)
	}
}

func TestQuery_Update(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO users (name) VALUES ('Dave')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "UPDATE users SET name = 'David' WHERE name = 'Dave' RETURNING name"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.Rows[0]["name"] != "David" {
		t.Fatalf("expected David, got %v", output.Rows[0]["name"])
	}
}

func TestQuery_Delete(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO users (name) VALUES ('Eve')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "DELETE FROM users WHERE name = 'Eve' RETURNING name"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 || output.Rows[0]["name"] != "Eve" {
		t.Fatalf("expected deleted row Eve, got %v", output.Rows)
	}
}

func TestQuery_EmptyResult(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE empty_table (id serial PRIMARY KEY, name text)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM empty_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(output.Rows))
	}
	if len(output.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(output.Columns))
	}
	// Verify JSON serializes as [] not null
	b, _ := json.Marshal(output.Rows)
	if string(b) != "[]" {
		t.Fatalf("expected [], got %s", string(b))
	}
}

func TestQuery_NullValues(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE nullable_table (id serial PRIMARY KEY, name text, email text)")
	setupTable(t, p, "INSERT INTO nullable_table (name) VALUES (NULL)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT name, email FROM nullable_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.Rows[0]["name"] != nil {
		t.Fatalf("expected nil for name, got %v", output.Rows[0]["name"])
	}
}

func TestQuery_UUIDColumn(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowCreateExtension = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
	setupTable(t, p, "CREATE TABLE uuid_table (id uuid DEFAULT uuid_generate_v4() PRIMARY KEY)")
	setupTable(t, p, "INSERT INTO uuid_table DEFAULT VALUES")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT id FROM uuid_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	uuidStr, ok := output.Rows[0]["id"].(string)
	if !ok {
		t.Fatalf("expected string UUID, got %T", output.Rows[0]["id"])
	}
	// UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	if len(uuidStr) != 36 {
		t.Fatalf("expected 36 char UUID, got %q (%d)", uuidStr, len(uuidStr))
	}
}

func TestQuery_TimestampColumn(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE ts_table (created_at timestamptz DEFAULT NOW())")
	setupTable(t, p, "INSERT INTO ts_table DEFAULT VALUES")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT created_at FROM ts_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	tsStr, ok := output.Rows[0]["created_at"].(string)
	if !ok {
		t.Fatalf("expected string timestamp, got %T", output.Rows[0]["created_at"])
	}
	_, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		t.Fatalf("failed to parse timestamp %q: %v", tsStr, err)
	}
}

func TestQuery_NumericColumn(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE price_table (price numeric(10,2))")
	setupTable(t, p, "INSERT INTO price_table VALUES (123.45)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT price FROM price_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	priceStr, ok := output.Rows[0]["price"].(string)
	if !ok {
		t.Fatalf("expected string for numeric, got %T: %v", output.Rows[0]["price"], output.Rows[0]["price"])
	}
	if priceStr != "123.45" {
		t.Fatalf("expected 123.45, got %s", priceStr)
	}
}

func TestQuery_BigIntColumn(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE bigint_table (big_id bigint)")
	setupTable(t, p, "INSERT INTO bigint_table VALUES (9007199254740993)") // 2^53+1

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT big_id FROM bigint_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	// int64 should be preserved exactly
	val := output.Rows[0]["big_id"]
	if val != int64(9007199254740993) {
		t.Fatalf("expected int64(9007199254740993), got %T: %v", val, val)
	}
}

func TestQuery_ByteaColumn(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE binary_table (data bytea)")
	setupTable(t, p, "INSERT INTO binary_table VALUES (decode('deadbeef', 'hex'))")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT data FROM binary_table"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	b64, ok := output.Rows[0]["data"].(string)
	if !ok {
		t.Fatalf("expected string for bytea, got %T", output.Rows[0]["data"])
	}
	if b64 != "3q2+7w==" { // base64 of 0xdeadbeef
		t.Fatalf("expected base64 'deadbeef', got %s", b64)
	}
}

func TestQuery_SelectJSONB(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE items (data jsonb)")
	setupTable(t, p, `INSERT INTO items VALUES ('{"name":"test","count":42}')`)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT data FROM items"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	dataMap, ok := output.Rows[0]["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected map for JSONB, got %T: %v", output.Rows[0]["data"], output.Rows[0]["data"])
	}
	if dataMap["name"] != "test" {
		t.Fatalf("expected name=test, got %v", dataMap["name"])
	}
}

func TestQuery_SelectArray(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE posts (tags text[])")
	setupTable(t, p, "INSERT INTO posts VALUES (ARRAY['go','postgres','mcp'])")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT tags FROM posts"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	arr, ok := output.Rows[0]["tags"].([]interface{})
	if !ok {
		t.Fatalf("expected array, got %T", output.Rows[0]["tags"])
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
}

func TestQuery_SelectCTE(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO users (name) VALUES ('Alice'), ('Bob')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "WITH cte AS (SELECT * FROM users) SELECT name FROM cte ORDER BY name"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(output.Rows))
	}
}

func TestQuery_Timeout(t *testing.T) {
	config := defaultConfig()
	config.Query.DefaultTimeoutSeconds = 1
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(10)"})
	if output.Error == "" {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(output.Error, "context deadline exceeded") && !strings.Contains(output.Error, "canceling statement") {
		t.Fatalf("expected timeout error, got %q", output.Error)
	}
}

func TestQuery_ProtectionEndToEnd(t *testing.T) {
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "DROP TABLE users"})
	if output.Error == "" {
		t.Fatal("expected protection error")
	}
	if !strings.Contains(output.Error, "DROP statements are not allowed") {
		t.Fatalf("expected drop error, got %q", output.Error)
	}
}

func TestQuery_SanitizationEndToEnd(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Sanitization = []pgmcp.SanitizationRule{
		{Pattern: `\d{3}-\d{3}-\d{4}`, Replacement: "***-***-****"},
	}
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE contacts (phone text)")
	setupTable(t, p, "INSERT INTO contacts VALUES ('555-123-4567')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT phone FROM contacts"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	phone := output.Rows[0]["phone"].(string)
	if phone != "***-***-****" {
		t.Fatalf("expected sanitized phone, got %q", phone)
	}
}

func TestQuery_ErrorPromptEndToEnd(t *testing.T) {
	config := defaultConfig()
	config.ErrorPrompts = []pgmcp.ErrorPromptRule{
		{Pattern: "does not exist", Message: "The table you referenced does not exist. Try list_tables to see available tables."},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM nonexistent_table"})
	if output.Error == "" {
		t.Fatal("expected error")
	}
	if !strings.Contains(output.Error, "does not exist") {
		t.Fatalf("expected 'does not exist' error, got %q", output.Error)
	}
	if !strings.Contains(output.Error, "Try list_tables") {
		t.Fatalf("expected error prompt appended, got %q", output.Error)
	}
}

func TestQuery_MaxResultLength(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Query.MaxResultLength = 100 // very small limit
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE big_table (data text)")
	for i := 0; i < 20; i++ {
		setupTable(t, p, fmt.Sprintf("INSERT INTO big_table VALUES ('row number %d with some padding text here')", i))
	}

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM big_table"})
	if output.Error == "" {
		t.Fatal("expected truncation error")
	}
	if !strings.Contains(output.Error, "[truncated]") {
		t.Fatalf("expected truncation marker, got %q", output.Error)
	}
}

func TestQuery_ReadOnlyMode(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.ReadOnly = true
	p, _ := newTestInstance(t, config)

	// SELECT should work
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1 AS val"})
	if output.Error != "" {
		t.Fatalf("SELECT should work in read-only mode: %s", output.Error)
	}

	// INSERT should fail
	output = p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE TABLE test (id int)"})
	if output.Error == "" {
		t.Fatal("expected error for CREATE in read-only mode")
	}
}

func TestQuery_ReadOnlyModeBlocksSetBypass(t *testing.T) {
	config := defaultConfig()
	config.ReadOnly = true
	config.Protection.AllowSet = true // allow SET in general
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SET default_transaction_read_only = off"})
	if output.Error == "" {
		t.Fatal("expected error for SET default_transaction_read_only in read-only mode")
	}
	if !strings.Contains(output.Error, "default_transaction_read_only") {
		t.Fatalf("expected error about default_transaction_read_only, got %q", output.Error)
	}
}

func TestQuery_Timezone(t *testing.T) {
	config := defaultConfig()
	config.Timezone = "America/New_York"
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT current_setting('timezone') AS tz"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	tz := output.Rows[0]["tz"].(string)
	if tz != "America/New_York" {
		t.Fatalf("expected America/New_York, got %s", tz)
	}
}

func TestQuery_TimezoneUTC(t *testing.T) {
	config := defaultConfig()
	config.Timezone = "UTC"
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT current_setting('timezone') AS tz"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	tz := output.Rows[0]["tz"].(string)
	if tz != "UTC" {
		t.Fatalf("expected UTC, got %s", tz)
	}
}

func TestQuery_MaxSQLLength(t *testing.T) {
	config := defaultConfig()
	config.Query.MaxSQLLength = 100
	p, _ := newTestInstance(t, config)

	longSQL := "SELECT " + strings.Repeat("1,", 100) + "1"
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: longSQL})
	if output.Error == "" {
		t.Fatal("expected SQL length error")
	}
	if !strings.Contains(output.Error, "SQL query too long") {
		t.Fatalf("expected SQL length error, got %q", output.Error)
	}
}

func TestQuery_DDLBlocked(t *testing.T) {
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE TABLE test (id int)"})
	if output.Error == "" {
		t.Fatal("expected DDL blocked error")
	}
	if !strings.Contains(output.Error, "CREATE TABLE is not allowed") {
		t.Fatalf("expected DDL error, got %q", output.Error)
	}
}

func TestQuery_DDLAllowed(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE TABLE test (id int)"})
	if output.Error != "" {
		t.Fatalf("expected DDL to succeed when allowed: %s", output.Error)
	}
}

func TestQuery_TransactionControlBlocked(t *testing.T) {
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	for _, sql := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
		output := p.Query(context.Background(), pgmcp.QueryInput{SQL: sql})
		if output.Error == "" {
			t.Fatalf("expected error for %s", sql)
		}
		if !strings.Contains(output.Error, "transaction control statements are not allowed") {
			t.Fatalf("expected transaction control error for %s, got %q", sql, output.Error)
		}
	}
}

func TestQuery_RowsAffected_Insert(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "INSERT INTO users (name) VALUES ('a'), ('b'), ('c')"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.RowsAffected != 3 {
		t.Fatalf("expected RowsAffected=3, got %d", output.RowsAffected)
	}
}

func TestQuery_RowsAffected_Update(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, active boolean DEFAULT false)")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "UPDATE users SET active = true WHERE id <= 3"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.RowsAffected != 3 {
		t.Fatalf("expected RowsAffected=3, got %d", output.RowsAffected)
	}
}

func TestQuery_RowsAffected_Delete(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY)")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")
	setupTable(t, p, "INSERT INTO users DEFAULT VALUES")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "DELETE FROM users WHERE id <= 2"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.RowsAffected != 2 {
		t.Fatalf("expected RowsAffected=2, got %d", output.RowsAffected)
	}
}

func TestQuery_SemaphoreContention(t *testing.T) {
	config := defaultConfig()
	config.Pool.MaxConns = 1
	config.Query.DefaultTimeoutSeconds = 30
	p, _ := newTestInstance(t, config)

	// Start a slow query in a goroutine to hold the semaphore
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(5)"})
	}()

	// Give the goroutine time to acquire the semaphore
	time.Sleep(100 * time.Millisecond)

	// Try to run another query with a short context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected semaphore contention error")
	}
	if !strings.Contains(output.Error, "failed to acquire query slot") {
		t.Fatalf("expected semaphore error, got %q", output.Error)
	}

	<-done
}

func TestQuery_AfterHookRejectRollbacksWrite(t *testing.T) {
	// Create table with a non-hooked instance first
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupP.Close(context.Background())

	// Now create a hooked instance using the same connection string
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger(), pgmcp.WithServerHooks(pgmcp.ServerHooksConfig{
		AfterQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("reject.sh")},
		},
	}))
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "INSERT INTO users (name) VALUES ('rejected_row') RETURNING *"})
	if output.Error == "" {
		t.Fatal("expected hook rejection error")
	}
	if !strings.Contains(output.Error, "rejected by test hook") {
		t.Fatalf("expected rejection message, got %q", output.Error)
	}

	// Verify the row was NOT inserted (rollback happened)
	// Use a non-hooked instance to verify
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users WHERE name = 'rejected_row'"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	cnt := verifyOutput.Rows[0]["cnt"]
	if cnt != int64(0) {
		t.Fatalf("expected 0 rows (rollback), got %v (%T)", cnt, cnt)
	}
}

func TestQuery_AfterHookAcceptCommitsWrite(t *testing.T) {
	// Create table with a non-hooked instance first
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupP.Close(context.Background())

	// Create hooked instance
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger(), pgmcp.WithServerHooks(pgmcp.ServerHooksConfig{
		AfterQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
	}))
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "INSERT INTO users (name) VALUES ('accepted_row') RETURNING *"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Verify the row WAS inserted (commit happened) using non-hooked instance
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users WHERE name = 'accepted_row'"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	cnt := verifyOutput.Rows[0]["cnt"]
	if cnt != int64(1) {
		t.Fatalf("expected 1 row (committed), got %v (%T)", cnt, cnt)
	}
}

func TestQuery_HookCrashStopsPipeline(t *testing.T) {
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5

	p := newTestInstanceWithHooks(t, config, pgmcp.ServerHooksConfig{
		BeforeQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("crash.sh")},
		},
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected hook crash error")
	}
	if !strings.Contains(output.Error, "hook failed") {
		t.Fatalf("expected hook failed error, got %q", output.Error)
	}
}

func TestQuery_HookBadJsonStopsPipeline(t *testing.T) {
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5

	p := newTestInstanceWithHooks(t, config, pgmcp.ServerHooksConfig{
		BeforeQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("bad_json.sh")},
		},
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected unparseable response error")
	}
	if !strings.Contains(output.Error, "unparseable response") {
		t.Fatalf("expected unparseable response error, got %q", output.Error)
	}
}

func TestQuery_ExplainAnalyzeProtection(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN ANALYZE DELETE FROM users"})
	if output.Error == "" {
		t.Fatal("expected protection error for EXPLAIN ANALYZE DELETE")
	}
	if !strings.Contains(output.Error, "DELETE without WHERE") {
		t.Fatalf("expected delete protection error, got %q", output.Error)
	}
}

func TestQuery_UTF8Truncation(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Query.MaxResultLength = 50 // very small
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE emoji_table (data text)")
	setupTable(t, p, "INSERT INTO emoji_table VALUES ('Hello world! Here are some special chars: cafe\u0301 naïve résumé')")
	setupTable(t, p, "INSERT INTO emoji_table VALUES ('More text to ensure we exceed the limit easily here')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM emoji_table"})
	if output.Error == "" {
		t.Fatal("expected truncation")
	}
	// Verify the truncated output is valid UTF-8
	idx := strings.Index(output.Error, "...[truncated]")
	if idx == -1 {
		t.Fatalf("expected truncation marker, got %q", output.Error)
	}
	truncatedPart := output.Error[:idx]
	if !utf8.ValidString(truncatedPart) {
		t.Fatalf("truncated output is not valid UTF-8: %q", truncatedPart)
	}
}

func TestQuery_TimeoutRuleMatch(t *testing.T) {
	config := defaultConfig()
	config.Query.DefaultTimeoutSeconds = 30
	config.Query.TimeoutRules = []pgmcp.TimeoutRule{
		{Pattern: "pg_sleep", TimeoutSeconds: 1},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(10)"})
	if output.Error == "" {
		t.Fatal("expected timeout from rule")
	}
	// Should have timed out after ~1s, not 30s
}

func TestQuery_InetColumn(t *testing.T) {
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE servers (ip inet)")
	setupTable(t, p, "INSERT INTO servers VALUES ('192.168.1.1/24')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT ip FROM servers"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	ipStr, ok := output.Rows[0]["ip"].(string)
	if !ok {
		t.Fatalf("expected string for inet, got %T", output.Rows[0]["ip"])
	}
	if !strings.Contains(ipStr, "192.168.1.1") {
		t.Fatalf("expected 192.168.1.1 in inet, got %s", ipStr)
	}
}
