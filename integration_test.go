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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestQuery_MultipleErrorPromptsConcat(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.ErrorPrompts = []pgmcp.ErrorPromptRule{
		{Pattern: "does not exist", Message: "Hint 1: Try list_tables."},
		{Pattern: "relation", Message: "Hint 2: Check the table name spelling."},
	}
	p, _ := newTestInstance(t, config)

	// This error message contains both "does not exist" and "relation"
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM nonexistent_table"})
	if output.Error == "" {
		t.Fatal("expected error")
	}
	if !strings.Contains(output.Error, "Hint 1: Try list_tables.") {
		t.Fatalf("expected first error prompt, got %q", output.Error)
	}
	if !strings.Contains(output.Error, "Hint 2: Check the table name spelling.") {
		t.Fatalf("expected second error prompt, got %q", output.Error)
	}
}

func TestQuery_MaxResultLength(t *testing.T) {
	t.Parallel()
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
	if output.Rows != nil {
		t.Fatalf("expected Rows to be nil after truncation, got %v", output.Rows)
	}
	if !strings.HasPrefix(output.Error, "[") {
		t.Fatalf("expected Error to start with '[' (partial JSON array), got %q", output.Error)
	}
}

func TestQuery_ReadOnlyMode(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestQuery_ReadOnlyBlocksResetAll(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.ReadOnly = true
	config.Protection.AllowSet = true
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "RESET ALL"})
	if output.Error == "" {
		t.Fatal("expected error for RESET ALL in read-only mode")
	}
	if !strings.Contains(output.Error, "RESET ALL is blocked in read-only mode") {
		t.Fatalf("expected RESET ALL blocked message, got %q", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksResetTransactionReadOnly(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.ReadOnly = true
	config.Protection.AllowSet = true
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "RESET default_transaction_read_only"})
	if output.Error == "" {
		t.Fatal("expected error for RESET default_transaction_read_only in read-only mode")
	}
	if !strings.Contains(output.Error, "RESET default_transaction_read_only is blocked in read-only mode") {
		t.Fatalf("expected RESET default_transaction_read_only blocked message, got %q", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksBeginReadWrite(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.ReadOnly = true
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "BEGIN READ WRITE"})
	if output.Error == "" {
		t.Fatal("expected error for BEGIN READ WRITE in read-only mode")
	}
	if !strings.Contains(output.Error, "BEGIN READ WRITE is blocked in read-only mode") {
		t.Fatalf("expected BEGIN READ WRITE blocked message, got %q", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksStartTransactionReadWrite(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.ReadOnly = true
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "START TRANSACTION READ WRITE"})
	if output.Error == "" {
		t.Fatal("expected error for START TRANSACTION READ WRITE in read-only mode")
	}
	if !strings.Contains(output.Error, "BEGIN READ WRITE is blocked in read-only mode") {
		t.Fatalf("expected BEGIN READ WRITE blocked message, got %q", output.Error)
	}
}

// --- Read-Only Mode: Direct DML Blocked ---

func TestQuery_ReadOnlyBlocksInsert(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_insert (id serial PRIMARY KEY, name text)")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "INSERT INTO ro_insert (name) VALUES ('test')"})
	if output.Error == "" {
		t.Fatal("expected error for INSERT in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksUpdate(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_update (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_update (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "UPDATE ro_update SET name = 'Bob' WHERE id = 1"})
	if output.Error == "" {
		t.Fatal("expected error for UPDATE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksDelete(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_delete (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_delete (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "DELETE FROM ro_delete WHERE id = 1"})
	if output.Error == "" {
		t.Fatal("expected error for DELETE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksMerge(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowMerge = true
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_merge_target (id int PRIMARY KEY, name text)")
		setupTable(t, p, "CREATE TABLE ro_merge_source (id int PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_merge_target (id, name) VALUES (1, 'Alice')")
		setupTable(t, p, "INSERT INTO ro_merge_source (id, name) VALUES (1, 'Bob')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "MERGE INTO ro_merge_target t USING ro_merge_source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
	})
	if output.Error == "" {
		t.Fatal("expected error for MERGE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

// --- Read-Only Mode: DML Inside CTEs Blocked ---

func TestQuery_ReadOnlyBlocksCTEInsert(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_cte_insert (id serial PRIMARY KEY, name text)")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "WITH ins AS (INSERT INTO ro_cte_insert (name) VALUES ('test') RETURNING *) SELECT * FROM ins",
	})
	if output.Error == "" {
		t.Fatal("expected error for INSERT inside CTE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksCTEUpdate(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_cte_update (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_cte_update (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "WITH upd AS (UPDATE ro_cte_update SET name = 'Bob' WHERE id = 1 RETURNING *) SELECT * FROM upd",
	})
	if output.Error == "" {
		t.Fatal("expected error for UPDATE inside CTE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksCTEDelete(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_cte_delete (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_cte_delete (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "WITH del AS (DELETE FROM ro_cte_delete WHERE id = 1 RETURNING *) SELECT * FROM del",
	})
	if output.Error == "" {
		t.Fatal("expected error for DELETE inside CTE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksCTEMerge(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowMerge = true
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_cte_merge_target (id int PRIMARY KEY, name text)")
		setupTable(t, p, "CREATE TABLE ro_cte_merge_source (id int PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_cte_merge_target (id, name) VALUES (1, 'Alice')")
		setupTable(t, p, "INSERT INTO ro_cte_merge_source (id, name) VALUES (1, 'Bob')")
	})

	// PostgreSQL's parser accepts MERGE in CTEs (pg_query_go parses it), but the
	// execution engine rejects it: "MERGE not supported in WITH query".
	// This error occurs before the read-only check, so we verify it's blocked regardless.
	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "WITH m AS (MERGE INTO ro_cte_merge_target t USING ro_cte_merge_source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name) SELECT 1",
	})
	if output.Error == "" {
		t.Fatal("expected error for MERGE inside CTE in read-only mode")
	}
	if !strings.Contains(output.Error, "MERGE not supported in WITH query") {
		t.Fatalf("expected 'MERGE not supported in WITH query' error, got: %s", output.Error)
	}
}

// --- Read-Only Mode: EXPLAIN (no ANALYZE) Allows DML Planning ---

func TestQuery_ReadOnlyAllowsExplainInsert(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_explain_insert (id serial PRIMARY KEY, name text)")
	})

	// EXPLAIN without ANALYZE only plans — doesn't execute DML
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN INSERT INTO ro_explain_insert (name) VALUES ('test')"})
	if output.Error != "" {
		t.Fatalf("EXPLAIN INSERT should work in read-only mode (planning only): %s", output.Error)
	}
}

func TestQuery_ReadOnlyAllowsExplainUpdate(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_explain_update (id serial PRIMARY KEY, name text)")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN UPDATE ro_explain_update SET name = 'Bob' WHERE id = 1"})
	if output.Error != "" {
		t.Fatalf("EXPLAIN UPDATE should work in read-only mode (planning only): %s", output.Error)
	}
}

func TestQuery_ReadOnlyAllowsExplainDelete(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_explain_delete (id serial PRIMARY KEY, name text)")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN DELETE FROM ro_explain_delete WHERE id = 1"})
	if output.Error != "" {
		t.Fatalf("EXPLAIN DELETE should work in read-only mode (planning only): %s", output.Error)
	}
}

func TestQuery_ReadOnlyAllowsExplainMerge(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowMerge = true
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_explain_merge_t (id int PRIMARY KEY, name text)")
		setupTable(t, p, "CREATE TABLE ro_explain_merge_s (id int PRIMARY KEY, name text)")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "EXPLAIN MERGE INTO ro_explain_merge_t t USING ro_explain_merge_s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
	})
	if output.Error != "" {
		t.Fatalf("EXPLAIN MERGE should work in read-only mode (planning only): %s", output.Error)
	}
}

// --- Read-Only Mode: EXPLAIN ANALYZE Blocks DML (actually executes) ---

func TestQuery_ReadOnlyBlocksExplainAnalyzeInsert(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_ea_insert (id serial PRIMARY KEY, name text)")
	})

	// EXPLAIN ANALYZE actually executes the DML — should fail in read-only
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN ANALYZE INSERT INTO ro_ea_insert (name) VALUES ('test')"})
	if output.Error == "" {
		t.Fatal("expected error for EXPLAIN ANALYZE INSERT in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksExplainAnalyzeUpdate(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_ea_update (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_ea_update (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN ANALYZE UPDATE ro_ea_update SET name = 'Bob' WHERE id = 1"})
	if output.Error == "" {
		t.Fatal("expected error for EXPLAIN ANALYZE UPDATE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksExplainAnalyzeDelete(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_ea_delete (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_ea_delete (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN ANALYZE DELETE FROM ro_ea_delete WHERE id = 1"})
	if output.Error == "" {
		t.Fatal("expected error for EXPLAIN ANALYZE DELETE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

func TestQuery_ReadOnlyBlocksExplainAnalyzeMerge(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowMerge = true
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_ea_merge_t (id int PRIMARY KEY, name text)")
		setupTable(t, p, "CREATE TABLE ro_ea_merge_s (id int PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_ea_merge_t (id, name) VALUES (1, 'Alice')")
		setupTable(t, p, "INSERT INTO ro_ea_merge_s (id, name) VALUES (1, 'Bob')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{
		SQL: "EXPLAIN ANALYZE MERGE INTO ro_ea_merge_t t USING ro_ea_merge_s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
	})
	if output.Error == "" {
		t.Fatal("expected error for EXPLAIN ANALYZE MERGE in read-only mode")
	}
	if !strings.Contains(output.Error, "read-only transaction") {
		t.Fatalf("expected read-only transaction error, got: %s", output.Error)
	}
}

// --- Read-Only Mode: EXPLAIN ANALYZE SELECT Works ---

func TestQuery_ReadOnlyAllowsExplainAnalyzeSelect(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_ea_select (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_ea_select (name) VALUES ('Alice')")
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "EXPLAIN ANALYZE SELECT * FROM ro_ea_select"})
	if output.Error != "" {
		t.Fatalf("EXPLAIN ANALYZE SELECT should work in read-only mode: %s", output.Error)
	}
}

// --- Read-Only Mode: SELECT Confirms No Side Effects ---

func TestQuery_ReadOnlySelectVerifiesData(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p := newReadOnlyTestInstance(t, config, func(t *testing.T, p *pgmcp.PostgresMcp) {
		setupTable(t, p, "CREATE TABLE ro_verify (id serial PRIMARY KEY, name text)")
		setupTable(t, p, "INSERT INTO ro_verify (name) VALUES ('Alice'), ('Bob')")
	})

	// SELECT should return existing data
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT name FROM ro_verify ORDER BY id"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
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

func TestQuery_Timezone(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE TABLE test (id int)"})
	if output.Error != "" {
		t.Fatalf("expected DDL to succeed when allowed: %s", output.Error)
	}
}

func TestQuery_TransactionControlBlocked(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestQuery_TimeoutFallbackToDefault(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Query.DefaultTimeoutSeconds = 1 // short default
	config.Query.TimeoutRules = []pgmcp.TimeoutRule{
		{Pattern: "NEVER_MATCH_THIS_PATTERN", TimeoutSeconds: 30},
	}
	p, _ := newTestInstance(t, config)

	// This query won't match the rule, so it should use default (1s) and timeout
	start := time.Now()
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(10)"})
	elapsed := time.Since(start)

	if output.Error == "" {
		t.Fatal("expected timeout from default timeout")
	}
	// Should have timed out after ~1s (default), not 30s (rule)
	if elapsed > 5*time.Second {
		t.Fatalf("expected timeout near 1s (default), but took %v", elapsed)
	}
}

func TestQuery_InetColumn(t *testing.T) {
	t.Parallel()
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

// --- Section 4: Missing Query Integration Tests ---

func TestQuery_JSONBReturnType(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE items (data jsonb)")
	setupTable(t, p, `INSERT INTO items VALUES ('{"name":"test","nested":{"arr":[1,2,3]},"flag":true,"nothing":null,"count":42}')`)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT data FROM items"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}

	// Verify JSONB is returned as parsed Go map, not string
	dataMap, ok := output.Rows[0]["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected map[string]interface{} for JSONB, got %T: %v", output.Rows[0]["data"], output.Rows[0]["data"])
	}

	// Verify nested object
	nested, ok := dataMap["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested map, got %T", dataMap["nested"])
	}

	// Verify nested array
	arr, ok := nested["arr"].([]interface{})
	if !ok {
		t.Fatalf("expected nested array, got %T", nested["arr"])
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements in array, got %d", len(arr))
	}

	// Verify boolean
	flag, ok := dataMap["flag"].(bool)
	if !ok {
		t.Fatalf("expected bool for flag, got %T", dataMap["flag"])
	}
	if flag != true {
		t.Fatalf("expected true, got %v", flag)
	}

	// Verify null
	if dataMap["nothing"] != nil {
		t.Fatalf("expected nil for nothing, got %v (%T)", dataMap["nothing"], dataMap["nothing"])
	}

	// Verify string
	if dataMap["name"] != "test" {
		t.Fatalf("expected name=test, got %v", dataMap["name"])
	}
}

func TestQuery_JSONBNumericPrecision(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE items (data jsonb)")
	setupTable(t, p, `INSERT INTO items VALUES ('{"id": 9007199254740993}')`) // 2^53+1

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT data FROM items"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Known limitation: large integer inside JSONB loses precision to float64
	// because pgx parses JSONB internally and numbers become float64.
	dataMap, ok := output.Rows[0]["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected map for JSONB, got %T", output.Rows[0]["data"])
	}

	// The value should be a float64 (precision lost — pgx limitation)
	val, ok := dataMap["id"].(float64)
	if !ok {
		t.Fatalf("expected float64 for JSONB large int (pgx limitation), got %T: %v", dataMap["id"], dataMap["id"])
	}
	// 9007199254740993 → 9007199254740992 (precision lost)
	if val != 9.007199254740992e+15 {
		t.Fatalf("expected 9.007199254740992e+15 (precision loss), got %v", val)
	}
}

func TestQuery_SelectNestedSubquery(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE departments (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE TABLE employees (id serial PRIMARY KEY, name text, dept_id int)")
	setupTable(t, p, "INSERT INTO departments (name) VALUES ('Engineering'), ('Sales')")
	setupTable(t, p, "INSERT INTO employees (name, dept_id) VALUES ('Alice', 1), ('Bob', 1), ('Charlie', 2)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: `
		SELECT name FROM employees
		WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering')
		ORDER BY name
	`})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
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

func TestQuery_Transaction(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, connStr := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE tx_test (id serial PRIMARY KEY, val int)")
	setupTable(t, p, "INSERT INTO tx_test (val) VALUES (1), (2), (3)")

	// INSERT with RETURNING should commit (data persisted)
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "INSERT INTO tx_test (val) VALUES (42) RETURNING val"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.Rows[0]["val"] != int32(42) {
		t.Fatalf("expected 42, got %v", output.Rows[0]["val"])
	}

	// Verify data persisted by creating a fresh instance and querying
	ctx := context.Background()
	verifyP, err := pgmcp.New(ctx, connStr, defaultConfig(), testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM tx_test WHERE val = 42"})
	if verifyOutput.Error != "" {
		t.Fatalf("verify query failed: %s", verifyOutput.Error)
	}
	if verifyOutput.Rows[0]["cnt"] != int64(1) {
		t.Fatalf("expected 1 row persisted, got %v", verifyOutput.Rows[0]["cnt"])
	}
}

func TestQuery_HooksEndToEnd(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.DefaultHookTimeoutSeconds = 5

	p := newTestInstanceWithHooks(t, config, pgmcp.ServerHooksConfig{
		BeforeQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("modify_query.sh")},
		},
	})

	// modify_query.sh changes any query to "SELECT 1 AS modified"
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 999"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	if len(output.Columns) != 1 || output.Columns[0] != "modified" {
		t.Fatalf("expected column 'modified', got %v", output.Columns)
	}
	val, ok := output.Rows[0]["modified"].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T: %v", output.Rows[0]["modified"], output.Rows[0]["modified"])
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestQuery_HookTimeoutStopsPipeline(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 1

	p := newTestInstanceWithHooks(t, config, pgmcp.ServerHooksConfig{
		BeforeQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: hookScript("slow.sh")},
		},
	})

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected hook timeout error")
	}
	if !strings.Contains(output.Error, "hook timed out") {
		t.Fatalf("expected 'hook timed out' in error, got %q", output.Error)
	}
}

func TestQuery_TimezoneEmpty(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Timezone = "" // empty = server default
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT current_setting('timezone') AS tz"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	tz, ok := output.Rows[0]["tz"].(string)
	if !ok {
		t.Fatalf("expected string for timezone, got %T", output.Rows[0]["tz"])
	}
	// Server default timezone should be non-empty (not overridden)
	if tz == "" {
		t.Fatal("expected non-empty timezone from server default")
	}
}

func TestQuery_TimezoneWithReadOnly(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Timezone = "Asia/Jakarta"
	config.ReadOnly = true
	p, _ := newTestInstance(t, config)

	// Verify timezone is applied
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT current_setting('timezone') AS tz"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	tz := output.Rows[0]["tz"].(string)
	if tz != "Asia/Jakarta" {
		t.Fatalf("expected Asia/Jakarta, got %s", tz)
	}

	// Verify read_only is also applied (CREATE should fail)
	output = p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE TABLE should_fail (id int)"})
	if output.Error == "" {
		t.Fatal("expected error for CREATE in read-only mode")
	}
}

func TestQuery_NumericPrecisionWithHooks(t *testing.T) {
	t.Parallel()
	// Setup: create table with bigint 2^53+1
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE bigint_cmd_hook (big_id bigint)")
	setupTable(t, setupP, "INSERT INTO bigint_cmd_hook VALUES (9007199254740993)") // 2^53+1
	setupP.Close(context.Background())

	// Create instance with accept.sh AfterQuery hook (triggers JSON round-trip)
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

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT big_id FROM bigint_cmd_hook"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// After JSON round-trip with UseNumber(), the value survives as json.Number
	val := output.Rows[0]["big_id"]
	numVal, ok := val.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number after cmd hook round-trip, got %T: %v", val, val)
	}
	// Verify exact integer preserved via UseNumber()
	int64Val, err := numVal.Int64()
	if err != nil {
		t.Fatalf("failed to convert json.Number to int64: %v", err)
	}
	if int64Val != 9007199254740993 {
		t.Fatalf("expected 9007199254740993, got %d", int64Val)
	}
}

func TestQuery_NumericPrecisionWithoutHooks(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE bigint_nohook (big_id bigint)")
	setupTable(t, p, "INSERT INTO bigint_nohook VALUES (9007199254740993)") // 2^53+1

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT big_id FROM bigint_nohook"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Without hooks, no JSON round-trip, value stays as int64
	val := output.Rows[0]["big_id"]
	int64Val, ok := val.(int64)
	if !ok {
		t.Fatalf("expected int64 without hooks, got %T: %v", val, val)
	}
	if int64Val != 9007199254740993 {
		t.Fatalf("expected 9007199254740993, got %d", int64Val)
	}
}

func TestQuery_RowsAffected_Select(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users_ra (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO users_ra (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM users_ra"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(output.Rows))
	}
	if output.RowsAffected != 5 {
		t.Fatalf("expected RowsAffected=5, got %d", output.RowsAffected)
	}
}

func TestQuery_RowsAffected_InsertReturning(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users_ra2 (id serial PRIMARY KEY, name text)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "INSERT INTO users_ra2 (name) VALUES ('a') RETURNING *"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if output.RowsAffected != 1 {
		t.Fatalf("expected RowsAffected=1, got %d", output.RowsAffected)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	if output.Rows[0]["name"] != "a" {
		t.Fatalf("expected name='a', got %v", output.Rows[0]["name"])
	}
}

func TestQuery_CidrColumn(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE subnets (network cidr)")
	setupTable(t, p, "INSERT INTO subnets VALUES ('10.0.0.0/8')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT network FROM subnets"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	netStr, ok := output.Rows[0]["network"].(string)
	if !ok {
		t.Fatalf("expected string for cidr, got %T", output.Rows[0]["network"])
	}
	if netStr != "10.0.0.0/8" {
		t.Fatalf("expected 10.0.0.0/8, got %s", netStr)
	}
}

func TestQuery_AfterHookRejectSelectNoSideEffect(t *testing.T) {
	t.Parallel()
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users_select_reject (id serial PRIMARY KEY, name text)")
	setupTable(t, setupP, "INSERT INTO users_select_reject (name) VALUES ('existing')")
	setupP.Close(context.Background())

	// Create instance with rejecting AfterQuery hook
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

	// SELECT is read-only — hook rejects but no side effect
	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT * FROM users_select_reject"})
	if output.Error == "" {
		t.Fatal("expected hook rejection error for SELECT")
	}
	if !strings.Contains(output.Error, "rejected by test hook") {
		t.Fatalf("expected rejection message, got %q", output.Error)
	}

	// Verify data unchanged — use non-hooked instance
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users_select_reject"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	if verifyOutput.Rows[0]["cnt"] != int64(1) {
		t.Fatalf("expected 1 row unchanged, got %v", verifyOutput.Rows[0]["cnt"])
	}
}

func TestQuery_ReadOnlyStatementRollbacksBeforeHooks(t *testing.T) {
	t.Parallel()
	// This test verifies that SELECT causes rollback before AfterQuery hooks run.
	// The hook still runs, but the transaction is already rolled back.
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.DefaultHookTimeoutSeconds = 5

	// Use a capture hook to verify it receives data (hook is called even though tx is rolled back)
	captureHook := &captureAfterHook{}
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "capture", Hook: captureHook},
	}
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE readonly_hook_test (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO readonly_hook_test (name) VALUES ('Alice')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM readonly_hook_test"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Hook was called (it captured the result)
	if captureHook.captured == nil {
		t.Fatal("expected AfterQuery hook to be called for SELECT")
	}
	if len(captureHook.captured.Rows) != 1 {
		t.Fatalf("expected hook to receive 1 row, got %d", len(captureHook.captured.Rows))
	}
	if captureHook.captured.Rows[0]["name"] != "Alice" {
		t.Fatalf("expected hook to receive 'Alice', got %v", captureHook.captured.Rows[0]["name"])
	}
}

func TestQuery_MaxSQLLength_ExactLimit(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Query.MaxSQLLength = 20
	p, _ := newTestInstance(t, config)

	// "SELECT 1" is 8 bytes, well under 20
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error != "" {
		t.Fatalf("expected query under limit to succeed, got error: %s", output.Error)
	}
}

func TestQuery_CreateExtensionBlocked(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	// AllowCreateExtension defaults to false
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE EXTENSION IF NOT EXISTS pg_trgm"})
	if output.Error == "" {
		t.Fatal("expected CREATE EXTENSION to be blocked")
	}
	if !strings.Contains(output.Error, "CREATE EXTENSION is not allowed") {
		t.Fatalf("expected CREATE EXTENSION error, got %q", output.Error)
	}
}

func TestQuery_MaintenanceBlocked(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE maint_test (id serial PRIMARY KEY)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "ANALYZE maint_test"})
	if output.Error == "" {
		t.Fatal("expected ANALYZE to be blocked")
	}
	if !strings.Contains(output.Error, "VACUUM/ANALYZE is not allowed") {
		t.Fatalf("expected maintenance error, got %q", output.Error)
	}
}

func TestQuery_CreateTriggerBlocked(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowCreateFunction = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE trigger_test (id serial PRIMARY KEY)")
	setupTable(t, p, "CREATE FUNCTION audit_func() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE TRIGGER trg AFTER INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION audit_func()"})
	if output.Error == "" {
		t.Fatal("expected CREATE TRIGGER to be blocked")
	}
	if !strings.Contains(output.Error, "CREATE TRIGGER is not allowed") {
		t.Fatalf("expected trigger error, got %q", output.Error)
	}
}

func TestQuery_CreateRuleBlocked(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE rule_test (id serial PRIMARY KEY)")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE RULE r AS ON INSERT TO rule_test DO ALSO NOTIFY rule_test_changed"})
	if output.Error == "" {
		t.Fatal("expected CREATE RULE to be blocked")
	}
	if !strings.Contains(output.Error, "CREATE RULE is not allowed") {
		t.Fatalf("expected rule error, got %q", output.Error)
	}
}

func TestQuery_CommitBlocked(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "COMMIT"})
	if output.Error == "" {
		t.Fatal("expected COMMIT to be blocked")
	}
	if !strings.Contains(output.Error, "transaction control statements are not allowed") {
		t.Fatalf("expected transaction control error, got %q", output.Error)
	}
}

func TestQuery_AlterExtensionBlocked(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "ALTER EXTENSION pg_trgm UPDATE"})
	if output.Error == "" {
		t.Fatal("expected ALTER EXTENSION to be blocked")
	}
	if !strings.Contains(output.Error, "ALTER EXTENSION is not allowed") {
		t.Fatalf("expected ALTER EXTENSION error, got %q", output.Error)
	}
}

// --- Full Pipeline Test ---

// pipelineBeforeHook rewrites any query to SELECT id, name, phone FROM pipeline_test ORDER BY id.
type pipelineBeforeHook struct{}

func (h *pipelineBeforeHook) Run(_ context.Context, _ string) (string, error) {
	return "SELECT id, name, phone FROM pipeline_test ORDER BY id", nil
}

// pipelineAfterHook adds a "hook_stage" column to every row.
type pipelineAfterHook struct{}

func (h *pipelineAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	result.Columns = append(result.Columns, "hook_stage")
	for _, row := range result.Rows {
		row["hook_stage"] = "after_hook_applied"
	}
	return result, nil
}

func TestFullPipeline(t *testing.T) {
	t.Parallel()

	// First, create a plain instance (no hooks) to set up the test table.
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)

	setupTable(t, setupP, "CREATE TABLE pipeline_test (id serial PRIMARY KEY, name text, phone text)")
	setupTable(t, setupP, "INSERT INTO pipeline_test (name, phone) VALUES ('Alice', '555-123-4567'), ('Bob', '555-987-6543')")

	// Now create the full pipeline instance with hooks, sanitization, and error prompts
	// on the same database.
	pipelineConfig := defaultConfig()
	pipelineConfig.DefaultHookTimeoutSeconds = 10
	pipelineConfig.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "rewrite", Hook: &pipelineBeforeHook{}},
	}
	pipelineConfig.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "add_column", Hook: &pipelineAfterHook{}},
	}
	pipelineConfig.Sanitization = []pgmcp.SanitizationRule{
		{
			Pattern:     `\d{3}-\d{3}-\d{4}`,
			Replacement: "***-***-****",
			Description: "mask phone numbers",
		},
	}
	pipelineConfig.ErrorPrompts = []pgmcp.ErrorPromptRule{
		{
			Pattern: "does not exist",
			Message: "The table may not exist. Try running ListTables first.",
		},
	}

	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, pipelineConfig, testLogger())
	if err != nil {
		t.Fatalf("failed to create pipeline instance: %v", err)
	}
	defer p.Close(ctx)

	// --- Test 1: Successful query through full pipeline ---
	// Send a dummy query — BeforeQuery hook rewrites it to SELECT from pipeline_test.
	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Verify AfterQuery hook added the "hook_stage" column
	if len(output.Columns) != 4 {
		t.Fatalf("expected 4 columns (id, name, phone, hook_stage), got %d: %v", len(output.Columns), output.Columns)
	}
	foundHookStage := false
	for _, col := range output.Columns {
		if col == "hook_stage" {
			foundHookStage = true
			break
		}
	}
	if !foundHookStage {
		t.Fatalf("expected 'hook_stage' column from AfterQuery hook, got columns: %v", output.Columns)
	}

	// Verify we got 2 rows
	if len(output.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(output.Rows))
	}

	// Verify BeforeQuery hook rewrote the query (we get pipeline_test data, not "SELECT 1")
	if output.Rows[0]["name"] != "Alice" {
		t.Fatalf("expected 'Alice' in first row, got %v", output.Rows[0]["name"])
	}
	if output.Rows[1]["name"] != "Bob" {
		t.Fatalf("expected 'Bob' in second row, got %v", output.Rows[1]["name"])
	}

	// Verify sanitization masked phone numbers
	phone0, ok := output.Rows[0]["phone"].(string)
	if !ok {
		t.Fatalf("expected phone to be string, got %T", output.Rows[0]["phone"])
	}
	if phone0 != "***-***-****" {
		t.Fatalf("expected phone masked as '***-***-****', got %q", phone0)
	}
	phone1, ok := output.Rows[1]["phone"].(string)
	if !ok {
		t.Fatalf("expected phone to be string, got %T", output.Rows[1]["phone"])
	}
	if phone1 != "***-***-****" {
		t.Fatalf("expected phone masked as '***-***-****', got %q", phone1)
	}

	// Verify AfterQuery hook applied to rows
	for i, row := range output.Rows {
		if row["hook_stage"] != "after_hook_applied" {
			t.Fatalf("row %d: expected hook_stage='after_hook_applied', got %v", i, row["hook_stage"])
		}
	}

	// --- Test 2: Error prompts applied on failure ---
	// Use the setup instance (no hooks) with error prompts to test error prompt matching.
	errPromptConfig := defaultConfig()
	errPromptConfig.ErrorPrompts = []pgmcp.ErrorPromptRule{
		{
			Pattern: "does not exist",
			Message: "The table may not exist. Try running ListTables first.",
		},
	}
	pErrPrompt, err := pgmcp.New(ctx, connStr, errPromptConfig, testLogger())
	if err != nil {
		t.Fatalf("failed to create error prompt instance: %v", err)
	}
	defer pErrPrompt.Close(ctx)

	errOutput := pErrPrompt.Query(ctx, pgmcp.QueryInput{SQL: "SELECT * FROM nonexistent_table_xyz"})
	if errOutput.Error == "" {
		t.Fatal("expected error for nonexistent table")
	}
	if !strings.Contains(errOutput.Error, "does not exist") {
		t.Fatalf("expected 'does not exist' in error, got %q", errOutput.Error)
	}
	if !strings.Contains(errOutput.Error, "The table may not exist. Try running ListTables first.") {
		t.Fatalf("expected error prompt in error message, got %q", errOutput.Error)
	}
}

// --- Config Defaults Tests ---

func TestLoadConfigDefaults_MaxResultLength(t *testing.T) {
	t.Parallel()
	// Config with MaxResultLength omitted (0) — should default to 100000.
	config := defaultConfig()
	config.Query.MaxResultLength = 0
	p, _ := newTestInstance(t, config)

	// A simple query should succeed — the default (100000) is applied, not 0.
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 'hello' AS greeting"})
	if output.Error != "" {
		t.Fatalf("unexpected error with default max_result_length: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	if output.Rows[0]["greeting"] != "hello" {
		t.Fatalf("expected 'hello', got %v", output.Rows[0]["greeting"])
	}
}

func TestLoadConfigDefaults_MaxSQLLength(t *testing.T) {
	t.Parallel()
	// Config with MaxSQLLength omitted (0) — should default to 100000.
	config := defaultConfig()
	config.Query.MaxSQLLength = 0
	p, _ := newTestInstance(t, config)

	// A normal-length query should succeed — the default (100000) is applied, not 0.
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1 AS num"})
	if output.Error != "" {
		t.Fatalf("unexpected error with default max_sql_length: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
}

func TestClose_SubsequentOperationsFail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	connStr := acquireTestDB(t)
	config := defaultConfig()

	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("failed to create pgmcp instance: %v", err)
	}

	// Verify the instance works before closing.
	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT 1 AS num"})
	if output.Error != "" {
		t.Fatalf("unexpected error before close: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}

	// Close the instance.
	p.Close(ctx)

	// Query should return an error in output.Error (pool is closed).
	output = p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT 1 AS num"})
	if output.Error == "" {
		t.Fatalf("expected error after close, got none")
	}

	// ListTables should return a Go error.
	_, err = p.ListTables(ctx, pgmcp.ListTablesInput{})
	if err == nil {
		t.Fatalf("expected error from ListTables after close, got nil")
	}

	// DescribeTable should return a Go error.
	_, err = p.DescribeTable(ctx, pgmcp.DescribeTableInput{Table: "nonexistent"})
	if err == nil {
		t.Fatalf("expected error from DescribeTable after close, got nil")
	}
}
