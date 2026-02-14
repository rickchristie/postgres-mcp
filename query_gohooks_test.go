package pgmcp_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

// --- Go hook implementations for testing ---

// passthroughBeforeHook returns the query unchanged.
type passthroughBeforeHook struct{}

func (h *passthroughBeforeHook) Run(_ context.Context, query string) (string, error) {
	return query, nil
}

// rejectBeforeHook always returns an error.
type rejectBeforeHook struct{}

func (h *rejectBeforeHook) Run(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf("query not allowed by policy")
}

// modifyBeforeHook replaces the query with a fixed query.
type modifyBeforeHook struct {
	replacement string
}

func (h *modifyBeforeHook) Run(_ context.Context, _ string) (string, error) {
	return h.replacement, nil
}

// slowBeforeHook sleeps until context is cancelled or duration elapses.
type slowBeforeHook struct {
	sleepDuration time.Duration
}

func (h *slowBeforeHook) Run(ctx context.Context, query string) (string, error) {
	select {
	case <-time.After(h.sleepDuration):
		return query, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// trackingBeforeHook records whether it was called.
type trackingBeforeHook struct {
	called bool
}

func (h *trackingBeforeHook) Run(_ context.Context, query string) (string, error) {
	h.called = true
	return query, nil
}

// passthroughAfterHook returns the result unchanged.
type passthroughAfterHook struct{}

func (h *passthroughAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	return result, nil
}

// rejectAfterHook always returns an error.
type rejectAfterHook struct{}

func (h *rejectAfterHook) Run(_ context.Context, _ *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	return nil, fmt.Errorf("result rejected by audit hook")
}

// addColumnAfterHook adds a synthetic column to every row.
type addColumnAfterHook struct{}

func (h *addColumnAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	result.Columns = append(result.Columns, "hook_added")
	for _, row := range result.Rows {
		row["hook_added"] = "injected"
	}
	return result, nil
}

// slowAfterHook sleeps until context is cancelled or duration elapses.
type slowAfterHook struct {
	sleepDuration time.Duration
}

func (h *slowAfterHook) Run(ctx context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	select {
	case <-time.After(h.sleepDuration):
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// captureAfterHook captures the result for later inspection.
type captureAfterHook struct {
	captured *pgmcp.QueryOutput
}

func (h *captureAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	h.captured = result
	return result, nil
}

// --- Test cases ---

func TestQuery_GoBeforeHook_Accept(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "passthrough", Hook: &passthroughBeforeHook{}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1 AS val"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	val, ok := output.Rows[0]["val"].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T: %v", output.Rows[0]["val"], output.Rows[0]["val"])
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestQuery_GoBeforeHook_Reject(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "rejector", Hook: &rejectBeforeHook{}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected hook rejection error")
	}
	if !strings.Contains(output.Error, "rejector") {
		t.Fatalf("expected hook name 'rejector' in error, got %q", output.Error)
	}
	if !strings.Contains(output.Error, "query not allowed by policy") {
		t.Fatalf("expected rejection message in error, got %q", output.Error)
	}
}

func TestQuery_GoBeforeHook_ModifyQuery(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "modifier", Hook: &modifyBeforeHook{replacement: "SELECT 2 AS val"}},
	}
	p, _ := newTestInstance(t, config)

	// The hook replaces any query with "SELECT 2 AS val"
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 999 AS val"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	val, ok := output.Rows[0]["val"].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T: %v", output.Rows[0]["val"], output.Rows[0]["val"])
	}
	if val != 2 {
		t.Fatalf("expected 2, got %d", val)
	}
}

func TestQuery_GoBeforeHook_Timeout(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 1
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "slowpoke", Hook: &slowBeforeHook{sleepDuration: 10 * time.Second}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected hook timeout error")
	}
	if !strings.Contains(output.Error, "hook timed out") {
		t.Fatalf("expected 'hook timed out' in error, got %q", output.Error)
	}
	if !strings.Contains(output.Error, "slowpoke") {
		t.Fatalf("expected hook name 'slowpoke' in error, got %q", output.Error)
	}
}

func TestQuery_GoBeforeHook_ProtectionStillApplied(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.Protection.AllowDDL = true
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "sneaky", Hook: &modifyBeforeHook{replacement: "DROP TABLE users"}},
	}
	p, _ := newTestInstance(t, config)

	// Even though hook modifies query to DROP TABLE, protection checker should catch it
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected protection error after hook modified query")
	}
	if !strings.Contains(output.Error, "DROP") {
		t.Fatalf("expected DROP protection error, got %q", output.Error)
	}
}

func TestQuery_GoAfterHook_Accept(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "passthrough", Hook: &passthroughAfterHook{}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 42 AS val"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	val, ok := output.Rows[0]["val"].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T: %v", output.Rows[0]["val"], output.Rows[0]["val"])
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestQuery_GoAfterHook_Reject(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "auditor", Hook: &rejectAfterHook{}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected hook rejection error")
	}
	if !strings.Contains(output.Error, "auditor") {
		t.Fatalf("expected hook name 'auditor' in error, got %q", output.Error)
	}
	if !strings.Contains(output.Error, "result rejected by audit hook") {
		t.Fatalf("expected rejection message in error, got %q", output.Error)
	}
}

func TestQuery_GoAfterHook_ModifyResult(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "enricher", Hook: &addColumnAfterHook{}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1 AS val"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	if len(output.Columns) != 2 {
		t.Fatalf("expected 2 columns (val + hook_added), got %d: %v", len(output.Columns), output.Columns)
	}
	if output.Columns[1] != "hook_added" {
		t.Fatalf("expected 'hook_added' column, got %q", output.Columns[1])
	}
	if output.Rows[0]["hook_added"] != "injected" {
		t.Fatalf("expected 'injected' value, got %v", output.Rows[0]["hook_added"])
	}
}

func TestQuery_GoAfterHook_Timeout(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 1
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "slow_auditor", Hook: &slowAfterHook{sleepDuration: 10 * time.Second}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error == "" {
		t.Fatal("expected hook timeout error")
	}
	if !strings.Contains(output.Error, "hook timed out") {
		t.Fatalf("expected 'hook timed out' in error, got %q", output.Error)
	}
	if !strings.Contains(output.Error, "slow_auditor") {
		t.Fatalf("expected hook name 'slow_auditor' in error, got %q", output.Error)
	}
}

func TestQuery_GoAfterHook_NoPrecisionLoss(t *testing.T) {
	t.Parallel()
	// Setup: create table and insert bigint 2^53+1
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE bigint_hook_test (big_id bigint)")
	setupTable(t, setupP, "INSERT INTO bigint_hook_test VALUES (9007199254740993)") // 2^53+1
	setupP.Close(context.Background())

	// Create instance with capture hook to inspect the value the hook receives
	captureHook := &captureAfterHook{}
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "capture", Hook: captureHook},
	}
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT big_id FROM bigint_hook_test"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Verify the hook received int64 with exact precision
	if captureHook.captured == nil {
		t.Fatal("hook did not capture result")
	}
	val := captureHook.captured.Rows[0]["big_id"]
	int64Val, ok := val.(int64)
	if !ok {
		t.Fatalf("expected int64 in hook, got %T: %v", val, val)
	}
	if int64Val != 9007199254740993 {
		t.Fatalf("expected 9007199254740993, got %d", int64Val)
	}

	// Also verify the final output preserves the value
	finalVal := output.Rows[0]["big_id"]
	finalInt64, ok := finalVal.(int64)
	if !ok {
		t.Fatalf("expected int64 in output, got %T: %v", finalVal, finalVal)
	}
	if finalInt64 != 9007199254740993 {
		t.Fatalf("expected 9007199254740993 in output, got %d", finalInt64)
	}
}

func TestQuery_GoAfterHook_RejectRollbacksWrite(t *testing.T) {
	t.Parallel()
	// Setup: create table with a non-hooked instance
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users_go_reject (id serial PRIMARY KEY, name text)")
	setupP.Close(context.Background())

	// Create instance with rejecting after-hook
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "rejector", Hook: &rejectAfterHook{}},
	}
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "INSERT INTO users_go_reject (name) VALUES ('rejected_row') RETURNING *"})
	if output.Error == "" {
		t.Fatal("expected hook rejection error")
	}
	if !strings.Contains(output.Error, "result rejected by audit hook") {
		t.Fatalf("expected rejection message, got %q", output.Error)
	}

	// Verify the row was NOT inserted (rollback happened) using a non-hooked instance
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users_go_reject WHERE name = 'rejected_row'"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	cnt := verifyOutput.Rows[0]["cnt"]
	if cnt != int64(0) {
		t.Fatalf("expected 0 rows (rollback), got %v (%T)", cnt, cnt)
	}
}

func TestQuery_GoAfterHook_AcceptCommitsWrite(t *testing.T) {
	t.Parallel()
	// Setup: create table with a non-hooked instance
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users_go_accept (id serial PRIMARY KEY, name text)")
	setupP.Close(context.Background())

	// Create instance with passthrough after-hook
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "passthrough", Hook: &passthroughAfterHook{}},
	}
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "INSERT INTO users_go_accept (name) VALUES ('accepted_row') RETURNING *"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Verify the row WAS inserted (commit happened) using a non-hooked instance
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users_go_accept WHERE name = 'accepted_row'"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	cnt := verifyOutput.Rows[0]["cnt"]
	if cnt != int64(1) {
		t.Fatalf("expected 1 row (committed), got %v (%T)", cnt, cnt)
	}
}

func TestQuery_GoHooksMutualExclusion(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when both Go hooks and command hooks are configured")
		}
		panicMsg := fmt.Sprintf("%v", r)
		if !strings.Contains(panicMsg, "mutually exclusive") {
			t.Fatalf("expected 'mutually exclusive' in panic message, got %q", panicMsg)
		}
	}()

	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "go-hook", Hook: &passthroughBeforeHook{}},
	}

	connStr := acquireTestDB(t)
	ctx := context.Background()
	// This should panic because both Go hooks and command hooks are set
	_, _ = pgmcp.New(ctx, connStr, config, testLogger(), pgmcp.WithServerHooks(pgmcp.ServerHooksConfig{
		BeforeQuery: []pgmcp.HookEntry{
			{Pattern: ".*", Command: "echo", Args: []string{"{}"}},
		},
	}))
}

func TestQuery_GoHooksDefaultTimeoutRequired(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic when DefaultHookTimeoutSeconds is 0 with Go hooks configured")
		}
		panicMsg := fmt.Sprintf("%v", r)
		if !strings.Contains(panicMsg, "default_hook_timeout_seconds") {
			t.Fatalf("expected 'default_hook_timeout_seconds' in panic message, got %q", panicMsg)
		}
	}()

	config := defaultConfig()
	// DefaultHookTimeoutSeconds is 0 (not set)
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "go-hook", Hook: &passthroughBeforeHook{}},
	}

	connStr := acquireTestDB(t)
	ctx := context.Background()
	// This should panic because DefaultHookTimeoutSeconds is 0
	_, _ = pgmcp.New(ctx, connStr, config, testLogger())
}

// --- Section 5: Missing Go Hook Integration Tests ---

// appendBeforeHook appends a suffix to the query.
type appendBeforeHook struct {
	suffix string
}

func (h *appendBeforeHook) Run(_ context.Context, query string) (string, error) {
	return query + h.suffix, nil
}

// appendRowAfterHook appends a synthetic row to the result.
type appendRowAfterHook struct{}

func (h *appendRowAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	newRow := make(map[string]interface{})
	for _, col := range result.Columns {
		newRow[col] = "appended"
	}
	result.Rows = append(result.Rows, newRow)
	return result, nil
}

// typeAssertAfterHook verifies the Go types received by the hook and stores them.
type typeAssertAfterHook struct {
	receivedTypes map[string]string // column name -> Go type name
}

func (h *typeAssertAfterHook) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	h.receivedTypes = make(map[string]string)
	if len(result.Rows) > 0 {
		for col, val := range result.Rows[0] {
			h.receivedTypes[col] = fmt.Sprintf("%T", val)
		}
	}
	return result, nil
}

func TestQuery_GoBeforeHook_Chaining(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	// First hook appends " AS a", second hook appends " -- tagged"
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "append_as_a", Hook: &appendBeforeHook{suffix: " AS a"}},
		{Name: "append_tag", Hook: &appendBeforeHook{suffix: " -- tagged"}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}
	// "SELECT 1" → "SELECT 1 AS a" → "SELECT 1 AS a -- tagged"
	// The final query executed is "SELECT 1 AS a -- tagged", column should be "a"
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	if len(output.Columns) != 1 || output.Columns[0] != "a" {
		t.Fatalf("expected column 'a' from chained hooks, got %v", output.Columns)
	}
	val, ok := output.Rows[0]["a"].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T: %v", output.Rows[0]["a"], output.Rows[0]["a"])
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestQuery_GoBeforeHook_PerHookTimeout(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 1 // default timeout is 1s
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{
			Name:    "slow_but_ok",
			Timeout: 2 * time.Second, // per-hook timeout is 2s
			Hook:    &slowBeforeHook{sleepDuration: 1500 * time.Millisecond}, // sleeps 1.5s
		},
	}
	p, _ := newTestInstance(t, config)

	// Hook sleeps 1.5s. Default timeout 1s would fail, but per-hook timeout 2s should succeed.
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1 AS val"})
	if output.Error != "" {
		t.Fatalf("expected query to succeed with per-hook timeout override, got error: %s", output.Error)
	}
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	val, ok := output.Rows[0]["val"].(int32)
	if !ok {
		t.Fatalf("expected int32, got %T: %v", output.Rows[0]["val"], output.Rows[0]["val"])
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestQuery_GoAfterHook_Chaining(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	// First hook adds a column, second hook appends a row
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "add_column", Hook: &addColumnAfterHook{}},
		{Name: "append_row", Hook: &appendRowAfterHook{}},
	}
	p, _ := newTestInstance(t, config)

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT 1 AS val"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// First hook adds "hook_added" column, second hook appends a row
	if len(output.Columns) != 2 {
		t.Fatalf("expected 2 columns (val + hook_added), got %d: %v", len(output.Columns), output.Columns)
	}
	if output.Columns[0] != "val" || output.Columns[1] != "hook_added" {
		t.Fatalf("expected columns [val, hook_added], got %v", output.Columns)
	}

	// Should have 2 rows: original + appended
	if len(output.Rows) != 2 {
		t.Fatalf("expected 2 rows (original + appended), got %d", len(output.Rows))
	}

	// Original row has int32(1) for val, "injected" for hook_added
	if output.Rows[0]["hook_added"] != "injected" {
		t.Fatalf("expected 'injected' in first row, got %v", output.Rows[0]["hook_added"])
	}

	// Appended row has "appended" for both columns (appendRowAfterHook sets all to "appended")
	if output.Rows[1]["val"] != "appended" {
		t.Fatalf("expected 'appended' in appended row val, got %v", output.Rows[1]["val"])
	}
	if output.Rows[1]["hook_added"] != "appended" {
		t.Fatalf("expected 'appended' in appended row hook_added, got %v", output.Rows[1]["hook_added"])
	}
}

func TestQuery_GoAfterHook_ReceivesNativeTypes(t *testing.T) {
	t.Parallel()
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE native_types_test (big_id bigint, name text)")
	setupTable(t, setupP, "INSERT INTO native_types_test VALUES (9007199254740993, 'hello')") // 2^53+1
	setupP.Close(context.Background())

	// Hook that captures the Go types of each column
	typeHook := &typeAssertAfterHook{}
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "type_check", Hook: typeHook},
	}
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT big_id, name FROM native_types_test"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// Verify the hook received native Go types (no JSON serialization)
	if typeHook.receivedTypes["big_id"] != "int64" {
		t.Fatalf("expected int64 for big_id, hook received %s", typeHook.receivedTypes["big_id"])
	}
	if typeHook.receivedTypes["name"] != "string" {
		t.Fatalf("expected string for name, hook received %s", typeHook.receivedTypes["name"])
	}

	// Also verify the actual value preserved
	val := output.Rows[0]["big_id"]
	int64Val, ok := val.(int64)
	if !ok {
		t.Fatalf("expected int64 in output, got %T", val)
	}
	if int64Val != 9007199254740993 {
		t.Fatalf("expected 9007199254740993, got %d", int64Val)
	}
}

func TestQuery_GoAfterHook_SelectRollbacksBeforeHooks(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.DefaultHookTimeoutSeconds = 5

	// Use a passthrough hook — the key assertion is that SELECT rollback happens before hooks
	// and the hook still runs successfully with the query result
	captureHook := &captureAfterHook{}
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "capture", Hook: captureHook},
	}
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE select_rb_test (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "INSERT INTO select_rb_test (name) VALUES ('test_row')")

	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT * FROM select_rb_test"})
	if output.Error != "" {
		t.Fatalf("unexpected error: %s", output.Error)
	}

	// The hook was called and received the SELECT result
	if captureHook.captured == nil {
		t.Fatal("expected hook to be called")
	}
	if len(captureHook.captured.Rows) != 1 {
		t.Fatalf("expected hook to receive 1 row, got %d", len(captureHook.captured.Rows))
	}
	if captureHook.captured.Rows[0]["name"] != "test_row" {
		t.Fatalf("expected hook to receive 'test_row', got %v", captureHook.captured.Rows[0]["name"])
	}

	// Result correct
	if len(output.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(output.Rows))
	}
	if output.Rows[0]["name"] != "test_row" {
		t.Fatalf("expected 'test_row', got %v", output.Rows[0]["name"])
	}
}

func TestQuery_GoAfterHook_TimeoutRollbacksInsert(t *testing.T) {
	t.Parallel()
	// Setup: create table with a non-hooked instance
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users_go_timeout_insert (id serial PRIMARY KEY, name text)")
	setupP.Close(context.Background())

	// Create instance with slow after-hook that will time out
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 1
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "slow_auditor", Hook: &slowAfterHook{sleepDuration: 10 * time.Second}},
	}
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "INSERT INTO users_go_timeout_insert (name) VALUES ('timeout_row') RETURNING *"})
	if output.Error == "" {
		t.Fatal("expected hook timeout error")
	}
	if !strings.Contains(output.Error, "hook timed out") {
		t.Fatalf("expected 'hook timed out' in error, got %q", output.Error)
	}

	// Verify the row was NOT inserted (rollback happened) using a non-hooked instance
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users_go_timeout_insert WHERE name = 'timeout_row'"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	cnt := verifyOutput.Rows[0]["cnt"]
	if cnt != int64(0) {
		t.Fatalf("expected 0 rows (rollback), got %v (%T)", cnt, cnt)
	}
}

func TestQuery_GoAfterHook_TimeoutRollbacksUpdate(t *testing.T) {
	t.Parallel()
	// Setup: create table and insert initial data with a non-hooked instance
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, "CREATE TABLE users_go_timeout_update (id serial PRIMARY KEY, name text)")
	setupTable(t, setupP, "INSERT INTO users_go_timeout_update (name) VALUES ('original_name')")
	setupP.Close(context.Background())

	// Create instance with slow after-hook that will time out
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 1
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "slow_auditor", Hook: &slowAfterHook{sleepDuration: 10 * time.Second}},
	}
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	defer p.Close(ctx)

	output := p.Query(ctx, pgmcp.QueryInput{SQL: "UPDATE users_go_timeout_update SET name = 'updated_name' WHERE name = 'original_name' RETURNING *"})
	if output.Error == "" {
		t.Fatal("expected hook timeout error")
	}
	if !strings.Contains(output.Error, "hook timed out") {
		t.Fatalf("expected 'hook timed out' in error, got %q", output.Error)
	}

	// Verify the update was NOT applied (rollback happened) using a non-hooked instance
	verifyConfig := defaultConfig()
	verifyP, err := pgmcp.New(ctx, connStr, verifyConfig, testLogger())
	if err != nil {
		t.Fatalf("Failed to create verify instance: %v", err)
	}
	defer verifyP.Close(ctx)

	// The original row should still have the original name
	verifyOutput := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users_go_timeout_update WHERE name = 'original_name'"})
	if verifyOutput.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput.Error)
	}
	cnt := verifyOutput.Rows[0]["cnt"]
	if cnt != int64(1) {
		t.Fatalf("expected 1 row with original_name (rollback preserved it), got %v (%T)", cnt, cnt)
	}

	// No row should have the updated name
	verifyOutput2 := verifyP.Query(ctx, pgmcp.QueryInput{SQL: "SELECT count(*) AS cnt FROM users_go_timeout_update WHERE name = 'updated_name'"})
	if verifyOutput2.Error != "" {
		t.Fatalf("verification query failed: %s", verifyOutput2.Error)
	}
	cnt2 := verifyOutput2.Rows[0]["cnt"]
	if cnt2 != int64(0) {
		t.Fatalf("expected 0 rows with updated_name (rollback), got %v (%T)", cnt2, cnt2)
	}
}

func TestQuery_MaxSQLLength_RejectsBeforeHooks(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Query.MaxSQLLength = 20
	config.DefaultHookTimeoutSeconds = 5

	tracker := &trackingBeforeHook{}
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "tracker", Hook: tracker},
	}
	p, _ := newTestInstance(t, config)

	longSQL := "SELECT " + strings.Repeat("1,", 20) + "1"
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: longSQL})
	if output.Error == "" {
		t.Fatal("expected SQL length error")
	}
	if !strings.Contains(output.Error, "SQL query too long") {
		t.Fatalf("expected SQL length error, got %q", output.Error)
	}
	if tracker.called {
		t.Fatal("expected BeforeQuery hook to NOT be called when max_sql_length rejects the query")
	}
}
