package pgmcp

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// --- Mock hook implementations for unit tests ---

// mockPassthroughBeforeHook returns the query unchanged.
type mockPassthroughBeforeHook struct{}

func (h *mockPassthroughBeforeHook) Run(_ context.Context, query string) (string, error) {
	return query, nil
}

// mockModifyBeforeHook replaces the query with a fixed string.
type mockModifyBeforeHook struct {
	replacement string
}

func (h *mockModifyBeforeHook) Run(_ context.Context, _ string) (string, error) {
	return h.replacement, nil
}

// mockCaptureBeforeHook captures the query it receives and returns it unchanged.
type mockCaptureBeforeHook struct {
	received string
	called   bool
}

func (h *mockCaptureBeforeHook) Run(_ context.Context, query string) (string, error) {
	h.received = query
	h.called = true
	return query, nil
}

// mockRejectBeforeHook always returns an error.
type mockRejectBeforeHook struct{}

func (h *mockRejectBeforeHook) Run(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf("blocked")
}

// mockNeverCalledBeforeHook tracks whether it was called.
type mockNeverCalledBeforeHook struct {
	called bool
}

func (h *mockNeverCalledBeforeHook) Run(_ context.Context, query string) (string, error) {
	h.called = true
	return query, nil
}

// mockSlowBeforeHook sleeps until context is cancelled or duration elapses.
type mockSlowBeforeHook struct {
	sleepDuration time.Duration
}

func (h *mockSlowBeforeHook) Run(ctx context.Context, query string) (string, error) {
	select {
	case <-time.After(h.sleepDuration):
		return query, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// mockPassthroughAfterHook returns the result unchanged.
type mockPassthroughAfterHook struct{}

func (h *mockPassthroughAfterHook) Run(_ context.Context, result *QueryOutput) (*QueryOutput, error) {
	return result, nil
}

// mockAddColumnAfterHook adds a synthetic column to every row.
type mockAddColumnAfterHook struct{}

func (h *mockAddColumnAfterHook) Run(_ context.Context, result *QueryOutput) (*QueryOutput, error) {
	result.Columns = append(result.Columns, "hook_added")
	for _, row := range result.Rows {
		row["hook_added"] = "injected"
	}
	return result, nil
}

// mockCaptureAfterHook captures the result it receives and returns it unchanged.
type mockCaptureAfterHook struct {
	captured *QueryOutput
	called   bool
}

func (h *mockCaptureAfterHook) Run(_ context.Context, result *QueryOutput) (*QueryOutput, error) {
	h.captured = result
	h.called = true
	return result, nil
}

// mockTypeAssertAfterHook type-asserts specific values to verify no serialization occurred.
type mockTypeAssertAfterHook struct {
	int64Val  int64
	stringVal string
	typesOK   bool
}

func (h *mockTypeAssertAfterHook) Run(_ context.Context, result *QueryOutput) (*QueryOutput, error) {
	if len(result.Rows) == 0 {
		return result, fmt.Errorf("no rows to inspect")
	}
	row := result.Rows[0]

	iv, ok := row["id"].(int64)
	if !ok {
		return result, fmt.Errorf("expected int64 for 'id', got %T", row["id"])
	}
	h.int64Val = iv

	sv, ok := row["name"].(string)
	if !ok {
		return result, fmt.Errorf("expected string for 'name', got %T", row["name"])
	}
	h.stringVal = sv
	h.typesOK = true

	return result, nil
}

// --- Helper to create minimal PostgresMcp for unit tests ---

func newUnitTestInstance(beforeHooks []BeforeQueryHookEntry, afterHooks []AfterQueryHookEntry, defaultTimeoutSec int) *PostgresMcp {
	return &PostgresMcp{
		config: Config{
			DefaultHookTimeoutSeconds: defaultTimeoutSec,
		},
		goBeforeHooks: beforeHooks,
		goAfterHooks:  afterHooks,
	}
}

// --- Before hooks unit tests ---

func TestGoBeforeHooks_Chaining(t *testing.T) {
	captureHook := &mockCaptureBeforeHook{}
	p := newUnitTestInstance(
		[]BeforeQueryHookEntry{
			{Name: "modifier", Hook: &mockModifyBeforeHook{replacement: "SELECT 1 AS modified"}},
			{Name: "capture", Hook: captureHook},
		},
		nil,
		5,
	)

	result, err := p.runGoBeforeHooks(context.Background(), "SELECT original")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "SELECT 1 AS modified" {
		t.Fatalf("expected 'SELECT 1 AS modified', got %q", result)
	}
	if !captureHook.called {
		t.Fatal("second hook was not called")
	}
	if captureHook.received != "SELECT 1 AS modified" {
		t.Fatalf("second hook received %q, expected 'SELECT 1 AS modified'", captureHook.received)
	}
}

func TestGoBeforeHooks_ChainStopsOnReject(t *testing.T) {
	neverCalled := &mockNeverCalledBeforeHook{}
	p := newUnitTestInstance(
		[]BeforeQueryHookEntry{
			{Name: "rejector", Hook: &mockRejectBeforeHook{}},
			{Name: "never", Hook: neverCalled},
		},
		nil,
		5,
	)

	_, err := p.runGoBeforeHooks(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error from rejecting hook")
	}
	if neverCalled.called {
		t.Fatal("second hook should not have been called after first hook rejected")
	}
	expected := `before_query hook error: hook rejected query (name: rejector): blocked`
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestGoBeforeHooks_PerHookTimeoutOverridesDefault(t *testing.T) {
	p := newUnitTestInstance(
		[]BeforeQueryHookEntry{
			{
				Name:    "slow_but_ok",
				Timeout: 3 * time.Second,
				Hook:    &mockSlowBeforeHook{sleepDuration: 2 * time.Second},
			},
		},
		nil,
		1, // default timeout is 1s, but per-hook timeout is 3s
	)

	result, err := p.runGoBeforeHooks(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("expected success (per-hook timeout 3s > sleep 2s), got error: %v", err)
	}
	if result != "SELECT 1" {
		t.Fatalf("expected 'SELECT 1', got %q", result)
	}
}

func TestGoBeforeHooks_Empty(t *testing.T) {
	p := newUnitTestInstance(nil, nil, 5)

	result, err := p.runGoBeforeHooks(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "SELECT 1" {
		t.Fatalf("expected 'SELECT 1', got %q", result)
	}
}

// --- After hooks unit tests ---

func TestGoAfterHooks_Chaining(t *testing.T) {
	captureHook := &mockCaptureAfterHook{}
	p := newUnitTestInstance(
		nil,
		[]AfterQueryHookEntry{
			{Name: "enricher", Hook: &mockAddColumnAfterHook{}},
			{Name: "capture", Hook: captureHook},
		},
		5,
	)

	input := &QueryOutput{
		Columns: []string{"val"},
		Rows: []map[string]interface{}{
			{"val": int32(1)},
		},
	}

	result, err := p.runGoAfterHooks(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First hook adds "hook_added" column
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(result.Columns), result.Columns)
	}
	if result.Columns[0] != "val" {
		t.Fatalf("expected first column 'val', got %q", result.Columns[0])
	}
	if result.Columns[1] != "hook_added" {
		t.Fatalf("expected second column 'hook_added', got %q", result.Columns[1])
	}
	if result.Rows[0]["hook_added"] != "injected" {
		t.Fatalf("expected 'injected', got %v", result.Rows[0]["hook_added"])
	}

	// Second hook should have received the modified result
	if !captureHook.called {
		t.Fatal("second hook was not called")
	}
	if len(captureHook.captured.Columns) != 2 {
		t.Fatalf("second hook received %d columns, expected 2", len(captureHook.captured.Columns))
	}
	if captureHook.captured.Columns[1] != "hook_added" {
		t.Fatalf("second hook did not receive modified result, columns: %v", captureHook.captured.Columns)
	}
	if captureHook.captured.Rows[0]["hook_added"] != "injected" {
		t.Fatalf("second hook did not receive modified row, got %v", captureHook.captured.Rows[0]["hook_added"])
	}
}

func TestGoAfterHooks_Empty(t *testing.T) {
	p := newUnitTestInstance(nil, nil, 5)

	input := &QueryOutput{
		Columns: []string{"val"},
		Rows: []map[string]interface{}{
			{"val": int32(42)},
		},
		RowsAffected: 1,
	}

	result, err := p.runGoAfterHooks(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != input {
		t.Fatal("expected same pointer returned when no hooks")
	}
	if len(result.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(result.Columns))
	}
	if result.Columns[0] != "val" {
		t.Fatalf("expected column 'val', got %q", result.Columns[0])
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0]["val"] != int32(42) {
		t.Fatalf("expected val=42 (int32), got %v (%T)", result.Rows[0]["val"], result.Rows[0]["val"])
	}
	if result.RowsAffected != 1 {
		t.Fatalf("expected RowsAffected=1, got %d", result.RowsAffected)
	}
}

func TestGoAfterHooks_PreservesTypes(t *testing.T) {
	typeChecker := &mockTypeAssertAfterHook{}
	p := newUnitTestInstance(
		nil,
		[]AfterQueryHookEntry{
			{Name: "type_checker", Hook: typeChecker},
		},
		5,
	)

	input := &QueryOutput{
		Columns: []string{"id", "name"},
		Rows: []map[string]interface{}{
			{
				"id":   int64(9007199254740993), // 2^53+1, would lose precision in JSON
				"name": "test_user",
			},
		},
		RowsAffected: 1,
	}

	result, err := p.runGoAfterHooks(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the hook successfully type-asserted the values
	if !typeChecker.typesOK {
		t.Fatal("hook failed to type-assert values")
	}
	if typeChecker.int64Val != 9007199254740993 {
		t.Fatalf("expected int64 9007199254740993, got %d", typeChecker.int64Val)
	}
	if typeChecker.stringVal != "test_user" {
		t.Fatalf("expected string 'test_user', got %q", typeChecker.stringVal)
	}

	// Verify the output preserves the same types
	id, ok := result.Rows[0]["id"].(int64)
	if !ok {
		t.Fatalf("expected int64 for 'id' in output, got %T", result.Rows[0]["id"])
	}
	if id != 9007199254740993 {
		t.Fatalf("expected 9007199254740993, got %d", id)
	}

	name, ok := result.Rows[0]["name"].(string)
	if !ok {
		t.Fatalf("expected string for 'name' in output, got %T", result.Rows[0]["name"])
	}
	if name != "test_user" {
		t.Fatalf("expected 'test_user', got %q", name)
	}

	if result.RowsAffected != 1 {
		t.Fatalf("expected RowsAffected=1, got %d", result.RowsAffected)
	}
}
