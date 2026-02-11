package hooks

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func testLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).Level(zerolog.Disabled)
}

func hookScript(name string) string {
	// Find testdata relative to the repo root.
	// Tests run from the package directory, so we go up two levels.
	return filepath.Join("..", "..", "testdata", "hooks", name)
}

// --- BeforeQuery Tests ---

func TestBeforeQuery_Accept(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
	}, testLogger())

	result, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "SELECT 1" {
		t.Fatalf("expected query unchanged, got %q", result)
	}
}

func TestBeforeQuery_Reject(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("reject.sh")},
		},
	}, testLogger())

	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "rejected by test hook") {
		t.Fatalf("expected rejection message, got %q", err.Error())
	}
}

func TestBeforeQuery_ModifyQuery(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("modify_query.sh")},
		},
	}, testLogger())

	result, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "SELECT 1 AS modified" {
		t.Fatalf("expected modified query, got %q", result)
	}
}

func TestBeforeQuery_PatternNoMatch(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: "NEVER_MATCH", Command: hookScript("reject.sh")},
		},
	}, testLogger())

	result, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "SELECT 1" {
		t.Fatalf("expected query unchanged, got %q", result)
	}
}

func TestBeforeQuery_Chaining(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("modify_query.sh")},
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
	}, testLogger())

	result, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// First hook modifies to "SELECT 1 AS modified", second accepts unchanged
	if result != "SELECT 1 AS modified" {
		t.Fatalf("expected modified query, got %q", result)
	}
}

func TestBeforeQuery_ChainPatternReEval(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("modify_query.sh")},
			{Pattern: "modified", Command: hookScript("reject.sh")},
		},
	}, testLogger())

	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error from second hook matching modified query")
	}
	if !strings.Contains(err.Error(), "rejected by test hook") {
		t.Fatalf("expected rejection, got %q", err.Error())
	}
}

func TestBeforeQuery_Timeout(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 1 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("slow.sh")},
		},
	}, testLogger())

	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "hook timed out") {
		t.Fatalf("expected timeout error, got %q", err.Error())
	}
}

func TestBeforeQuery_Crash(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("crash.sh")},
		},
	}, testLogger())

	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected crash error")
	}
	if !strings.Contains(err.Error(), "hook failed") {
		t.Fatalf("expected hook failed error, got %q", err.Error())
	}
}

func TestBeforeQuery_UnparseableResponse(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("bad_json.sh")},
		},
	}, testLogger())

	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected unparseable response error")
	}
	if !strings.Contains(err.Error(), "unparseable response") {
		t.Fatalf("expected unparseable response error, got %q", err.Error())
	}
}

// --- AfterQuery Tests ---

func TestAfterQuery_Accept(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
	}, testLogger())

	result, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != `{"columns":["a"],"rows":[]}` {
		t.Fatalf("expected result unchanged, got %q", result)
	}
}

func TestAfterQuery_Reject(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("reject.sh")},
		},
	}, testLogger())

	_, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "rejected by test hook") {
		t.Fatalf("expected rejection, got %q", err.Error())
	}
}

func TestAfterQuery_ModifyResult(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("modify_result.sh")},
		},
	}, testLogger())

	result, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "modified") {
		t.Fatalf("expected modified result, got %q", result)
	}
}

func TestAfterQuery_Chaining(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("modify_result.sh")},
			{Pattern: ".*", Command: hookScript("accept.sh")},
		},
	}, testLogger())

	result, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "modified") {
		t.Fatalf("expected modified result, got %q", result)
	}
}

func TestAfterQuery_Timeout(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 1 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("slow.sh")},
		},
	}, testLogger())

	_, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "hook timed out") {
		t.Fatalf("expected timeout error, got %q", err.Error())
	}
}

func TestAfterQuery_Crash(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("crash.sh")},
		},
	}, testLogger())

	_, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err == nil {
		t.Fatal("expected crash error")
	}
	if !strings.Contains(err.Error(), "hook failed") {
		t.Fatalf("expected hook failed error, got %q", err.Error())
	}
}

func TestAfterQuery_UnparseableResponse(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("bad_json.sh")},
		},
	}, testLogger())

	_, err := r.RunAfterQuery(context.Background(), `{"columns":["a"],"rows":[]}`)
	if err == nil {
		t.Fatal("expected unparseable response error")
	}
	if !strings.Contains(err.Error(), "unparseable response") {
		t.Fatalf("expected unparseable response error, got %q", err.Error())
	}
}

// --- Hook Input / Args Tests ---

func TestHookWithArgs(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("echo_args.sh"), Args: []string{"--flag", "value"}},
		},
	}, testLogger())

	result, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "ARGS: --flag value") {
		t.Fatalf("expected args in modified query, got %q", result)
	}
}

func TestHookWithEmptyArgs(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("accept.sh"), Args: []string{}},
		},
	}, testLogger())

	result, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "SELECT 1" {
		t.Fatalf("expected unchanged, got %q", result)
	}
}

func TestHookDefaultTimeout(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 1 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("slow.sh")}, // no per-hook timeout, uses default
		},
	}, testLogger())

	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected timeout error (default timeout)")
	}
	if !strings.Contains(err.Error(), "hook timed out") {
		t.Fatalf("expected timeout error, got %q", err.Error())
	}
}

func TestHookPerHookTimeoutOverridesDefault(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 1 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: hookScript("slow.sh"), Timeout: 2 * time.Second}, // per-hook 2s, still times out (sleep 30)
		},
	}, testLogger())

	start := time.Now()
	_, err := r.RunBeforeQuery(context.Background(), "SELECT 1")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	// Should have waited ~2s (per-hook timeout), not ~1s (default)
	if elapsed < 1500*time.Millisecond {
		t.Fatalf("expected per-hook timeout (~2s), but elapsed only %v", elapsed)
	}
}

func TestHookPanicOnZeroDefaultTimeout(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "default_hook_timeout_seconds") {
			t.Fatalf("expected panic about default_hook_timeout_seconds, got %v", r)
		}
	}()

	NewRunner(Config{
		DefaultTimeout: 0,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: "dummy"},
		},
	}, testLogger())
}

func TestHasAfterQueryHooks_True(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		AfterQuery: []HookEntry{
			{Pattern: ".*", Command: "dummy"},
		},
	}, testLogger())

	if !r.HasAfterQueryHooks() {
		t.Fatal("expected HasAfterQueryHooks to return true")
	}
}

func TestHasAfterQueryHooks_False(t *testing.T) {
	r := NewRunner(Config{
		DefaultTimeout: 5 * time.Second,
		BeforeQuery: []HookEntry{
			{Pattern: ".*", Command: "dummy"},
		},
	}, testLogger())

	if r.HasAfterQueryHooks() {
		t.Fatal("expected HasAfterQueryHooks to return false")
	}
}
