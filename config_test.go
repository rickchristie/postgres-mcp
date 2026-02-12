package pgmcp_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
	"github.com/rs/zerolog"
)

// dummyConnString is a parseable connString for tests that expect panics before pool creation.
const dummyConnString = "postgresql://user:pass@localhost:5432/db?sslmode=disable"

// configTestLogger returns a disabled zerolog logger for config tests.
func configTestLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).Level(zerolog.Disabled)
}

// validConfig returns a minimal valid Config for testing.
func validConfig() pgmcp.Config {
	return pgmcp.Config{
		Pool: pgmcp.PoolConfig{MaxConns: 5},
		Query: pgmcp.QueryConfig{
			DefaultTimeoutSeconds:       30,
			ListTablesTimeoutSeconds:    10,
			DescribeTableTimeoutSeconds: 10,
		},
	}
}

// expectPanic calls f and asserts that it panics with a message containing substr.
func expectPanic(t *testing.T, substr string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, but no panic occurred", substr)
		}
		msg := ""
		switch v := r.(type) {
		case string:
			msg = v
		case error:
			msg = v.Error()
		default:
			t.Fatalf("expected panic string/error containing %q, got %T: %v", substr, r, r)
		}
		if !strings.Contains(msg, substr) {
			t.Fatalf("expected panic containing %q, got %q", substr, msg)
		}
	}()
	f()
}

// expectNoPanic calls f and asserts that it does NOT panic.
func expectNoPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	f()
}

func TestLoadConfigInvalidRegex(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.Sanitization = []pgmcp.SanitizationRule{
		{Pattern: "[invalid(regex", Replacement: "***"},
	}

	expectPanic(t, "regex", func() {
		// NewSanitizer is called inside New(), which will panic on invalid regex
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_ZeroMaxConns(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.Pool.MaxConns = 0

	expectPanic(t, "pool.max_conns", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_ZeroDefaultTimeout(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.Query.DefaultTimeoutSeconds = 0

	expectPanic(t, "default_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_MissingDefaultTimeout(t *testing.T) {
	t.Parallel()
	// Omitting DefaultTimeoutSeconds leaves it at 0 (Go zero value)
	config := pgmcp.Config{
		Pool: pgmcp.PoolConfig{MaxConns: 5},
		Query: pgmcp.QueryConfig{
			ListTablesTimeoutSeconds:    10,
			DescribeTableTimeoutSeconds: 10,
		},
	}

	expectPanic(t, "default_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_ZeroListTablesTimeout(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.Query.ListTablesTimeoutSeconds = 0

	expectPanic(t, "list_tables_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_ZeroDescribeTableTimeout(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.Query.DescribeTableTimeoutSeconds = 0

	expectPanic(t, "describe_table_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_NegativeTimeout(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.Query.DefaultTimeoutSeconds = -1

	expectPanic(t, "default_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_ZeroHookDefaultTimeout(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.DefaultHookTimeoutSeconds = 0
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "test", Hook: &passthroughBeforeHookConfig{}},
	}

	expectPanic(t, "default_hook_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_MissingHookDefaultTimeout(t *testing.T) {
	t.Parallel()
	// Omitting DefaultHookTimeoutSeconds leaves it at 0
	config := validConfig()
	config.AfterQueryHooks = []pgmcp.AfterQueryHookEntry{
		{Name: "test", Hook: &passthroughAfterHookConfig{}},
	}

	expectPanic(t, "default_hook_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_HookDefaultTimeoutNotRequiredWithoutHooks(t *testing.T) {
	t.Parallel()
	// No hooks configured, DefaultHookTimeoutSeconds omitted (0) — should NOT panic
	// (will fail at pool creation, but should not panic on hook validation)
	config := validConfig()
	config.DefaultHookTimeoutSeconds = 0

	expectNoPanic(t, func() {
		// This will return an error (can't connect to dummy DB) but should NOT panic
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_HookTimeoutFallback(t *testing.T) {
	t.Parallel()
	// Per-hook timeout = 0 (zero value) should fall back to DefaultHookTimeoutSeconds.
	// This test verifies the config is accepted without panic — the actual fallback
	// behavior is tested in the Go hook unit tests.
	config := validConfig()
	config.DefaultHookTimeoutSeconds = 10
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "test", Hook: &passthroughBeforeHookConfig{}}, // Timeout = 0 (will use default)
	}

	expectNoPanic(t, func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigProtectionDefaults(t *testing.T) {
	t.Parallel()
	// Parse a minimal config JSON — all protection fields should be false (Go zero-value)
	configJSON := `{
		"pool": {"max_conns": 5},
		"query": {
			"default_timeout_seconds": 30,
			"list_tables_timeout_seconds": 10,
			"describe_table_timeout_seconds": 10
		}
	}`

	var config pgmcp.Config
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify all Allow* fields default to false
	p := config.Protection
	if p.AllowSet || p.AllowDrop || p.AllowTruncate || p.AllowDo {
		t.Fatal("expected AllowSet/AllowDrop/AllowTruncate/AllowDo to be false")
	}
	if p.AllowCopyFrom || p.AllowCopyTo {
		t.Fatal("expected AllowCopyFrom/AllowCopyTo to be false")
	}
	if p.AllowCreateFunction || p.AllowPrepare {
		t.Fatal("expected AllowCreateFunction/AllowPrepare to be false")
	}
	if p.AllowDeleteWithoutWhere || p.AllowUpdateWithoutWhere {
		t.Fatal("expected AllowDeleteWithoutWhere/AllowUpdateWithoutWhere to be false")
	}
	if p.AllowAlterSystem || p.AllowMerge {
		t.Fatal("expected AllowAlterSystem/AllowMerge to be false")
	}
	if p.AllowGrantRevoke || p.AllowManageRoles {
		t.Fatal("expected AllowGrantRevoke/AllowManageRoles to be false")
	}
	if p.AllowCreateExtension || p.AllowLockTable || p.AllowListenNotify {
		t.Fatal("expected AllowCreateExtension/AllowLockTable/AllowListenNotify to be false")
	}
	if p.AllowMaintenance || p.AllowDDL || p.AllowDiscard {
		t.Fatal("expected AllowMaintenance/AllowDDL/AllowDiscard to be false")
	}
	if p.AllowComment || p.AllowCreateTrigger || p.AllowCreateRule {
		t.Fatal("expected AllowComment/AllowCreateTrigger/AllowCreateRule to be false")
	}
}

func TestLoadConfigProtectionExplicitAllow(t *testing.T) {
	t.Parallel()
	configJSON := `{
		"pool": {"max_conns": 5},
		"query": {
			"default_timeout_seconds": 30,
			"list_tables_timeout_seconds": 10,
			"describe_table_timeout_seconds": 10
		},
		"protection": {
			"allow_drop": true
		}
	}`

	var config pgmcp.Config
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if !config.Protection.AllowDrop {
		t.Fatal("expected AllowDrop to be true")
	}
	// Verify others remain false
	if config.Protection.AllowSet || config.Protection.AllowTruncate || config.Protection.AllowDo {
		t.Fatal("expected other protection fields to remain false")
	}
	if config.Protection.AllowDDL || config.Protection.AllowCreateFunction || config.Protection.AllowPrepare {
		t.Fatal("expected other protection fields to remain false")
	}
}

func TestLoadConfigProtectionNewFields(t *testing.T) {
	t.Parallel()
	configJSON := `{
		"pool": {"max_conns": 5},
		"query": {
			"default_timeout_seconds": 30,
			"list_tables_timeout_seconds": 10,
			"describe_table_timeout_seconds": 10
		},
		"protection": {
			"allow_copy_from": true,
			"allow_copy_to": true,
			"allow_create_function": true,
			"allow_prepare": true,
			"allow_merge": true,
			"allow_grant_revoke": true,
			"allow_manage_roles": true,
			"allow_create_extension": true,
			"allow_lock_table": true,
			"allow_listen_notify": true,
			"allow_maintenance": true,
			"allow_ddl": true,
			"allow_discard": true,
			"allow_comment": true,
			"allow_create_trigger": true,
			"allow_create_rule": true
		}
	}`

	var config pgmcp.Config
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	p := config.Protection
	if !p.AllowCopyFrom {
		t.Fatal("expected AllowCopyFrom to be true")
	}
	if !p.AllowCopyTo {
		t.Fatal("expected AllowCopyTo to be true")
	}
	if !p.AllowCreateFunction {
		t.Fatal("expected AllowCreateFunction to be true")
	}
	if !p.AllowPrepare {
		t.Fatal("expected AllowPrepare to be true")
	}
	if !p.AllowMerge {
		t.Fatal("expected AllowMerge to be true")
	}
	if !p.AllowGrantRevoke {
		t.Fatal("expected AllowGrantRevoke to be true")
	}
	if !p.AllowManageRoles {
		t.Fatal("expected AllowManageRoles to be true")
	}
	if !p.AllowCreateExtension {
		t.Fatal("expected AllowCreateExtension to be true")
	}
	if !p.AllowLockTable {
		t.Fatal("expected AllowLockTable to be true")
	}
	if !p.AllowListenNotify {
		t.Fatal("expected AllowListenNotify to be true")
	}
	if !p.AllowMaintenance {
		t.Fatal("expected AllowMaintenance to be true")
	}
	if !p.AllowDDL {
		t.Fatal("expected AllowDDL to be true")
	}
	if !p.AllowDiscard {
		t.Fatal("expected AllowDiscard to be true")
	}
	if !p.AllowComment {
		t.Fatal("expected AllowComment to be true")
	}
	if !p.AllowCreateTrigger {
		t.Fatal("expected AllowCreateTrigger to be true")
	}
	if !p.AllowCreateRule {
		t.Fatal("expected AllowCreateRule to be true")
	}
	// Verify non-set fields remain false
	if p.AllowSet || p.AllowDrop || p.AllowTruncate || p.AllowDo {
		t.Fatal("expected AllowSet/AllowDrop/AllowTruncate/AllowDo to remain false")
	}
}

func TestLoadConfigSSLMode(t *testing.T) {
	t.Parallel()
	configJSON := `{
		"pool": {"max_conns": 5},
		"query": {
			"default_timeout_seconds": 30,
			"list_tables_timeout_seconds": 10,
			"describe_table_timeout_seconds": 10
		},
		"connection": {
			"sslmode": "verify-full"
		},
		"server": {
			"port": 8080
		}
	}`

	var config pgmcp.ServerConfig
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if config.Connection.SSLMode != "verify-full" {
		t.Fatalf("expected sslmode 'verify-full', got %q", config.Connection.SSLMode)
	}
}

func TestLoadConfigValidation_GoHooksAndCmdHooksMutuallyExclusive(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.DefaultHookTimeoutSeconds = 10
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "go-hook", Hook: &passthroughBeforeHookConfig{}},
	}

	expectPanic(t, "mutually exclusive", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger(),
			pgmcp.WithServerHooks(pgmcp.ServerHooksConfig{
				BeforeQuery: []pgmcp.HookEntry{
					{Pattern: ".*", Command: "dummy", TimeoutSeconds: 5},
				},
			}),
		)
	})
}

func TestLoadConfigValidation_GoHooksRequireDefaultTimeout(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.DefaultHookTimeoutSeconds = 0
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "go-hook", Hook: &passthroughBeforeHookConfig{}},
	}

	expectPanic(t, "default_hook_timeout_seconds", func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

func TestLoadConfigValidation_GoHooksOnlyNoCmd(t *testing.T) {
	t.Parallel()
	config := validConfig()
	config.DefaultHookTimeoutSeconds = 10
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{Name: "go-hook", Hook: &passthroughBeforeHookConfig{}},
	}

	// Should NOT panic (only Go hooks, no cmd hooks)
	expectNoPanic(t, func() {
		pgmcp.New(context.Background(), dummyConnString, config, configTestLogger())
	})
}

// --- Minimal hook implementations for config tests ---

type passthroughBeforeHookConfig struct{}

func (h *passthroughBeforeHookConfig) Run(_ context.Context, query string) (string, error) {
	return query, nil
}

type passthroughAfterHookConfig struct{}

func (h *passthroughAfterHookConfig) Run(_ context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	return result, nil
}
