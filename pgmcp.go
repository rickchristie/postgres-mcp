package pgmcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/rickchristie/postgres-mcp/internal/errprompt"
	"github.com/rickchristie/postgres-mcp/internal/hooks"
	"github.com/rickchristie/postgres-mcp/internal/protection"
	"github.com/rickchristie/postgres-mcp/internal/sanitize"
	"github.com/rickchristie/postgres-mcp/internal/timeout"
)

// PostgresMcp is the core engine that provides Query, ListTables, and DescribeTable tools.
// All exported methods are safe for concurrent use from multiple goroutines.
type PostgresMcp struct {
	config        Config
	pool          *pgxpool.Pool
	semaphore     chan struct{}
	protection    *protection.Checker
	cmdHooks      *hooks.Runner          // command-based hooks (CLI mode)
	goBeforeHooks []BeforeQueryHookEntry // Go function hooks (library mode)
	goAfterHooks  []AfterQueryHookEntry  // Go function hooks (library mode)
	sanitizer     *sanitize.Sanitizer
	errPrompts    *errprompt.Matcher
	timeoutMgr    *timeout.Manager
	logger        zerolog.Logger
}

// Option is a functional option for New().
type Option func(*options)

type options struct {
	serverHooks *ServerHooksConfig
}

// WithServerHooks passes command-based hook configuration to PostgresMcp.
// Mutually exclusive with Config.BeforeQueryHooks/AfterQueryHooks (Go hooks).
func WithServerHooks(h ServerHooksConfig) Option {
	return func(o *options) {
		o.serverHooks = &h
	}
}

// New creates a new PostgresMcp instance.
// connString is the PostgreSQL connection string (must include credentials).
// In library mode, connString is required — Config.Connection fields are ignored
// (the CLI is responsible for building connString from Config.Connection + prompted credentials).
// Panics on invalid config. Returns error only for runtime failures (e.g., pool creation).
func New(ctx context.Context, connString string, config Config, logger zerolog.Logger, opts ...Option) (*PostgresMcp, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	// --- Config validation (panics on invalid config) ---

	if connString == "" {
		panic("pgmcp: connString must be non-empty")
	}
	if config.Pool.MaxConns <= 0 {
		panic("pgmcp: pool.max_conns must be > 0")
	}
	if config.Query.DefaultTimeoutSeconds <= 0 {
		panic("pgmcp: query.default_timeout_seconds must be > 0")
	}
	if config.Query.ListTablesTimeoutSeconds <= 0 {
		panic("pgmcp: query.list_tables_timeout_seconds must be > 0")
	}
	if config.Query.DescribeTableTimeoutSeconds <= 0 {
		panic("pgmcp: query.describe_table_timeout_seconds must be > 0")
	}

	// Apply defaults for zero values
	if config.Query.MaxSQLLength == 0 {
		config.Query.MaxSQLLength = 100000
	}
	if config.Query.MaxResultLength == 0 {
		config.Query.MaxResultLength = 100000
	}
	if config.Query.MaxSQLLength < 0 {
		panic("pgmcp: query.max_sql_length must be > 0")
	}
	if config.Query.MaxResultLength < 0 {
		panic("pgmcp: query.max_result_length must be > 0")
	}

	// Validate hook configuration: Go hooks and command hooks are mutually exclusive
	hasGoHooks := len(config.BeforeQueryHooks) > 0 || len(config.AfterQueryHooks) > 0
	hasCmdHooks := o.serverHooks != nil && (len(o.serverHooks.BeforeQuery) > 0 || len(o.serverHooks.AfterQuery) > 0)
	if hasGoHooks && hasCmdHooks {
		panic("pgmcp: Go hooks (Config.BeforeQueryHooks/AfterQueryHooks) and command hooks (WithServerHooks) are mutually exclusive")
	}

	// Validate DefaultHookTimeoutSeconds if any hooks are configured
	if hasGoHooks && config.DefaultHookTimeoutSeconds <= 0 {
		panic("pgmcp: default_hook_timeout_seconds must be > 0 when Go hooks are configured")
	}

	// Validate per-hook timeouts for Go hooks
	for _, entry := range config.BeforeQueryHooks {
		if entry.Timeout < 0 {
			panic(fmt.Sprintf("pgmcp: before_query hook %q has negative timeout", entry.Name))
		}
	}
	for _, entry := range config.AfterQueryHooks {
		if entry.Timeout < 0 {
			panic(fmt.Sprintf("pgmcp: after_query hook %q has negative timeout", entry.Name))
		}
	}

	// Validate timeout rules
	for _, rule := range config.Query.TimeoutRules {
		if rule.TimeoutSeconds <= 0 {
			panic(fmt.Sprintf("pgmcp: timeout_rule with pattern %q has timeout_seconds <= 0", rule.Pattern))
		}
	}

	// --- Configure pgxpool ---

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(config.Pool.MaxConns)
	poolConfig.MinConns = int32(config.Pool.MinConns)
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	// Parse pool duration strings
	if config.Pool.MaxConnLifetime != "" {
		d, err := time.ParseDuration(config.Pool.MaxConnLifetime)
		if err != nil {
			panic(fmt.Sprintf("pgmcp: invalid pool.max_conn_lifetime %q: %v", config.Pool.MaxConnLifetime, err))
		}
		poolConfig.MaxConnLifetime = d
	}
	if config.Pool.MaxConnIdleTime != "" {
		d, err := time.ParseDuration(config.Pool.MaxConnIdleTime)
		if err != nil {
			panic(fmt.Sprintf("pgmcp: invalid pool.max_conn_idle_time %q: %v", config.Pool.MaxConnIdleTime, err))
		}
		poolConfig.MaxConnIdleTime = d
	}
	if config.Pool.HealthCheckPeriod != "" {
		d, err := time.ParseDuration(config.Pool.HealthCheckPeriod)
		if err != nil {
			panic(fmt.Sprintf("pgmcp: invalid pool.health_check_period %q: %v", config.Pool.HealthCheckPeriod, err))
		}
		poolConfig.HealthCheckPeriod = d
	}

	// Set AfterConnect hook for session-level settings
	if config.ReadOnly || config.Timezone != "" {
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			if config.ReadOnly {
				if _, err := conn.Exec(ctx, "SET default_transaction_read_only = on"); err != nil {
					return fmt.Errorf("failed to SET default_transaction_read_only: %w", err)
				}
			}
			if config.Timezone != "" {
				escaped := strings.ReplaceAll(config.Timezone, "'", "''")
				if _, err := conn.Exec(ctx, fmt.Sprintf("SET timezone = '%s'", escaped)); err != nil {
					return fmt.Errorf("failed to SET timezone: %w", err)
				}
			}
			return nil
		}
	}

	// --- Create pool ---

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// --- Initialize internal components ---

	protectionChecker := protection.NewChecker(protection.Config{
		AllowSet:                config.Protection.AllowSet,
		AllowDrop:               config.Protection.AllowDrop,
		AllowTruncate:           config.Protection.AllowTruncate,
		AllowDo:                 config.Protection.AllowDo,
		AllowCopyFrom:           config.Protection.AllowCopyFrom,
		AllowCopyTo:             config.Protection.AllowCopyTo,
		AllowCreateFunction:     config.Protection.AllowCreateFunction,
		AllowPrepare:            config.Protection.AllowPrepare,
		AllowDeleteWithoutWhere: config.Protection.AllowDeleteWithoutWhere,
		AllowUpdateWithoutWhere: config.Protection.AllowUpdateWithoutWhere,
		AllowAlterSystem:        config.Protection.AllowAlterSystem,
		AllowMerge:              config.Protection.AllowMerge,
		AllowGrantRevoke:        config.Protection.AllowGrantRevoke,
		AllowManageRoles:        config.Protection.AllowManageRoles,
		AllowCreateExtension:    config.Protection.AllowCreateExtension,
		AllowLockTable:          config.Protection.AllowLockTable,
		AllowListenNotify:       config.Protection.AllowListenNotify,
		AllowMaintenance:        config.Protection.AllowMaintenance,
		AllowDDL:                config.Protection.AllowDDL,
		AllowDiscard:            config.Protection.AllowDiscard,
		AllowComment:            config.Protection.AllowComment,
		AllowCreateTrigger:      config.Protection.AllowCreateTrigger,
		AllowCreateRule:         config.Protection.AllowCreateRule,
		ReadOnly:                config.ReadOnly,
	})

	san := sanitize.NewSanitizer(mapSanitizationRules(config.Sanitization))
	matcher := errprompt.NewMatcher(mapErrorPromptRules(config.ErrorPrompts))
	timeoutRules := make([]timeout.Rule, len(config.Query.TimeoutRules))
	for i, r := range config.Query.TimeoutRules {
		timeoutRules[i] = timeout.Rule{
			Pattern: r.Pattern,
			Timeout: time.Duration(r.TimeoutSeconds) * time.Second,
		}
	}
	tmgr := timeout.NewManager(timeout.Config{
		DefaultTimeout: time.Duration(config.Query.DefaultTimeoutSeconds) * time.Second,
		Rules:          timeoutRules,
	})

	// Initialize command hooks if configured
	var cmdHooks *hooks.Runner
	if hasCmdHooks {
		hookEntries := func(entries []HookEntry) []hooks.HookEntry {
			result := make([]hooks.HookEntry, len(entries))
			for i, e := range entries {
				result[i] = hooks.HookEntry{
					Pattern: e.Pattern,
					Command: e.Command,
					Args:    e.Args,
					Timeout: time.Duration(e.TimeoutSeconds) * time.Second,
				}
			}
			return result
		}
		cmdHooks = hooks.NewRunner(hooks.Config{
			DefaultTimeout: time.Duration(config.DefaultHookTimeoutSeconds) * time.Second,
			BeforeQuery:    hookEntries(o.serverHooks.BeforeQuery),
			AfterQuery:     hookEntries(o.serverHooks.AfterQuery),
		}, logger)
	}

	return &PostgresMcp{
		config:        config,
		pool:          pool,
		semaphore:     make(chan struct{}, config.Pool.MaxConns),
		protection:    protectionChecker,
		cmdHooks:      cmdHooks,
		goBeforeHooks: config.BeforeQueryHooks,
		goAfterHooks:  config.AfterQueryHooks,
		sanitizer:     san,
		errPrompts:    matcher,
		timeoutMgr:    tmgr,
		logger:        logger,
	}, nil
}

// Close closes the connection pool. Accepts context for API forward-compatibility,
// but does not currently use it — pgxpool.Pool.Close() does not support context-based shutdown.
func (p *PostgresMcp) Close(ctx context.Context) {
	p.pool.Close()
}

// mapSanitizationRules converts pgmcp SanitizationRules to internal sanitize.Rules.
func mapSanitizationRules(rules []SanitizationRule) []sanitize.Rule {
	result := make([]sanitize.Rule, len(rules))
	for i, r := range rules {
		result[i] = sanitize.Rule{
			Pattern:     r.Pattern,
			Replacement: r.Replacement,
		}
	}
	return result
}

// mapErrorPromptRules converts pgmcp ErrorPromptRules to internal errprompt.Rules.
func mapErrorPromptRules(rules []ErrorPromptRule) []errprompt.Rule {
	result := make([]errprompt.Rule, len(rules))
	for i, r := range rules {
		result[i] = errprompt.Rule{
			Pattern: r.Pattern,
			Message: r.Message,
		}
	}
	return result
}
