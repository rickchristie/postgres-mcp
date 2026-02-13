# Implementation Plan: gopgmcp — Postgres MCP Server

## Context

This is a greenfield Go project implementing a Model Context Protocol (MCP) server for PostgreSQL. The server exposes three tools (Query, ListTables, DescribeTable) over HTTP transport, with a comprehensive security and processing pipeline including SQL protection, hooks, sanitization, error prompts, and timeout management. It also serves as a library that can be imported directly into Go agent code.

The repository currently contains only `REQUIREMENTS.md`, `LICENSE`, and pgflock test infrastructure (`.pgflock/`). No Go code exists yet.

---

## Technology Choices

| Component | Library | Rationale |
|---|---|---|
| MCP Server | `github.com/mark3labs/mcp-go` | Production-ready, 8k+ stars, supports Streamable HTTP, MCP 2025-03-26 compatible, clean tool registration API |
| Postgres Driver | `github.com/jackc/pgx/v5` + `pgxpool` | Industry standard, supports `QueryExecModeExec` for single-statement enforcement |
| SQL Parser | `github.com/pganalyze/pg_query_go/v6` | Uses PostgreSQL's actual C parser via cgo, 100% parsing fidelity |
| Logger | `github.com/rs/zerolog` | Per requirements |
| Test DBs | `github.com/rickchristie/govner/pgflock/client` | Already configured in `.pgflock/`, locker port 9776 |

---

## Module & Package Structure

**Module**: `github.com/rickchristie/postgres-mcp`

```
postgres-mcp/
├── cmd/
│   └── gopgmcp/
│       ├── main.go                    # CLI entrypoint (cobra or simple flag-based)
│       ├── serve.go                   # `gopgmcp serve` command
│       └── configure.go              # `gopgmcp configure` interactive command
│
├── pgmcp.go                          # Public API: PostgresMcp struct, New(), Close()
├── config.go                         # Public config struct definitions
├── query.go                          # Query() method implementation
├── listtables.go                     # ListTables() method implementation
├── describetable.go                  # DescribeTable() method implementation
├── mcp.go                            # RegisterMCPTools() — bridges pgmcp ↔ mcp-go
│
├── internal/
│   ├── protection/
│   │   ├── protection.go             # SQL AST checker using pg_query_go
│   │   └── protection_test.go
│   │
│   ├── hooks/
│   │   ├── hooks.go                  # Hook runner (exec.Command + stdin/stdout)
│   │   └── hooks_test.go
│   │
│   ├── sanitize/
│   │   ├── sanitize.go               # Per-field regex sanitization, JSONB recursion
│   │   └── sanitize_test.go
│   │
│   ├── errprompt/
│   │   ├── errprompt.go              # Error message → prompt matching
│   │   └── errprompt_test.go
│   │
│   ├── timeout/
│   │   ├── timeout.go                # Regex → timeout duration matching
│   │   └── timeout_test.go
│   │
│   └── configure/
│       ├── configure.go              # Interactive config wizard logic
│       └── configure_test.go
│
├── integration_test.go               # Integration tests (pgflock)
├── stress_test.go                    # Stress tests (pgflock)
├── testdata/
│   └── hooks/
│       ├── accept.sh                 # Test hook: returns accept:true
│       ├── reject.sh                 # Test hook: returns accept:false
│       ├── modify_query.sh           # Test hook: returns modified_query
│       ├── modify_result.sh          # Test hook: returns modified_result
│       ├── slow.sh                   # Test hook: sleeps (for timeout testing)
│       ├── crash.sh                  # Test hook: exits with error
│       └── bad_json.sh              # Test hook: returns unparseable content
│
├── go.mod
├── go.sum
├── REQUIREMENTS.md
├── IMPLEMENTATION.md
└── LICENSE
```

---

## Phase 1: Project Bootstrap & Config

### 1.1 Initialize Go module

**File: `go.mod`**

```
module github.com/rickchristie/postgres-mcp

go 1.23
```

Dependencies to add:
```
github.com/mark3labs/mcp-go
github.com/jackc/pgx/v5
github.com/pganalyze/pg_query_go/v6
github.com/rs/zerolog
github.com/rickchristie/govner/pgflock/client  (test only)
```

### 1.2 Config Structs

**File: `config.go`**

All config structs are exported (public API for library mode).

```go
package pgmcp

// Config is the base configuration used by library mode via New().
type Config struct {
    Pool                     PoolConfig         `json:"pool"`
    Protection               ProtectionConfig   `json:"protection"`
    Query                    QueryConfig        `json:"query"`
    ErrorPrompts             []ErrorPromptRule  `json:"error_prompts"`
    Sanitization             []SanitizationRule `json:"sanitization"`
    ReadOnly                 bool               `json:"read_only"`
    Timezone                 string             `json:"timezone"`
    DefaultHookTimeoutSeconds int               `json:"default_hook_timeout_seconds"`

    // Library mode: Go function hooks (json:"-", not serializable).
    // Mutually exclusive with ServerConfig.ServerHooks — if Go hooks are set, command hooks must not be configured.
    BeforeQueryHooks []BeforeQueryHookEntry `json:"-"`
    AfterQueryHooks  []AfterQueryHookEntry  `json:"-"`
}

// ServerConfig embeds Config and adds server-only fields for CLI mode.
type ServerConfig struct {
    Config
    Connection  ConnectionConfig  `json:"connection"`
    Server      ServerSettings    `json:"server"`
    Logging     LoggingConfig     `json:"logging"`
    ServerHooks ServerHooksConfig `json:"server_hooks"`
}

type ConnectionConfig struct {
    Host    string `json:"host"`
    Port    int    `json:"port"`
    DBName  string `json:"dbname"`
    SSLMode string `json:"sslmode"`
}

type PoolConfig struct {
    MaxConns        int    `json:"max_conns"`
    MinConns        int    `json:"min_conns"`
    MaxConnLifetime string `json:"max_conn_lifetime"`
    MaxConnIdleTime string `json:"max_conn_idle_time"`
    HealthCheckPeriod string `json:"health_check_period"`
}

type ServerSettings struct {
    Port               int    `json:"port"`
    HealthCheckEnabled bool   `json:"health_check_enabled"`
    HealthCheckPath    string `json:"health_check_path"`
}

type LoggingConfig struct {
    Level  string `json:"level"`   // debug, info, warn, error
    Format string `json:"format"`  // json, text
    Output string `json:"output"`  // stdout, or file path
}

type ProtectionConfig struct {
    AllowSet                bool `json:"allow_set"`
    AllowDrop               bool `json:"allow_drop"`
    AllowTruncate           bool `json:"allow_truncate"`
    AllowDo                 bool `json:"allow_do"`
    AllowCopyFrom           bool `json:"allow_copy_from"`
    AllowCopyTo             bool `json:"allow_copy_to"`
    AllowCreateFunction     bool `json:"allow_create_function"`
    AllowPrepare            bool `json:"allow_prepare"`
    AllowDeleteWithoutWhere bool `json:"allow_delete_without_where"`
    AllowUpdateWithoutWhere bool `json:"allow_update_without_where"`
    AllowAlterSystem        bool `json:"allow_alter_system"`
    AllowMerge              bool `json:"allow_merge"`
    AllowGrantRevoke        bool `json:"allow_grant_revoke"`
    AllowManageRoles        bool `json:"allow_manage_roles"`
    AllowCreateExtension    bool `json:"allow_create_extension"`
    AllowLockTable          bool `json:"allow_lock_table"`
    AllowListenNotify       bool `json:"allow_listen_notify"`
    AllowMaintenance        bool `json:"allow_maintenance"`
    AllowDDL                bool `json:"allow_ddl"`
    AllowDiscard            bool `json:"allow_discard"`
    AllowComment            bool `json:"allow_comment"`
    AllowCreateTrigger      bool `json:"allow_create_trigger"`
    AllowCreateRule         bool `json:"allow_create_rule"`
}

type QueryConfig struct {
    DefaultTimeoutSeconds        int            `json:"default_timeout_seconds"`
    ListTablesTimeoutSeconds     int            `json:"list_tables_timeout_seconds"`
    DescribeTableTimeoutSeconds  int            `json:"describe_table_timeout_seconds"`
    MaxSQLLength                 int            `json:"max_sql_length"`
    MaxResultLength              int            `json:"max_result_length"`
    TimeoutRules                 []TimeoutRule  `json:"timeout_rules"`
}

type TimeoutRule struct {
    Pattern        string `json:"pattern"`
    TimeoutSeconds int    `json:"timeout_seconds"`
}

type ErrorPromptRule struct {
    Pattern string `json:"pattern"`
    Message string `json:"message"`
}

type SanitizationRule struct {
    Pattern     string `json:"pattern"`
    Replacement string `json:"replacement"`
    Description string `json:"description"`
}

type ServerHooksConfig struct {
    BeforeQuery []HookEntry `json:"before_query"`
    AfterQuery  []HookEntry `json:"after_query"`
}

type HookEntry struct {
    Pattern        string   `json:"pattern"`
    Command        string   `json:"command"`
    Args           []string `json:"args"`
    TimeoutSeconds int      `json:"timeout_seconds"`
}

// --- Library mode hook interfaces ---
// These are used when consumers embed pgmcp as a Go library.
// They avoid JSON serialization and work with native Go types.
// Library hooks and command hooks (via WithServerHooks option) are mutually exclusive —
// if Go hooks are set, command hooks must not be configured.

// BeforeQueryHook can inspect and modify queries before execution.
type BeforeQueryHook interface {
    // Run receives the SQL query string. Returns the (possibly modified) query.
    // Return error to reject the query (error message is returned to caller).
    Run(ctx context.Context, query string) (string, error)
}

// AfterQueryHook can inspect and modify results after execution.
type AfterQueryHook interface {
    // Run receives the query result as a Go struct (no JSON serialization).
    // Returns the (possibly modified) result.
    // Return error to reject the result (error message is returned to caller).
    Run(ctx context.Context, result *QueryOutput) (*QueryOutput, error)
}

// BeforeQueryHookEntry wraps a BeforeQueryHook with metadata.
type BeforeQueryHookEntry struct {
    Name    string            // Descriptive name for logging and error messages
    Timeout time.Duration     // Per-hook timeout; 0 = use default_hook_timeout_seconds
    Hook    BeforeQueryHook
}

// AfterQueryHookEntry wraps an AfterQueryHook with metadata.
type AfterQueryHookEntry struct {
    Name    string
    Timeout time.Duration
    Hook    AfterQueryHook
}
```

The `Config` struct includes Go hook fields (`BeforeQueryHooks`/`AfterQueryHooks`) for library mode. Command-based hooks (`ServerHooks`) are in `ServerConfig` only — the CLI extracts `ServerConfig.ServerHooks` and passes it to the hook runner separately. Go hooks and command hooks are mutually exclusive.

**Config loading logic** (internal to `pgmcp.go` or `cmd/`):
1. Check `GOPGMCP_CONFIG_PATH` env var → use that path
2. Otherwise use `<cwd>/.gopgmcp/config.json`
3. Parse JSON → `ServerConfig` struct (CLI mode) or `Config` struct (library mode via `New()`)
4. Validate: compile all regex patterns, check required fields, check timeout values > 0
5. Panic with descriptive message on validation failure — designed to catch incorrect settings at startup (see config validation philosophy below)

**Config defaults** (applied before validation, when fields are zero-value):
- `protection.*` → all `false` (Go zero-value = blocked, safe default; set to `true` to allow specific operations)
- `query.max_sql_length` → `100000` (when 0; prevents excessively large SQL from consuming memory)
- `query.max_result_length` → `100000` (when 0; cannot be disabled — there is no "no limit" option)

**Config validation (base Config, runs in `New()`)** — panics on failure:
- `pool.max_conns` must be > 0 — a zero-capacity semaphore would deadlock all queries
- `query.default_timeout_seconds` must be > 0 — no default, user must explicitly set this
- `query.list_tables_timeout_seconds` must be > 0 — no default, user must explicitly set this
- `query.describe_table_timeout_seconds` must be > 0 — no default, user must explicitly set this
- `query.max_sql_length` must be > 0 (guaranteed after defaults, but explicit validation for safety)
- `query.max_result_length` must be > 0 (guaranteed after defaults, but explicit validation for safety)
- `default_hook_timeout_seconds` must be > 0 if any hooks are configured (server_hooks or Go hooks) — no default, user must explicitly set this
- All regex patterns must compile successfully
- All per-hook and per-rule timeout values must be > 0

**ServerConfig validation (additional, runs in CLI mode only)** — panics on failure:
- `server.port` must be specified and > 0 — validated in `runServe()` with panic
- `server.health_check_path` must be non-empty if `server.health_check_enabled` is true — validated in `runServe()` with returned error (not panic)
- If both Go hooks (`Config.BeforeQueryHooks`/`Config.AfterQueryHooks`) and command hooks (`ServerConfig.ServerHooks.BeforeQuery`/`ServerConfig.ServerHooks.AfterQuery`) are configured, panic — they are mutually exclusive
- `config.DefaultHookTimeoutSeconds` must be > 0 if any command hooks are configured in `ServerConfig.ServerHooks`

**Config validation panics intentionally.** Both CLI and library mode initialize at application startup. Missing/invalid config should crash immediately rather than produce subtle runtime failures. Library users call `New()` during initialization, so panics are caught at startup. This philosophy applies to all validation across the entire codebase: `New()` validates all config fields relevant to the library API before proceeding, and all internal package constructors (`hooks.NewRunner`, `sanitize.NewSanitizer`, `errprompt.NewMatcher`, `timeout.NewManager`) panic on invalid config (e.g., invalid regex patterns). None of these constructors return errors for configuration issues — config problems are always panics. Only runtime failures (e.g., cannot connect to database) return errors.

---

## Phase 2: Internal Packages

### 2.1 Protection Checker

**File: `internal/protection/protection.go`**

Each internal package defines its own config type to avoid circular imports with the parent `pgmcp` package. The `pgmcp` package maps its config to internal configs when constructing components.

```go
package protection

// Config is the protection checker's own config type.
// The pgmcp package maps ProtectionConfig + Config.ReadOnly → this.
type Config struct {
    AllowSet                bool
    AllowDrop               bool
    AllowTruncate           bool
    AllowDo                 bool
    AllowCopyFrom           bool
    AllowCopyTo             bool
    AllowCreateFunction     bool
    AllowPrepare            bool
    AllowDeleteWithoutWhere bool
    AllowUpdateWithoutWhere bool
    AllowAlterSystem        bool
    AllowMerge              bool
    AllowGrantRevoke        bool
    AllowManageRoles        bool
    AllowCreateExtension    bool
    AllowLockTable          bool
    AllowListenNotify       bool
    AllowMaintenance        bool
    AllowDDL                bool
    AllowDiscard            bool
    AllowComment            bool
    AllowCreateTrigger      bool
    AllowCreateRule         bool
    ReadOnly                bool
}

type Checker struct {
    config Config
}

func NewChecker(config Config) *Checker

// Check parses SQL with pg_query_go and walks the AST.
// Returns nil if allowed, descriptive error if blocked.
// Error messages are descriptive, including the statement type and reason for blocking.
func (c *Checker) Check(sql string) error
```

**AST walking logic — recursive to catch DML inside CTEs:**

The checker uses recursive AST walking so that protection rules apply to DML statements inside CTEs (e.g., `WITH deleted AS (DELETE FROM users RETURNING *) SELECT * FROM deleted`). PostgreSQL allows INSERT, UPDATE, DELETE inside CTEs, and these must be checked just like top-level statements.

The `Check` method handles multi-statement detection (top-level only), then delegates to `checkNode` for recursive protection checking. `checkNode` first extracts and recurses into any CTEs attached to the node, then applies protection rules to the node itself.

```go
func (c *Checker) Check(sql string) error {
    result, err := pg_query.Parse(sql)
    if err != nil {
        return fmt.Errorf("SQL parse error: %w", err)
    }

    if len(result.Stmts) == 0 {
        return fmt.Errorf("SQL parse error: empty query")
    }

    // Multi-statement detection — always enforced, cannot be toggled off.
    // This catches "SELECT 1; DROP TABLE users" before any other check.
    if len(result.Stmts) > 1 {
        return fmt.Errorf("multi-statement queries are not allowed: found %d statements", len(result.Stmts))
    }

    for _, rawStmt := range result.Stmts {
        if err := c.checkNode(rawStmt.Stmt); err != nil {
            return err
        }
    }
    return nil
}

// checkNode recursively checks a single AST node and its CTEs against protection rules.
func (c *Checker) checkNode(node *pg_query.Node) error {
    if node == nil {
        return nil
    }

    // First, recurse into CTEs attached to this node.
    // SELECT, INSERT, UPDATE, DELETE can all have WITH clauses containing DML.
    if err := c.checkCTEs(node); err != nil {
        return err
    }

    // Then check the node itself against protection rules.
    switch n := node.Node.(type) {
    case *pg_query.Node_VariableSetStmt:
        varSetStmt := n.VariableSetStmt

        // readOnly: block RESET ALL and RESET default_transaction_read_only
        // (VAR_RESET / VAR_RESET_ALL kinds)
        if c.config.ReadOnly {
            if varSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET_ALL {
                return fmt.Errorf("RESET ALL is blocked in read-only mode: could disable read-only transaction setting")
            }
            if varSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET &&
                isTransactionReadOnlyVar(varSetStmt.Name) {
                return fmt.Errorf("RESET %s is blocked in read-only mode", varSetStmt.Name)
            }
            if isTransactionReadOnlyVar(varSetStmt.Name) {
                return fmt.Errorf("SET %s is blocked in read-only mode: cannot change transaction read-only setting", varSetStmt.Name)
            }
        }
        if !c.config.AllowSet {
            switch varSetStmt.Kind {
            case pg_query.VariableSetKind_VAR_RESET_ALL:
                return fmt.Errorf("RESET ALL is not allowed")
            case pg_query.VariableSetKind_VAR_RESET:
                return fmt.Errorf("RESET statements are not allowed: RESET %s", varSetStmt.Name)
            default:
                return fmt.Errorf("SET statements are not allowed: SET %s", varSetStmt.Name)
            }
        }

    case *pg_query.Node_DropStmt:
        if !c.config.AllowDrop {
            return fmt.Errorf("DROP statements are not allowed")
        }

    case *pg_query.Node_DropdbStmt:
        // DROP DATABASE is a separate AST node from generic DROP.
        if !c.config.AllowDrop {
            return fmt.Errorf("DROP DATABASE is not allowed")
        }

    case *pg_query.Node_TruncateStmt:
        if !c.config.AllowTruncate {
            return fmt.Errorf("TRUNCATE statements are not allowed")
        }

    case *pg_query.Node_DoStmt:
        if !c.config.AllowDo {
            return fmt.Errorf("DO $$ blocks are not allowed: DO blocks can execute arbitrary SQL bypassing protection checks")
        }

    case *pg_query.Node_DeleteStmt:
        if !c.config.AllowDeleteWithoutWhere && n.DeleteStmt.WhereClause == nil {
            return fmt.Errorf("DELETE without WHERE clause is not allowed")
        }

    case *pg_query.Node_UpdateStmt:
        if !c.config.AllowUpdateWithoutWhere && n.UpdateStmt.WhereClause == nil {
            return fmt.Errorf("UPDATE without WHERE clause is not allowed")
        }

    case *pg_query.Node_MergeStmt:
        if !c.config.AllowMerge {
            return fmt.Errorf("MERGE statements are not allowed: MERGE can perform INSERT, UPDATE, and DELETE operations bypassing individual DML protection rules")
        }

    case *pg_query.Node_CopyStmt:
        if !c.config.AllowCopyFrom && n.CopyStmt.IsFrom {
            return fmt.Errorf("COPY FROM is not allowed")
        }
        if !c.config.AllowCopyTo && !n.CopyStmt.IsFrom {
            return fmt.Errorf("COPY TO is not allowed: can export/exfiltrate data from tables")
        }

    case *pg_query.Node_CreateFunctionStmt:
        // Blocks both CREATE FUNCTION and CREATE PROCEDURE.
        // These can create server-side code containing arbitrary SQL that
        // bypasses protection checks when called, similar to DO blocks.
        if !c.config.AllowCreateFunction {
            if n.CreateFunctionStmt.IsProcedure {
                return fmt.Errorf("CREATE PROCEDURE is not allowed: can contain arbitrary SQL bypassing protection checks")
            }
            return fmt.Errorf("CREATE FUNCTION is not allowed: can contain arbitrary SQL bypassing protection checks")
        }

    case *pg_query.Node_PrepareStmt:
        // PREPARE creates session-level prepared statements that persist across
        // transactions. A subsequent EXECUTE can run the prepared content,
        // bypassing protection checks on the prepared SQL.
        if !c.config.AllowPrepare {
            return fmt.Errorf("PREPARE statements are not allowed: prepared statements can be executed later bypassing protection checks")
        }

    case *pg_query.Node_ExplainStmt:
        // Always recurse into the inner statement. EXPLAIN ANALYZE actually
        // executes the query, so "EXPLAIN ANALYZE DELETE FROM users" must be
        // blocked when DELETE-without-WHERE is blocked. Even plain EXPLAIN
        // is checked — the inner statement's protections still apply.
        // Note: Only ExplainableStmts (SELECT, INSERT, UPDATE, DELETE, MERGE) can appear
        // inside EXPLAIN. Non-explainable statements (DROP, TRUNCATE, etc.) produce parse
        // errors before reaching this code. Verified in explain_parse_test.go.
        if n.ExplainStmt.Query != nil {
            if err := c.checkNode(n.ExplainStmt.Query); err != nil {
                return err
            }
        }

    case *pg_query.Node_AlterSystemStmt:
        // ALTER SYSTEM modifies postgresql.auto.conf — can change any server-level
        // parameter including shared_preload_libraries, archive_command, ssl, etc.
        // Requires superuser, but dev environments often connect as superuser.
        if !c.config.AllowAlterSystem {
            return fmt.Errorf("ALTER SYSTEM is not allowed: can modify server-level configuration (shared_preload_libraries, archive_command, ssl, etc.)")
        }

    case *pg_query.Node_GrantStmt:
        if !c.config.AllowGrantRevoke {
            if n.GrantStmt.IsGrant {
                return fmt.Errorf("GRANT statements are not allowed: can modify database permissions")
            }
            return fmt.Errorf("REVOKE statements are not allowed: can modify database permissions")
        }

    case *pg_query.Node_GrantRoleStmt:
        if !c.config.AllowGrantRevoke {
            if n.GrantRoleStmt.IsGrant {
                return fmt.Errorf("GRANT ROLE is not allowed: can modify role memberships")
            }
            return fmt.Errorf("REVOKE ROLE is not allowed: can modify role memberships")
        }

    case *pg_query.Node_CreateRoleStmt:
        if !c.config.AllowManageRoles {
            return fmt.Errorf("CREATE ROLE/USER is not allowed: can create database roles with privileges")
        }

    case *pg_query.Node_AlterRoleStmt:
        if !c.config.AllowManageRoles {
            return fmt.Errorf("ALTER ROLE/USER is not allowed: can modify role privileges including SUPERUSER")
        }

    case *pg_query.Node_AlterRoleSetStmt:
        // ALTER USER testuser SET search_path = 'public' generates AlterRoleSetStmt, not AlterRoleStmt.
        if !c.config.AllowManageRoles {
            return fmt.Errorf("ALTER ROLE/USER is not allowed: can modify role privileges including SUPERUSER")
        }

    case *pg_query.Node_DropRoleStmt:
        if !c.config.AllowManageRoles {
            return fmt.Errorf("DROP ROLE/USER is not allowed: can delete database roles")
        }

    case *pg_query.Node_CreateExtensionStmt:
        if !c.config.AllowCreateExtension {
            return fmt.Errorf("CREATE EXTENSION is not allowed: can load arbitrary server-side code into PostgreSQL")
        }

    case *pg_query.Node_LockStmt:
        if !c.config.AllowLockTable {
            return fmt.Errorf("LOCK TABLE is not allowed: can acquire exclusive locks causing deadlocks or denial of service")
        }

    case *pg_query.Node_ListenStmt:
        if !c.config.AllowListenNotify {
            return fmt.Errorf("LISTEN is not allowed: can be used for side-channel communication between sessions")
        }

    case *pg_query.Node_NotifyStmt:
        if !c.config.AllowListenNotify {
            return fmt.Errorf("NOTIFY is not allowed: can send arbitrary payloads to listening sessions")
        }

    case *pg_query.Node_VacuumStmt:
        // Covers both VACUUM and standalone ANALYZE (they share the same AST node in PostgreSQL).
        if !c.config.AllowMaintenance {
            return fmt.Errorf("VACUUM/ANALYZE is not allowed: maintenance commands can acquire heavy locks and cause significant I/O load")
        }

    case *pg_query.Node_ClusterStmt:
        if !c.config.AllowMaintenance {
            return fmt.Errorf("CLUSTER is not allowed: acquires ACCESS EXCLUSIVE lock and rewrites the entire table")
        }

    case *pg_query.Node_ReindexStmt:
        if !c.config.AllowMaintenance {
            return fmt.Errorf("REINDEX is not allowed: can acquire ACCESS EXCLUSIVE lock on tables and indexes")
        }

    case *pg_query.Node_CreateStmt:
        // CREATE TABLE
        if !c.config.AllowDDL {
            return fmt.Errorf("CREATE TABLE is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_AlterTableStmt:
        if !c.config.AllowDDL {
            return fmt.Errorf("ALTER TABLE is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_IndexStmt:
        // CREATE INDEX
        if !c.config.AllowDDL {
            return fmt.Errorf("CREATE INDEX is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_CreateSchemaStmt:
        if !c.config.AllowDDL {
            return fmt.Errorf("CREATE SCHEMA is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_ViewStmt:
        // CREATE VIEW / CREATE OR REPLACE VIEW
        if !c.config.AllowDDL {
            return fmt.Errorf("CREATE VIEW is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_CreateSeqStmt:
        if !c.config.AllowDDL {
            return fmt.Errorf("CREATE SEQUENCE is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_CreateTableAsStmt:
        // CREATE TABLE ... AS SELECT / CREATE MATERIALIZED VIEW
        if !c.config.AllowDDL {
            return fmt.Errorf("CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_AlterSeqStmt:
        if !c.config.AllowDDL {
            return fmt.Errorf("ALTER SEQUENCE is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_RenameStmt:
        // ALTER ... RENAME TO (tables, columns, indexes, etc.)
        if !c.config.AllowDDL {
            return fmt.Errorf("RENAME is not allowed: DDL operations are blocked")
        }

    case *pg_query.Node_DiscardStmt:
        if !c.config.AllowDiscard {
            return fmt.Errorf("DISCARD is not allowed: resets session state including prepared statements and temporary tables")
        }

    case *pg_query.Node_CommentStmt:
        if !c.config.AllowComment {
            return fmt.Errorf("COMMENT ON is not allowed: modifies database object metadata")
        }

    case *pg_query.Node_CreateTrigStmt:
        if !c.config.AllowCreateTrigger {
            return fmt.Errorf("CREATE TRIGGER is not allowed: triggers execute arbitrary function calls on every DML operation, bypassing protection checks")
        }

    case *pg_query.Node_RuleStmt:
        if !c.config.AllowCreateRule {
            return fmt.Errorf("CREATE RULE is not allowed: rules rewrite queries at the parser level, can silently transform statements and bypass protection checks")
        }

    case *pg_query.Node_RefreshMatViewStmt:
        if !c.config.AllowMaintenance {
            return fmt.Errorf("REFRESH MATERIALIZED VIEW is not allowed: can acquire ACCESS EXCLUSIVE lock (without CONCURRENTLY) and cause significant I/O load")
        }

    case *pg_query.Node_AlterExtensionStmt:
        // ALTER EXTENSION ... UPDATE (e.g., ALTER EXTENSION foo UPDATE TO '2.0')
        if !c.config.AllowCreateExtension {
            return fmt.Errorf("ALTER EXTENSION is not allowed: can update extensions, loading new server-side code")
        }

    case *pg_query.Node_AlterExtensionContentsStmt:
        // ALTER EXTENSION ... ADD/DROP (e.g., ALTER EXTENSION foo ADD TABLE bar)
        if !c.config.AllowCreateExtension {
            return fmt.Errorf("ALTER EXTENSION is not allowed: can modify extension contents")
        }

    case *pg_query.Node_TransactionStmt:
        // Transaction control statements are always blocked — each query runs in its own
        // managed transaction with AfterQuery hooks running before commit. Allowing raw
        // transaction control would interfere with the pipeline's transaction management
        // and could bypass AfterQuery hook guardrails.
        //
        // Exception: in read-only mode, we give more specific error messages for
        // BEGIN READ WRITE attempts before the general block.
        if c.config.ReadOnly {
            txStmt := n.TransactionStmt
            for _, opt := range txStmt.Options {
                if defElem, ok := opt.Node.(*pg_query.Node_DefElem); ok {
                    if defElem.DefElem.Defname == "transaction_read_only" {
                        // In pg_query_go v6, the arg is AConst with Ival (not Node_Integer).
                        if aconst, ok := defElem.DefElem.Arg.Node.(*pg_query.Node_AConst); ok {
                            if ival, ok := aconst.AConst.Val.(*pg_query.A_Const_Ival); ok {
                                if ival.Ival.Ival == 0 { // 0 = false = READ WRITE
                                    return fmt.Errorf("BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction")
                                }
                            }
                        }
                    }
                }
            }
        }
        return fmt.Errorf("transaction control statements are not allowed: each query runs in a managed transaction with AfterQuery hooks as guardrails")
    }
    return nil
}

// checkCTEs extracts the WITH clause from a node (if any) and recursively
// checks each CTE's subquery. SELECT, INSERT, UPDATE, DELETE can all carry
// WITH clauses, and CTEs can contain DML that must be protection-checked.
func (c *Checker) checkCTEs(node *pg_query.Node) error {
    var withClause *pg_query.WithClause
    switch n := node.Node.(type) {
    case *pg_query.Node_SelectStmt:
        withClause = n.SelectStmt.WithClause
    case *pg_query.Node_InsertStmt:
        withClause = n.InsertStmt.WithClause
    case *pg_query.Node_UpdateStmt:
        withClause = n.UpdateStmt.WithClause
    case *pg_query.Node_DeleteStmt:
        withClause = n.DeleteStmt.WithClause
    case *pg_query.Node_MergeStmt:
        withClause = n.MergeStmt.WithClause
    }
    if withClause == nil {
        return nil
    }
    for _, cte := range withClause.Ctes {
        cteNode, ok := cte.Node.(*pg_query.Node_CommonTableExpr)
        if !ok {
            continue
        }
        // Recursively check the CTE's subquery — this handles nested CTEs
        // (a CTE whose subquery itself has a WITH clause) and DML inside CTEs.
        if err := c.checkNode(cteNode.CommonTableExpr.Ctequery); err != nil {
            return err
        }
    }
    return nil
}
```

Helper:
```go
func isTransactionReadOnlyVar(name string) bool {
    return name == "default_transaction_read_only" || name == "transaction_read_only"
}
```

### 2.2 Hook Runner

**File: `internal/hooks/hooks.go`**

```go
package hooks

// Config is the hook runner's own config type.
// The pgmcp package maps ServerHooksConfig + DefaultHookTimeoutSeconds → this, converting seconds to time.Duration.
type Config struct {
    DefaultTimeout time.Duration
    BeforeQuery    []HookEntry
    AfterQuery     []HookEntry
}

type HookEntry struct {
    Pattern string
    Command string
    Args    []string
    Timeout time.Duration // 0 means use DefaultTimeout
}

type BeforeQueryResult struct {
    Accept        bool   `json:"accept"`
    ModifiedQuery string `json:"modified_query,omitempty"`
    ErrorMessage  string `json:"error_message,omitempty"`
}

type AfterQueryResult struct {
    Accept         bool   `json:"accept"`
    ModifiedResult string `json:"modified_result,omitempty"`
    ErrorMessage   string `json:"error_message,omitempty"`
}

type compiledHook struct {
    pattern *regexp.Regexp
    command string
    args    []string
    timeout time.Duration
}

type Runner struct {
    beforeQuery    []compiledHook
    afterQuery     []compiledHook
    defaultTimeout time.Duration
    logger         zerolog.Logger
}

func NewRunner(config Config, logger zerolog.Logger) *Runner
// Compiles all regex patterns, panics on invalid regex or invalid config.
// For each hook: if Timeout > 0, uses that; otherwise falls back to config.DefaultTimeout.
// Panics if config.DefaultTimeout == 0 and any hook exists.

// HasAfterQueryHooks returns true if any AfterQuery hooks are configured.
// Used by the query pipeline to skip JSON serialization round-trip when no hooks exist.
func (r *Runner) HasAfterQueryHooks() bool

// RunBeforeQuery runs matching BeforeQuery hooks in middleware chain.
// Returns the (possibly modified) query string.
// If any hook rejects, returns error with hook's error_message.
// If any hook crashes/times out, logs error and continues.
func (r *Runner) RunBeforeQuery(ctx context.Context, query string) (string, error)

// RunAfterQuery runs matching AfterQuery hooks in middleware chain.
// Returns the (possibly modified) result string.
func (r *Runner) RunAfterQuery(ctx context.Context, result string) (string, error)
```

**Execution logic for a single hook:**
```go
func (r *Runner) executeHook(ctx context.Context, hook compiledHook, input string) ([]byte, error) {
    ctx, cancel := context.WithTimeout(ctx, hook.timeout)
    defer cancel()

    // Command and args are passed separately — no shell interpretation.
    // exec.Command(name, args...) executes the binary directly.
    cmd := exec.CommandContext(ctx, hook.command, hook.args...)
    cmd.Stdin = strings.NewReader(input)

    // Capture stderr separately for logging. Stdout is the JSON response.
    var stderr bytes.Buffer
    cmd.Stderr = &stderr

    output, err := cmd.Output()
    if err != nil {
        // Log stderr for debugging — stderr may contain diagnostic info from the hook.
        if stderr.Len() > 0 {
            r.logger.Warn().Str("command", hook.command).Str("stderr", stderr.String()).Msg("hook stderr output")
        }
        // Hooks are critical guardrails — any failure stops the pipeline.
        // This covers: non-zero exit code, crash, timeout (context deadline exceeded).
        if ctx.Err() == context.DeadlineExceeded {
            return nil, fmt.Errorf("hook timed out: %s", hook.command)
        }
        return nil, fmt.Errorf("hook failed (command: %s): %w", hook.command, err)
    }
    // Log stderr even on success — hooks may emit warnings or debug info.
    if stderr.Len() > 0 {
        r.logger.Debug().Str("command", hook.command).Str("stderr", stderr.String()).Msg("hook stderr output")
    }
    return output, nil
}
```

**Middleware chain logic (BeforeQuery):**

Hooks are critical guardrails. Any hook failure (crash, timeout, non-zero exit code, unparseable response) stops the entire pipeline and is treated as an error. This is the safe default — a failing hook means the guardrail cannot verify the query, so the query must be rejected.

```go
func (r *Runner) RunBeforeQuery(ctx context.Context, query string) (string, error) {
    current := query
    for _, hook := range r.beforeQuery {
        if !hook.pattern.MatchString(current) {
            continue
        }
        // executeHook returns error on crash, timeout, or non-zero exit code.
        // Any such error stops the entire pipeline.
        output, err := r.executeHook(ctx, hook, current)
        if err != nil {
            return "", fmt.Errorf("before_query hook error: %w", err)
        }

        // Unparseable response from hook is also a pipeline-stopping error.
        var result BeforeQueryResult
        if err := json.Unmarshal(output, &result); err != nil {
            return "", fmt.Errorf("before_query hook returned unparseable response (command: %s): %w", hook.command, err)
        }

        // Hook explicitly rejected the query.
        if !result.Accept {
            errMsg := "query rejected by hook"
            if result.ErrorMessage != "" {
                errMsg = result.ErrorMessage
            }
            return "", errors.New(errMsg)
        }
        if result.ModifiedQuery != "" {
            current = result.ModifiedQuery
        }
    }
    return current, nil
}
```

**AfterQuery follows the same pattern** but with `AfterQueryResult` and result string. Same error treatment — crash, timeout, non-zero exit, or unparseable response all stop the pipeline:

```go
func (r *Runner) RunAfterQuery(ctx context.Context, resultJSON string) (string, error) {
    current := resultJSON
    for _, hook := range r.afterQuery {
        if !hook.pattern.MatchString(current) {
            continue
        }
        output, err := r.executeHook(ctx, hook, current)
        if err != nil {
            return "", fmt.Errorf("after_query hook error: %w", err)
        }

        var result AfterQueryResult
        if err := json.Unmarshal(output, &result); err != nil {
            return "", fmt.Errorf("after_query hook returned unparseable response (command: %s): %w", hook.command, err)
        }

        if !result.Accept {
            errMsg := "result rejected by hook"
            if result.ErrorMessage != "" {
                errMsg = result.ErrorMessage
            }
            return "", errors.New(errMsg)
        }
        if result.ModifiedResult != "" {
            current = result.ModifiedResult
        }
    }
    return current, nil
}
```

### 2.3 Sanitization Engine

**File: `internal/sanitize/sanitize.go`**

```go
package sanitize

// Rule is the sanitizer's own rule type.
// The pgmcp package maps SanitizationRule → this.
type Rule struct {
    Pattern     string
    Replacement string
}

type compiledRule struct {
    pattern     *regexp.Regexp
    replacement string
}

type Sanitizer struct {
    rules []compiledRule
}

func NewSanitizer(rules []Rule) *Sanitizer
// Compiles all regex patterns, panics on invalid regex.

// SanitizeRows applies sanitization to each field value in the result rows.
// For JSONB/array fields (map[string]interface{}, []interface{}),
// recurses into primitive values.
func (s *Sanitizer) SanitizeRows(rows []map[string]interface{}) []map[string]interface{}
```

**Core logic:**

```go
// sanitizeValue applies all rules to a single value.
// Only applies to string values. For maps/slices, recurses.
func (s *Sanitizer) sanitizeValue(v interface{}) interface{} {
    switch val := v.(type) {
    case string:
        result := val
        for _, rule := range s.rules {
            result = rule.pattern.ReplaceAllString(result, rule.replacement)
        }
        return result
    case map[string]interface{}:
        for k, v := range val {
            val[k] = s.sanitizeValue(v)
        }
        return val
    case []interface{}:
        for i, item := range val {
            val[i] = s.sanitizeValue(item)
        }
        return val
    default:
        // Numeric, bool, nil, json.Number — return as-is.
        // json.Number (from UseNumber()) is type `string` underneath but does NOT
        // match `case string:` in Go type switches, so it correctly passes through.
        return v
    }
}
```

### 2.4 Error Prompt Matcher

**File: `internal/errprompt/errprompt.go`**

```go
package errprompt

// Rule is the error prompt matcher's own rule type.
// The pgmcp package maps ErrorPromptRule → this.
type Rule struct {
    Pattern string
    Message string
}

type compiledRule struct {
    pattern *regexp.Regexp
    message string
}

type Matcher struct {
    rules []compiledRule
}

func NewMatcher(rules []Rule) *Matcher
// Compiles all regex patterns, panics on invalid regex.

// Match checks error message against all rules (top to bottom).
// Returns all matching prompt messages joined with newline separators.
// Each prompt is displayed as its own paragraph.
// Returns empty string if no match.
func (m *Matcher) Match(errMsg string) string
```

### 2.5 Timeout Manager

**File: `internal/timeout/timeout.go`**

```go
package timeout

// Rule is the timeout manager's own rule type.
// The pgmcp package maps TimeoutRule → this, converting seconds to time.Duration.
type Rule struct {
    Pattern string
    Timeout time.Duration
}

// Config is the timeout manager's own config type.
type Config struct {
    DefaultTimeout time.Duration
    Rules          []Rule
}

type compiledRule struct {
    pattern *regexp.Regexp
    timeout time.Duration
}

type Manager struct {
    rules          []compiledRule
    defaultTimeout time.Duration
}

func NewManager(config Config) *Manager
// Compiles all regex patterns, panics on invalid regex.

// GetTimeout returns the timeout for the given SQL.
// First matching rule wins. Falls back to default.
func (m *Manager) GetTimeout(sql string) time.Duration
```

### 2.6 Interactive Configure

**File: `internal/configure/configure.go`**

```go
package configure

// Run runs the interactive configuration wizard.
// Reads existing config (if any), prompts for each field,
// writes updated config to the given path.
func Run(configPath string) error
```

**Logic:**
- Read existing config file if present
- For each scalar config field: display `Field (current: value):` prompt, read input, use current if empty
- For each array field (error_prompts, sanitization, timeout_rules, server_hooks.before_query, server_hooks.after_query):
  - Display current entries with indexes
  - Prompt: `[a]dd, [r]emove, [c]ontinue?`
  - On add: prompt for each sub-field one by one
  - On remove: prompt for index number
  - Loop back until user chooses continue
- Write JSON to config path (create dirs if needed)

---

## Phase 3: Core Engine (Public API)

### 3.1 PostgresMcp Struct

**File: `pgmcp.go`**

```go
package pgmcp

type PostgresMcp struct {
    config     Config
    pool       *pgxpool.Pool
    semaphore  chan struct{}
    protection *protection.Checker
    cmdHooks   *hooks.Runner          // command-based hooks (CLI mode, passed separately from ServerConfig.ServerHooks)
    goBeforeHooks []BeforeQueryHookEntry // Go function hooks (library mode)
    goAfterHooks  []AfterQueryHookEntry  // Go function hooks (library mode)
    sanitizer  *sanitize.Sanitizer
    errPrompts *errprompt.Matcher
    timeoutMgr *timeout.Manager
    logger     zerolog.Logger
}

// New creates a new PostgresMcp instance.
// connString is the PostgreSQL connection string (must include credentials).
// In library mode, connString is required — Config.Connection fields are ignored
// (the CLI is responsible for building connString from Config.Connection + prompted credentials).
// opts allows passing optional configuration such as command-based hooks (CLI mode).
// Panics on invalid config. Returns error only for runtime failures (e.g., pool creation).
func New(ctx context.Context, connString string, config Config, logger zerolog.Logger, opts ...Option) (*PostgresMcp, error)

// Option is a functional option for New().
type Option func(*options)

type options struct {
    serverHooks *ServerHooksConfig // command-based hooks, passed by CLI from ServerConfig.ServerHooks
}

// WithServerHooks passes command-based hook configuration to PostgresMcp.
// Mutually exclusive with Config.BeforeQueryHooks/AfterQueryHooks (Go hooks).
func WithServerHooks(hooks ServerHooksConfig) Option {
    return func(o *options) {
        o.serverHooks = &hooks
    }
}

// Close closes the connection pool. Accepts context for API forward-compatibility,
// but does not currently use it — pgxpool.Pool.Close() does not support context-based shutdown.
func (p *PostgresMcp) Close(ctx context.Context)
```

**Goroutine safety:** All exported methods of `PostgresMcp` (`Query`, `ListTables`, `DescribeTable`) are safe for concurrent use from multiple goroutines. All internal state is either:
- Immutable after construction: compiled regex patterns, config values, protection checker
- Goroutine-safe by design: `pgxpool.Pool`, `chan struct{}` semaphore, `zerolog.Logger`

Each query execution creates its own data (rows, maps) and the sanitizer operates on per-query data. No shared mutable state exists.

**New() logic:**
1. Validate config (panics on invalid config — catches incorrect settings at startup):
   - `connString` must be non-empty
   - `config.Pool.MaxConns` must be > 0 — a zero-capacity semaphore would deadlock all queries
   - `config.Query.DefaultTimeoutSeconds` must be > 0 — no default, user must explicitly set this
   - `config.Query.ListTablesTimeoutSeconds` must be > 0 — no default, user must explicitly set this
   - `config.Query.DescribeTableTimeoutSeconds` must be > 0 — no default, user must explicitly set this
   - `config.Query.MaxSQLLength` defaults to 100000 if 0; must be > 0 after default
   - `config.Query.MaxResultLength` defaults to 100000 if 0; must be > 0 after default
   - `config.DefaultHookTimeoutSeconds` must be > 0 if any hooks are configured (Go hooks) — no default, user must explicitly set this
   - All per-hook and per-rule `TimeoutSeconds` must be > 0
   - Pool duration strings (`MaxConnLifetime`, `MaxConnIdleTime`, `HealthCheckPeriod`) parsed via `time.ParseDuration()` — panics on invalid format (e.g., `"1h"` valid, `"forever"` invalid)
   - All regex patterns validated by internal constructors (they panic on invalid regex)
2. Configure `pgxpool.Config`: apply pool settings, set `DefaultQueryExecMode` to `pgx.QueryExecModeExec`. Pool duration fields (`MaxConnLifetime`, `MaxConnIdleTime`, `HealthCheckPeriod`) are strings in JSON config (e.g., `"1h"`, `"30m"`, `"1m"`) — parsed via `time.ParseDuration()` during config loading. If parsing fails, config validation panics with a descriptive error including the field name and invalid value.
3. Set `AfterConnect` hook on pool config to run session-level SET commands on each new connection:
   - If `config.ReadOnly`: run `SET default_transaction_read_only = on`.
   - If `config.Timezone` is non-empty: run `SET timezone = '<value>'`. Uses `pgx.Identifier{config.Timezone}.Sanitize()` — no, timezone values are not identifiers. Use parameterized query or string literal with validation. Actually, `SET timezone` does not support `$1` parameters. Use `fmt.Sprintf("SET timezone = '%s'", strings.ReplaceAll(config.Timezone, "'", "''"))` to safely escape single quotes, or simply rely on Postgres to reject invalid values.
   - Both can be combined in a single `AfterConnect` function.
4. Create `pgxpool.Pool` (returns error on connection failure — this is a runtime error, not a config error).
5. Create semaphore: `make(chan struct{}, config.Pool.MaxConns)` — bounds concurrent query pipelines.
6. Initialize all internal components, mapping pgmcp config types to internal package config types:
   - `protection.NewChecker(protection.Config{AllowSet: config.Protection.AllowSet, ..., AllowCopyFrom: config.Protection.AllowCopyFrom, AllowCopyTo: config.Protection.AllowCopyTo, AllowCreateFunction: config.Protection.AllowCreateFunction, AllowPrepare: config.Protection.AllowPrepare, AllowAlterSystem: config.Protection.AllowAlterSystem, AllowMerge: config.Protection.AllowMerge, AllowGrantRevoke: config.Protection.AllowGrantRevoke, AllowManageRoles: config.Protection.AllowManageRoles, AllowCreateExtension: config.Protection.AllowCreateExtension, AllowLockTable: config.Protection.AllowLockTable, AllowListenNotify: config.Protection.AllowListenNotify, AllowMaintenance: config.Protection.AllowMaintenance, AllowDDL: config.Protection.AllowDDL, AllowDiscard: config.Protection.AllowDiscard, AllowComment: config.Protection.AllowComment, AllowCreateTrigger: config.Protection.AllowCreateTrigger, AllowCreateRule: config.Protection.AllowCreateRule, ReadOnly: config.ReadOnly})`
   - If Go hooks are configured (library mode): store `config.BeforeQueryHooks` and `config.AfterQueryHooks` directly on the struct. No Runner needed.
   - If command hooks are configured (via `WithServerHooks` option): `hooks.NewRunner(hooks.Config{DefaultTimeout: time.Duration(config.DefaultHookTimeoutSeconds) * time.Second, ...}, logger)`. Panics if Go hooks are also configured (mutually exclusive).
   - If no hooks: `cmdHooks` is nil, Go hook slices are nil.
   - `sanitize.NewSanitizer(mapSanitizationRules(config.Sanitization))`
   - `errprompt.NewMatcher(mapErrorPromptRules(config.ErrorPrompts))`
   - `timeout.NewManager(timeout.Config{DefaultTimeout: time.Duration(config.Query.DefaultTimeoutSeconds) * time.Second, Rules: mapTimeoutRules(config.Query.TimeoutRules)})`
7. Return `*PostgresMcp`.

### 3.2 Query Tool

**File: `query.go`**

```go
type QueryInput struct {
    SQL string `json:"sql"`
}

type QueryOutput struct {
    Columns      []string                 `json:"columns"`
    Rows         []map[string]interface{} `json:"rows"`
    RowsAffected int64                    `json:"rows_affected"`
    Error        string                   `json:"error,omitempty"`
}

// Query executes the full query pipeline and returns only QueryOutput.
// All errors (Postgres errors, protection rejections, hook rejections, Go errors)
// are converted to output.Error. The error message is then evaluated against
// error_prompts patterns — any matching prompt messages are appended.
// This means callers only need to check output.Error, never a Go error.
func (p *PostgresMcp) Query(ctx context.Context, input QueryInput) *QueryOutput
```

**Full pipeline:**

```go
func (p *PostgresMcp) Query(ctx context.Context, input QueryInput) *QueryOutput {
    startTime := time.Now()
    sql := input.SQL

    // 1. Acquire semaphore (respects context cancellation to prevent deadlock)
    select {
    case p.semaphore <- struct{}{}:
    case <-ctx.Done():
        return p.handleError(fmt.Errorf("failed to acquire query slot: all %d connection slots are in use, context cancelled while waiting: %w", cap(p.semaphore), ctx.Err()))
    }
    defer func() { <-p.semaphore }()

    // 2. Check SQL length (before any processing — parsing, hooks, protection)
    if len(sql) > p.config.Query.MaxSQLLength {
        return p.handleError(fmt.Errorf("SQL query too long: %d bytes exceeds maximum of %d bytes", len(sql), p.config.Query.MaxSQLLength))
    }

    // 3. Run BeforeQuery hooks (middleware chain)
    var err error
    if len(p.goBeforeHooks) > 0 {
        sql, err = p.runGoBeforeHooks(ctx, sql)
    } else if p.cmdHooks != nil {
        sql, err = p.cmdHooks.RunBeforeQuery(ctx, sql)
    }
    if err != nil {
        return p.handleError(err)
    }

    // 4. Protection check (on potentially modified query)
    if err := p.protection.Check(sql); err != nil {
        return p.handleError(err)
    }

    // 5. Determine timeout
    timeout := p.timeoutMgr.GetTimeout(sql)
    queryCtx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    // 6. Acquire connection and execute in transaction
    conn, err := p.pool.Acquire(queryCtx)
    if err != nil {
        return p.handleError(err)
    }
    defer conn.Release()

    tx, err := conn.Begin(queryCtx)
    if err != nil {
        return p.handleError(err)
    }
    defer tx.Rollback(ctx) // use parent ctx, not queryCtx — if query timed out, queryCtx is cancelled and rollback would fail

    rows, err := tx.Query(queryCtx, sql)
    if err != nil {
        return p.handleError(err)
    }

    // 7. Collect results
    result, err := p.collectRows(rows)
    if err != nil {
        return p.handleError(err)
    }

    // 8. Detect read-only vs write statement from the parsed AST.
    // Since we enforce single-statement and have the parsed AST from protection check,
    // we can determine whether this is a read-only query (SELECT, EXPLAIN) or a write
    // (INSERT/UPDATE/DELETE/MERGE/etc.).
    isReadOnly := isReadOnlyStatement(sql)

    // 9. For read-only queries, rollback immediately (no commit needed).
    // This frees the transaction before running AfterQuery hooks.
    if isReadOnly {
        tx.Rollback(ctx) // explicit rollback for clarity, deferred rollback is no-op after this
    }

    // 10. AfterQuery hooks — run BEFORE commit for write queries.
    // This allows hooks to reject and trigger rollback for writes
    // (e.g., force-rollback if too many rows affected).
    var finalResult *QueryOutput
    if len(p.goAfterHooks) > 0 {
        // Go hooks (library mode): pass *QueryOutput directly, no JSON round-trip.
        // Preserves full Go type information and numeric precision.
        finalResult, err = p.runGoAfterHooks(ctx, result)
        if err != nil {
            // For write queries, deferred tx.Rollback() will undo the mutation.
            return p.handleError(err)
        }
    } else if p.cmdHooks != nil && p.cmdHooks.HasAfterQueryHooks() {
        // Command hooks (CLI mode): JSON serialize → hooks → deserialize.
        // Uses UseNumber() to preserve numeric precision through round-trip.
        resultJSON, err := json.Marshal(result)
        if err != nil {
            return p.handleError(err)
        }

        modifiedJSON, err := p.cmdHooks.RunAfterQuery(ctx, string(resultJSON))
        if err != nil {
            return p.handleError(err)
        }

        finalResult = &QueryOutput{}
        dec := json.NewDecoder(strings.NewReader(modifiedJSON))
        dec.UseNumber()
        if err := dec.Decode(finalResult); err != nil {
            return p.handleError(err)
        }
    } else {
        // No hooks: pass result through directly (no JSON round-trip).
        finalResult = result
    }

    // 11. For write queries, commit AFTER hooks have approved the result.
    // If we reach here, all AfterQuery hooks accepted. Deferred tx.Rollback() is no-op after commit.
    //
    // DESIGN NOTE: Commit uses queryCtx (not parent ctx) intentionally. The query timeout
    // covers the entire database operation lifecycle: query execution + commit. This means
    // if AfterQuery hooks consume significant time (they run with their own timeouts on the
    // parent ctx), the remaining queryCtx budget may be insufficient for commit. This is the
    // desired behavior — it ensures the entire pipeline (query + hooks + commit) completes
    // within the query timeout. If it doesn't, the commit fails and the transaction is rolled
    // back by the deferred tx.Rollback(ctx), providing a safety guarantee: no write persists
    // unless the full pipeline (including hooks) completes within the timeout. Users should
    // set query timeouts that account for both query execution and hook processing time.
    if !isReadOnly {
        if err := tx.Commit(queryCtx); err != nil {
            return p.handleError(err)
        }
    }

    // 12. Apply sanitization (per-field, recursive into JSONB/arrays)
    finalResult.Rows = p.sanitizer.SanitizeRows(finalResult.Rows)

    // 13. Apply max result length truncation in characters (keeps partial data — may be garbled JSON but still useful for agents)
    p.truncateIfNeeded(finalResult)

    // 14. Log successful query execution
    p.logger.Info().
        Str("sql", truncateForLog(sql, 200)).
        Dur("duration", time.Since(startTime)).
        Int("row_count", len(finalResult.Rows)).
        Int64("rows_affected", finalResult.RowsAffected).
        Msg("query executed")

    return finalResult
}
```

**isReadOnlyStatement helper** — uses pg_query_go to determine if a statement is read-only (SELECT, EXPLAIN without inner DML, SET, SHOW, etc.) vs write (INSERT, UPDATE, DELETE, MERGE, etc.). Since we already enforce single-statement queries, this only needs to check the top-level node type:

```go
// isReadOnlyStatement returns true if the SQL is a read-only statement.
// The SQL has already passed protection checks (single statement, parsed successfully).
// For read-only statements, we rollback the transaction immediately after collecting results
// (no commit needed). For write statements, we defer commit until after AfterQuery hooks.
func isReadOnlyStatement(sql string) bool {
    result, err := pg_query.Parse(sql)
    if err != nil || len(result.Stmts) == 0 {
        // If we can't parse (shouldn't happen — protection check already parsed),
        // assume write for safety (will attempt commit).
        return false
    }
    node := result.Stmts[0].Stmt
    switch node.Node.(type) {
    case *pg_query.Node_SelectStmt:
        return true
    case *pg_query.Node_ExplainStmt:
        return true
    case *pg_query.Node_VariableSetStmt:
        return true // SET/RESET — classified as read-only, so the transaction is rolled back.
        // Note: SET is blocked by default (AllowSet=false), so this case is only reached
        // when AllowSet=true. Even when allowed, SET has no lasting effect on the session
        // because PostgreSQL undoes SET within a rolled-back transaction. This is intentional:
        // session-level settings should be configured via Config.Timezone / Config.ReadOnly
        // (applied in AfterConnect), not via ad-hoc SET through the query tool.
    case *pg_query.Node_VariableShowStmt:
        return true // SHOW
    default:
        return false
    }
}
```

Note: This parses the SQL a second time (protection check already parsed it). This is acceptable since pg_query_go parsing is fast (~microseconds) and this avoids threading the parsed AST through the pipeline. If performance profiling shows this matters, the parsed result can be cached from the protection check.

**Go hook execution methods (library mode):**

```go
// runGoBeforeHooks runs Go-interface BeforeQuery hooks in middleware chain.
// Each hook receives the (possibly modified) query from the previous hook.
// Timeout is enforced per-hook: uses entry.Timeout if > 0, else config.DefaultHookTimeoutSeconds.
func (p *PostgresMcp) runGoBeforeHooks(ctx context.Context, sql string) (string, error) {
    for _, entry := range p.goBeforeHooks {
        timeout := entry.Timeout
        if timeout == 0 {
            timeout = time.Duration(p.config.DefaultHookTimeoutSeconds) * time.Second
        }
        hookCtx, cancel := context.WithTimeout(ctx, timeout)

        modified, err := entry.Hook.Run(hookCtx, sql)
        cancel()
        if err != nil {
            // Check if it was a timeout
            if hookCtx.Err() == context.DeadlineExceeded {
                return "", fmt.Errorf("before_query hook error: hook timed out (name: %s, timeout: %s)", entry.Name, timeout)
            }
            return "", fmt.Errorf("before_query hook error: hook rejected query (name: %s): %w", entry.Name, err)
        }
        sql = modified
    }
    return sql, nil
}

// runGoAfterHooks runs Go-interface AfterQuery hooks in middleware chain.
// Each hook receives the *QueryOutput directly (no JSON serialization).
// Timeout is enforced per-hook: uses entry.Timeout if > 0, else config.DefaultHookTimeoutSeconds.
func (p *PostgresMcp) runGoAfterHooks(ctx context.Context, result *QueryOutput) (*QueryOutput, error) {
    for _, entry := range p.goAfterHooks {
        timeout := entry.Timeout
        if timeout == 0 {
            timeout = time.Duration(p.config.DefaultHookTimeoutSeconds) * time.Second
        }
        hookCtx, cancel := context.WithTimeout(ctx, timeout)

        modified, err := entry.Hook.Run(hookCtx, result)
        cancel()
        if err != nil {
            if hookCtx.Err() == context.DeadlineExceeded {
                return nil, fmt.Errorf("after_query hook error: hook timed out (name: %s, timeout: %s)", entry.Name, timeout)
            }
            return nil, fmt.Errorf("after_query hook error: hook rejected result (name: %s): %w", entry.Name, err)
        }
        result = modified
    }
    return result, nil
}
```

**collectRows logic:**
```go
func (p *PostgresMcp) collectRows(rows pgx.Rows) (*QueryOutput, error) {
    defer rows.Close()

    fieldDescs := rows.FieldDescriptions()
    columns := make([]string, len(fieldDescs))
    for i, fd := range fieldDescs {
        columns[i] = fd.Name
    }

    resultRows := make([]map[string]interface{}, 0)
    for rows.Next() {
        values, err := rows.Values()
        if err != nil {
            return nil, err
        }
        row := make(map[string]interface{}, len(columns))
        for i, col := range columns {
            row[col] = convertValue(values[i])
        }
        resultRows = append(resultRows, row)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }

    // CommandTag provides rows affected for DML (INSERT/UPDATE/DELETE).
    // For SELECT, RowsAffected() returns the number of rows returned.
    rowsAffected := rows.CommandTag().RowsAffected()

    return &QueryOutput{Columns: columns, Rows: resultRows, RowsAffected: rowsAffected}, nil
}
```

**convertValue logic** — ensures all Postgres types are properly converted to JSON-friendly Go types. All types verified via `pgxtype_verification_test.go` integration tests against pgflock.

Handles (in switch order):
- `nil` → JSON null
- `time.Time` → ISO 8601 string (timestamptz, timestamp, date)
- `float32` → pass through; NaN/Inf → string "NaN"/"Infinity"/"-Infinity" (json.Marshal fails on these)
- `float64` → pass through; NaN/Inf → string "NaN"/"Infinity"/"-Infinity" (json.Marshal fails on these)
- `netip.Prefix` → string via `.String()` (verified: pgx v5 returns this for inet/cidr, NOT `net.IPNet`)
- `net.HardwareAddr` → string via `.String()` (macaddr/macaddr8; is `[]byte` underneath, json.Marshal would base64)
- `pgtype.Time` → formatted "HH:MM:SS" or "HH:MM:SS.ffffff" string (time without timezone)
- `pgtype.Interval` → human-readable string (e.g. `"1 year(s) 2 mon(s) 3 day(s) 4h5m6s"`)
- `pgtype.Numeric` → string (preserves full precision; checks InfinityModifier before MarshalJSON to prevent panic)
- `pgtype.Range[interface{}]` → formatted range string (e.g. `"[1,10)"`, `"empty"`)
- `pgtype.Point` → `"(x,y)"` format
- `pgtype.Line` → `"{A,B,C}"` format
- `pgtype.Lseg` → `"[(x1,y1),(x2,y2)]"` format
- `pgtype.Box` → `"(x1,y1),(x2,y2)"` format
- `pgtype.Path` → `"((x,y),(x,y))"` closed or `"[(x,y),(x,y)]"` open
- `pgtype.Polygon` → `"((x,y),(x,y),(x,y))"` format
- `pgtype.Circle` → `"<(x,y),r>"` format
- `pgtype.Bits` → bit string `"10101010"` (bit/varbit)
- `[16]byte` (UUID) → formatted UUID string
- `[]byte` → base64-encoded string (bytea, xml)
- `string` → pass through (money, timetz, char, varchar, text, enum, composite, tsvector, tsquery)
- `map[string]interface{}` → recursive convertValue on values (JSONB objects)
- `[]interface{}` → recursive convertValue on elements (JSONB arrays and Postgres arrays — arrays may contain typed elements like `[16]uint8` for uuid[])
- Other types (`int16`, `int32`, `int64`, `bool`, etc.) → pass through to json.Marshal

```go
func convertValue(v interface{}) interface{} {
    switch val := v.(type) {
    case nil:
        return nil
    case time.Time:
        return val.Format(time.RFC3339Nano)
    case float32:
        // float32 NaN/+Inf/-Inf breaks json.Marshal ("json: unsupported value").
        // Verified: real columns return float32 with these IEEE 754 special values.
        if math.IsNaN(float64(val)) {
            return "NaN"
        }
        if math.IsInf(float64(val), 1) {
            return "Infinity"
        }
        if math.IsInf(float64(val), -1) {
            return "-Infinity"
        }
        return val
    case float64:
        // float64 NaN/+Inf/-Inf breaks json.Marshal ("json: unsupported value").
        // Verified: double precision columns return float64 with these special values.
        if math.IsNaN(val) {
            return "NaN"
        }
        if math.IsInf(val, 1) {
            return "Infinity"
        }
        if math.IsInf(val, -1) {
            return "-Infinity"
        }
        return val
    case netip.Prefix:
        // pgx v5 returns netip.Prefix for inet/cidr columns (verified).
        return val.String()
    case net.HardwareAddr:
        // macaddr/macaddr8 columns. net.HardwareAddr is []byte underneath —
        // json.Marshal would base64 encode it. Use String() for human-readable
        // format: "08:00:2b:01:02:03" (6-byte) or "08:00:2b:01:02:03:04:05" (8-byte).
        return val.String()
    case pgtype.Time:
        // time without timezone. pgtype.Time has Microseconds (int64) and Valid (bool).
        // Microseconds since midnight. Format as "HH:MM:SS" or "HH:MM:SS.ffffff".
        // Note: timetz (time WITH timezone) returns as string, not pgtype.Time.
        if !val.Valid {
            return nil
        }
        us := val.Microseconds
        hours := us / 3_600_000_000
        us -= hours * 3_600_000_000
        minutes := us / 60_000_000
        us -= minutes * 60_000_000
        seconds := us / 1_000_000
        us -= seconds * 1_000_000
        if us > 0 {
            return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, us)
        }
        return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
    case pgtype.Interval:
        // pgtype.Interval has Microseconds (int64), Days (int32), Months (int32), Valid (bool).
        // All fields can be negative (e.g., '-3 days -2 hours', '1 month -5 days 3 hours').
        if !val.Valid {
            return nil
        }
        parts := []string{}
        if val.Months != 0 {
            years := val.Months / 12
            months := val.Months % 12
            if years != 0 {
                parts = append(parts, fmt.Sprintf("%d year(s)", years))
            }
            if months != 0 {
                parts = append(parts, fmt.Sprintf("%d mon(s)", months))
            }
        }
        if val.Days != 0 {
            parts = append(parts, fmt.Sprintf("%d day(s)", val.Days))
        }
        if val.Microseconds != 0 {
            // Use time.Duration for clean formatting of the time component.
            // Negative microseconds produce negative durations (e.g., "-2h0m0s").
            dur := time.Duration(val.Microseconds) * time.Microsecond
            parts = append(parts, dur.String())
        }
        if len(parts) == 0 {
            return "0"
        }
        return strings.Join(parts, " ")
    case pgtype.Numeric:
        // Always use string representation to preserve full precision.
        // float64 silently loses precision for high-precision numeric values
        // (e.g., financial data with many decimal places).
        // pgtype.Numeric does NOT have String() — use MarshalJSON() which returns
        // the number as text bytes (no quotes for valid numbers).
        if !val.Valid {
            return nil
        }
        if val.NaN {
            return "NaN"
        }
        // Must check InfinityModifier BEFORE calling MarshalJSON() —
        // MarshalJSON calls numberTextBytes() which does n.Int.String(),
        // and Int is nil for Infinity values, causing a nil pointer panic.
        if val.InfinityModifier == pgtype.Infinity {
            return "Infinity"
        }
        if val.InfinityModifier == pgtype.NegativeInfinity {
            return "-Infinity"
        }
        b, err := val.MarshalJSON()
        if err != nil {
            return nil // should not happen for valid, non-NaN, finite numerics
        }
        return string(b)
    case pgtype.Range[interface{}]:
        // Range types: int4range, int8range, numrange, tsrange, tstzrange, daterange.
        // pgx v5 returns pgtype.Range[interface{}] with QueryExecModeExec.
        // Format as Postgres text representation: "[1,10)", "(,5]", "empty", etc.
        if !val.Valid {
            return nil
        }
        if val.LowerType == pgtype.Empty {
            return "empty"
        }
        var sb strings.Builder
        if val.LowerType == pgtype.Inclusive {
            sb.WriteByte('[')
        } else {
            sb.WriteByte('(')
        }
        if val.LowerType != pgtype.Unbounded {
            sb.WriteString(fmt.Sprintf("%v", convertValue(val.Lower)))
        }
        sb.WriteByte(',')
        if val.UpperType != pgtype.Unbounded {
            sb.WriteString(fmt.Sprintf("%v", convertValue(val.Upper)))
        }
        if val.UpperType == pgtype.Inclusive {
            sb.WriteByte(']')
        } else {
            sb.WriteByte(')')
        }
        return sb.String()
    case pgtype.Point:
        if !val.Valid {
            return nil
        }
        return fmt.Sprintf("(%g,%g)", val.P.X, val.P.Y)
    case pgtype.Line:
        if !val.Valid {
            return nil
        }
        return fmt.Sprintf("{%g,%g,%g}", val.A, val.B, val.C)
    case pgtype.Lseg:
        if !val.Valid {
            return nil
        }
        return fmt.Sprintf("[(%g,%g),(%g,%g)]", val.P[0].X, val.P[0].Y, val.P[1].X, val.P[1].Y)
    case pgtype.Box:
        if !val.Valid {
            return nil
        }
        return fmt.Sprintf("(%g,%g),(%g,%g)", val.P[0].X, val.P[0].Y, val.P[1].X, val.P[1].Y)
    case pgtype.Path:
        if !val.Valid {
            return nil
        }
        points := make([]string, len(val.P))
        for i, p := range val.P {
            points[i] = fmt.Sprintf("(%g,%g)", p.X, p.Y)
        }
        joined := strings.Join(points, ",")
        if val.Closed {
            return "(" + joined + ")"
        }
        return "[" + joined + "]"
    case pgtype.Polygon:
        if !val.Valid {
            return nil
        }
        points := make([]string, len(val.P))
        for i, p := range val.P {
            points[i] = fmt.Sprintf("(%g,%g)", p.X, p.Y)
        }
        return "(" + strings.Join(points, ",") + ")"
    case pgtype.Circle:
        if !val.Valid {
            return nil
        }
        return fmt.Sprintf("<(%g,%g),%g>", val.P.X, val.P.Y, val.R)
    case pgtype.Bits:
        // bit(n) / varbit columns. Format as bit string "10101010".
        if !val.Valid {
            return nil
        }
        result := make([]byte, val.Len)
        for i := int32(0); i < val.Len; i++ {
            byteIdx := i / 8
            bitIdx := 7 - (i % 8)
            if val.Bytes[byteIdx]&(1<<uint(bitIdx)) != 0 {
                result[i] = '1'
            } else {
                result[i] = '0'
            }
        }
        return string(result)
    case [16]byte:
        // UUID
        return fmt.Sprintf("%x-%x-%x-%x-%x", val[0:4], val[4:6], val[6:8], val[8:10], val[10:16])
    case []byte:
        // Handles bytea and xml (both return as []uint8).
        // net.HardwareAddr is also []byte but matched by its own case above.
        // No JSON auto-parsing here — pgx with QueryExecModeExec returns JSONB as
        // parsed Go types (map[string]interface{}, []interface{}), never as []byte.
        // Binary data (bytea, xml) → base64-encoded string.
        // Note: xml columns return same Go type as bytea ([]uint8) — cannot distinguish.
        // XML content is base64 encoded. Users needing text should cast in SQL: v::text.
        return base64.StdEncoding.EncodeToString(val)
    case string:
        // Covers: money ("$1,234.56"), timetz ("10:30:00+05:30"), char, varchar, text,
        // enum, composite ("(a,b,c)"), tsvector, tsquery.
        // No JSON auto-parsing here — pgx with QueryExecModeExec returns JSONB as
        // parsed Go types (map[string]interface{}, []interface{}), never as raw string.
        // Auto-parsing would cause false positives: text columns containing JSON-like
        // strings (e.g., '{"key": "value"}') would be silently converted to maps.
        return val
    case map[string]interface{}:
        // JSONB objects. Recurse into values — nested values may need conversion.
        result := make(map[string]interface{}, len(val))
        for k, v := range val {
            result[k] = convertValue(v)
        }
        return result
    case []interface{}:
        // JSONB arrays and Postgres arrays (text[], int[], uuid[], etc.).
        // Must recurse — Postgres array elements may be typed values that need
        // conversion (e.g., uuid[] contains [16]uint8 elements, not strings).
        // For JSONB arrays, elements are already JSON-safe (string, float64, bool,
        // nil, map, []interface{}) so recursion is a no-op — no performance concern.
        // Note: pgx flattens 2D arrays into 1D []interface{}.
        result := make([]interface{}, len(val))
        for i, v := range val {
            result[i] = convertValue(v)
        }
        return result
    default:
        // int16 (smallint), int32 (integer/serial), int64 (bigint/bigserial),
        // bool (boolean) — all JSON-safe, pass through to json.Marshal.
        return val
    }
}
```

Additional imports required for convertValue:
```go
import (
    "math"         // float32/float64 NaN/Inf checks
    "net"          // net.HardwareAddr for macaddr/macaddr8
    "net/netip"    // netip.Prefix for inet/cidr
)
```

**JSONB handling (verified):** With `pgx.QueryExecModeExec`, pgx returns JSONB as parsed Go types (`map[string]interface{}`, `[]interface{}`) — NOT as raw `string` or `[]byte`. This was verified empirically via `pgxtype_verification_test.go`. Therefore, `convertValue` does NOT attempt JSON auto-parsing in the `string` or `[]byte` cases — doing so would cause false positives for text/varchar columns containing JSON-like strings (e.g., `'{"key": "value"}'` in a text column would be silently converted to a map). The `convertValue` cases for `map[string]interface{}` and `[]interface{}` recurse into values/elements, which is safe for JSONB (elements are already JSON-safe types, recursion is a no-op) and necessary for Postgres arrays (elements may be typed values like `[16]uint8` for uuid[]).

**JSONB numeric precision limitation (verified):** pgx internally parses JSONB numbers as `float64`, so large integers inside JSONB (e.g., `{"id": 9007199254740993}`) lose precision — `9007199254740993` becomes `9.007199254740992e+15`. This happens inside pgx before `convertValue` sees the data. The `UseNumber()` approach in `convertValue` only helps if JSONB were returned as `string`/`[]byte` (which it is not in practice). This is a known pgx limitation for JSONB — users needing exact large integers in JSONB should store them as strings in the JSON (e.g., `{"id": "9007199254740993"}`).

**[]byte / bytea / xml handling:** Binary data that is not valid JSON is encoded as base64, which is the standard representation of binary data in JSON. This applies to both bytea and xml columns — both return as `[]uint8` from pgx, indistinguishable by Go type. XML content is base64 encoded; users needing text should cast in SQL (`v::text`).

**pgx type verification (verified results from `pgxtype_verification_test.go`):**

The actual Go types returned by `rows.Values()` with `QueryExecModeExec` have been verified empirically against all Postgres types. Complete results:

| Postgres Type | Go Type | convertValue handling |
|---|---|---|
| `smallint` | `int16` | Pass through |
| `integer` / `serial` | `int32` | Pass through |
| `bigint` / `bigserial` | `int64` | Pass through (exact precision preserved) |
| `numeric` | `pgtype.Numeric` | String via MarshalJSON (NaN/Inf checks) |
| `real` | `float32` | Pass through; NaN/Inf → string |
| `double precision` | `float64` | Pass through; NaN/Inf → string |
| `money` | `string` | Pass through (e.g., `"$1,234.56"`) |
| `char(n)` | `string` | Pass through (space-padded) |
| `varchar(n)` / `text` | `string` | Pass through |
| `bytea` | `[]uint8` | base64 encode |
| `timestamptz` / `timestamp` / `date` | `time.Time` | Format as RFC3339Nano |
| `time` | `pgtype.Time` | Format as "HH:MM:SS" or "HH:MM:SS.ffffff" |
| `timetz` | `string` | Pass through (e.g., `"10:30:00+05:30"`) |
| `interval` | `pgtype.Interval` | Format as human-readable string |
| `boolean` | `bool` | Pass through |
| `uuid` | `[16]uint8` | Format as UUID string (uint8 = byte) |
| `inet` / `cidr` | `netip.Prefix` | `.String()` (e.g., `"192.168.1.1/24"`) |
| `macaddr` / `macaddr8` | `net.HardwareAddr` | `.String()` (e.g., `"08:00:2b:01:02:03"`) |
| `jsonb` (object) | `map[string]interface{}` | Recurse into values |
| `jsonb` (array) | `[]interface{}` | Recurse into elements |
| `jsonb` (null literal) | `nil` | Return nil |
| `jsonb` (scalar string) | `string` | Pass through |
| `jsonb` (scalar number) | `float64` | Pass through (NaN/Inf check) |
| `jsonb` (scalar bool) | `bool` | Pass through |
| `json` (object/array) | `map[string]interface{}` / `[]interface{}` | Same as JSONB |
| `text[]` / `int[]` / etc. | `[]interface{}` | Recurse into elements |
| `uuid[]` | `[]interface{}` | Recurse — elements are `[16]uint8` |
| 2D arrays | `[]interface{}` | Flattened to 1D by pgx |
| `enum` | `string` | Pass through |
| `int4range` / `int8range` / etc. | `pgtype.Range[interface{}]` | Format as `"[1,10)"` / `"empty"` |
| `point` | `pgtype.Point` | Format as `"(x,y)"` |
| `line` | `pgtype.Line` | Format as `"{A,B,C}"` |
| `lseg` | `pgtype.Lseg` | Format as `"[(x1,y1),(x2,y2)]"` |
| `box` | `pgtype.Box` | Format as `"(x1,y1),(x2,y2)"` |
| `path` | `pgtype.Path` | Closed: `"((x,y),...)"` Open: `"[(x,y),...]"` |
| `polygon` | `pgtype.Polygon` | Format as `"((x,y),(x,y),...)"` |
| `circle` | `pgtype.Circle` | Format as `"<(x,y),r>"` |
| `bit(n)` / `varbit` | `pgtype.Bits` | Format as bit string `"10101010"` |
| `tsvector` | `string` | Pass through |
| `tsquery` | `string` | Pass through |
| `xml` | `[]uint8` | base64 encode (same as bytea — cannot distinguish) |
| `composite type` | `string` | Pass through (e.g., `"(\"123 Main St\",Springfield,62701)"`) |
| `domain` | underlying type | Maps to base type (e.g., int domain → `int32`) |
| `NULL` (any type) | `nil` | Return nil |

**Key findings:**
- `net.IPNet` and `net.IP` are **NOT returned** by pgx v5 with QueryExecModeExec. Instead, `netip.Prefix` is used.
- `macaddr`/`macaddr8` return `net.HardwareAddr` (which is `[]byte`) — must be matched before the `[]byte` case to avoid base64 encoding.
- `time` (without timezone) returns `pgtype.Time`, but `timetz` (with timezone) returns `string` — different Go types for similar Postgres types.
- `json` and `jsonb` both return the same Go types (`map[string]interface{}`, `[]interface{}`, etc.) — no difference in behavior.
- Range types are generic: `pgtype.Range[interface{}]`. Lower/Upper elements are typed (e.g., `int32` for int4range, `pgtype.Numeric` for numrange) and need recursive conversion.
- Geometric types all have a `Valid` field that must be checked before formatting.
- `pgtype.Bits.Len` is `int32` — tracks exact bit count (important for varbit which may not be byte-aligned).
- 2D arrays are flattened to 1D `[]interface{}` by pgx — this is a pgx limitation.
- `xml` and `bytea` are both `[]uint8` — indistinguishable by Go type. XML gets base64 encoded. Users should cast to text in SQL if needed.
- JSONB large integers lose precision to `float64` inside pgx — known pgx limitation.
- `float32`/`float64` NaN/+Inf/-Inf from Postgres real/double precision columns break `json.Marshal` — must convert to string representations.
- `pgtype.Numeric` Infinity values panic in `MarshalJSON()` — must check `InfinityModifier` before calling it.

**truncateForLog helper** — truncates SQL for log output to avoid oversized log entries. Uses `utf8.RuneStart()` to avoid slicing mid-character (same approach as `truncateIfNeeded`):
```go
func truncateForLog(s string, maxLen int) string {
    if len(s) <= maxLen {
        return s
    }
    // Back up to the nearest valid UTF-8 boundary to avoid slicing mid-character.
    truncateAt := maxLen
    for truncateAt > 0 && !utf8.RuneStart(s[truncateAt]) {
        truncateAt--
    }
    return s[:truncateAt] + "...[truncated]"
}
```

**handleError logic:**
```go
// handleError converts any error into a QueryOutput with error message.
// The error message is always evaluated against error_prompts — matching
// prompt messages are appended. This applies to ALL errors: Postgres errors,
// protection rejections, hook rejections, hook error messages, Go errors.
// Errors are logged for server-side observability before prompt augmentation.
func (p *PostgresMcp) handleError(err error) *QueryOutput {
    // Log error for server-side observability. Log the raw error before
    // prompt augmentation so logs contain the original error without
    // appended guidance text.
    p.logger.Error().Err(err).Msg("query error")

    errMsg := err.Error()
    // Check error prompts (evaluated against ALL error messages).
    // Multiple matching prompts are joined with newlines — each prompt
    // is displayed as its own paragraph.
    prompt := p.errPrompts.Match(errMsg)
    if prompt != "" {
        errMsg = errMsg + "\n\n" + prompt
    }
    return &QueryOutput{Error: errMsg}
}
```

**truncateIfNeeded logic:**
```go
func (p *PostgresMcp) truncateIfNeeded(output *QueryOutput) {
    jsonBytes, _ := json.Marshal(output.Rows)
    jsonStr := string(jsonBytes)
    if utf8.RuneCountInString(jsonStr) <= p.config.Query.MaxResultLength {
        return
    }
    // Truncate to MaxResultLength characters (runes)
    runes := []rune(jsonStr)
    truncated := string(runes[:p.config.Query.MaxResultLength])
    output.Rows = nil
    output.Error = truncated + "...[truncated] Result is too long! Add limits in your query!"
}
```

### 3.3 ListTables Tool

**File: `listtables.go`**

```go
type ListTablesInput struct{}

type TableEntry struct {
    Schema              string `json:"schema"`
    Name                string `json:"name"`
    Type                string `json:"type"` // "table", "view", "materialized_view", "foreign_table", "partitioned_table"
    Owner               string `json:"owner"`
    SchemaAccessLimited bool   `json:"schema_access_limited,omitempty"` // true when user has SELECT on table but lacks USAGE on schema — queries may fail
}

type ListTablesOutput struct {
    Tables []TableEntry `json:"tables"`
    Error  string       `json:"error,omitempty"`
}

// ListTables returns (*ListTablesOutput, error). Unlike Query(), this returns a Go error
// because it doesn't go through the hook/protection/sanitization/error_prompts pipeline.
// Errors here are straightforward connection/query failures.
func (p *PostgresMcp) ListTables(ctx context.Context, input ListTablesInput) (*ListTablesOutput, error)
```

**ListTables implementation:**

```go
func (p *PostgresMcp) ListTables(ctx context.Context, input ListTablesInput) (*ListTablesOutput, error) {
    startTime := time.Now()

    // 1. Acquire semaphore (same as Query — bounds total concurrent operations to pool size)
    select {
    case p.semaphore <- struct{}{}:
    case <-ctx.Done():
        return nil, fmt.Errorf("ListTables: failed to acquire query slot: all %d connection slots are in use, context cancelled while waiting: %w", cap(p.semaphore), ctx.Err())
    }
    defer func() { <-p.semaphore }()

    // 2. Apply configurable timeout
    queryCtx, cancel := context.WithTimeout(ctx, time.Duration(p.config.Query.ListTablesTimeoutSeconds)*time.Second)
    defer cancel()

    // 3. Acquire connection and execute
    conn, err := p.pool.Acquire(queryCtx)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire connection: %w", err)
    }
    defer conn.Release()

    rows, err := conn.Query(queryCtx, listTablesSQL)
    // ... scan rows ...

    p.logger.Info().
        Dur("duration", time.Since(startTime)).
        Int("table_count", len(output.Tables)).
        Msg("ListTables executed")

    return output, nil
}
```

**SQL query:**
```sql
SELECT
    n.nspname AS schema,
    c.relname AS name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        WHEN 'f' THEN 'foreign_table'
        WHEN 'p' THEN 'partitioned_table'
    END AS type,
    pg_catalog.pg_get_userbyid(c.relowner) AS owner,
    NOT has_schema_privilege(n.oid, 'USAGE') AS schema_access_limited
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND has_table_privilege(c.oid, 'SELECT')
ORDER BY n.nspname, c.relname;
```

The `schema_access_limited` flag is `true` when the user has SELECT privilege on the table but lacks USAGE on its schema. This means queries against the table will likely fail with a permission error. Including this information lets AI agents make informed decisions (e.g., skip those tables or inform the user about schema access).

ListTables does NOT go through the hook/protection/sanitization pipeline — it's a read-only metadata query using a hardcoded SQL. It acquires the semaphore and uses `query.list_tables_timeout_seconds` for its timeout.

### 3.4 DescribeTable Tool

**File: `describetable.go`**

```go
type DescribeTableInput struct {
    Table  string `json:"table"`
    Schema string `json:"schema"` // defaults to "public"
}

type ColumnInfo struct {
    Name         string `json:"name"`
    Type         string `json:"type"`
    Nullable     bool   `json:"nullable"`
    Default      string `json:"default,omitempty"`
    IsPrimaryKey bool   `json:"is_primary_key"`
}

type IndexInfo struct {
    Name       string `json:"name"`
    Definition string `json:"definition"`
    IsUnique   bool   `json:"is_unique"`
    IsPrimary  bool   `json:"is_primary"`
}

type ConstraintInfo struct {
    Name       string `json:"name"`
    Type       string `json:"type"` // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
    Definition string `json:"definition"`
}

type ForeignKeyInfo struct {
    Name             string `json:"name"`
    Columns          string `json:"columns"`
    ReferencedTable  string `json:"referenced_table"`
    ReferencedColumns string `json:"referenced_columns"`
    OnUpdate         string `json:"on_update"`
    OnDelete         string `json:"on_delete"`
}

type PartitionInfo struct {
    Strategy       string   `json:"strategy"`                  // "range", "list", "hash"
    PartitionKey   string   `json:"partition_key"`             // e.g. "created_at", "region"
    Partitions     []string `json:"partitions,omitempty"`      // child partition table names
    ParentTable    string   `json:"parent_table,omitempty"`    // set if this is a child partition
}

type DescribeTableOutput struct {
    Schema      string           `json:"schema"`
    Name        string           `json:"name"`
    Type        string           `json:"type"`                   // "table", "view", "materialized_view", "foreign_table", "partitioned_table"
    Definition  string           `json:"definition,omitempty"`   // view/matview SQL definition
    Columns     []ColumnInfo     `json:"columns"`
    Indexes     []IndexInfo      `json:"indexes"`
    Constraints []ConstraintInfo `json:"constraints"`
    ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
    Partition   *PartitionInfo   `json:"partition,omitempty"`    // partition info for partitioned/child tables
    Error       string           `json:"error,omitempty"`
}

// DescribeTable returns (*DescribeTableOutput, error). Unlike Query(), this returns a Go error
// because it doesn't go through the hook/protection/sanitization/error_prompts pipeline.
func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error)
```

**DescribeTable implementation:**

```go
func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error) {
    startTime := time.Now()

    // Default schema to "public" when not specified.
    schema := input.Schema
    if schema == "" {
        schema = "public"
    }

    // 1. Acquire semaphore (same as Query — bounds total concurrent operations to pool size)
    select {
    case p.semaphore <- struct{}{}:
    case <-ctx.Done():
        return nil, fmt.Errorf("DescribeTable: failed to acquire query slot: all %d connection slots are in use, context cancelled while waiting: %w", cap(p.semaphore), ctx.Err())
    }
    defer func() { <-p.semaphore }()

    // 2. Apply configurable timeout
    queryCtx, cancel := context.WithTimeout(ctx, time.Duration(p.config.Query.DescribeTableTimeoutSeconds)*time.Second)
    defer cancel()

    // 3. Acquire connection and execute in read-only transaction.
    // Wrapping in a transaction ensures consistent metadata across multiple queries
    // (columns, indexes, constraints, etc.) — concurrent DDL won't cause inconsistencies.
    conn, err := p.pool.Acquire(queryCtx)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire connection: %w", err)
    }
    defer conn.Release()

    tx, err := conn.Begin(queryCtx)
    if err != nil {
        return nil, fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback(ctx) // always rollback — read-only metadata queries, no commit needed

    // Construct properly-quoted identifier for $1::regclass parameters.
    // quoteIdent doubles embedded double-quotes and wraps in double-quotes,
    // ensuring names with special characters are handled correctly.
    qualName := quoteIdent(schema) + "." + quoteIdent(input.Table)

    // ... run multiple pg_catalog queries using tx (not conn) with qualName for $1::regclass
    // and separate schema/table parameters for information_schema queries ...

    p.logger.Info().
        Str("schema", schema).
        Str("table", input.Table).
        Dur("duration", time.Since(startTime)).
        Str("type", output.Type).
        Int("column_count", len(output.Columns)).
        Msg("DescribeTable executed")

    return output, nil
}

// quoteIdent escapes a SQL identifier for safe use in $1::regclass.
// Doubles embedded double-quotes and wraps in double-quotes.
func quoteIdent(name string) string {
    return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
```

**Implementation:** Runs multiple `pg_catalog` queries for columns, indexes, constraints, foreign keys, and partition info, all within a single read-only transaction (for metadata consistency). Queries using `$1::regclass` receive the `qualName` parameter (properly quoted schema-qualified identifier). Queries using `information_schema` receive separate `schema` and `table` parameters. Does NOT go through the hook/protection/sanitization pipeline. Acquires the semaphore and uses `query.describe_table_timeout_seconds` for its timeout. The transaction is always rolled back (no commit — read-only metadata queries).

Must fully support **tables, views, materialized views, foreign tables, and partitioned tables** — the goal is for AI agents to have complete information to craft queries. The approach differs by object type:

**Object type detection** — first query determines the `relkind` (`r`=table, `v`=view, `m`=materialized view, `f`=foreign table, `p`=partitioned table) from `pg_class`. This determines which subsequent queries to run.

**Columns query:**
- For tables, views, foreign tables, partitioned tables: uses `information_schema.columns` joined with `pg_constraint` for primary key detection.
- For materialized views: uses `pg_attribute` joined with `pg_type` (materialized views are NOT in `information_schema.columns`). Query:
  ```sql
  SELECT a.attname AS name,
         pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
         NOT a.attnotnull AS nullable,
         pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default
  FROM pg_catalog.pg_attribute a
  LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
  WHERE a.attrelid = $1::regclass
    AND a.attnum > 0
    AND NOT a.attisdropped
  ORDER BY a.attnum;
  ```

**View definition** — for views and materialized views, also return the view SQL definition:
```sql
SELECT pg_catalog.pg_get_viewdef($1::regclass, true) AS definition;
```
The `Definition` field in `DescribeTableOutput` is populated for views and materialized views.

**Indexes query** — uses `pg_indexes` system view. Applicable to tables, partitioned tables, and materialized views (views don't have indexes).

**Constraints query** — uses `pg_constraint` with `pg_get_constraintdef()`. Applicable to tables and partitioned tables (views/matviews don't have constraints).

**Foreign keys query** — uses `pg_constraint` where `contype = 'f'`, joining `pg_attribute` for column names. Applicable to tables and partitioned tables.

**Partition info query** — for partitioned tables (`relkind = 'p'`), query partition strategy and key:
```sql
SELECT pg_catalog.pg_get_partkeydef(c.oid) AS partition_key,
       pt.partstrat AS strategy
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = c.oid
WHERE c.oid = $1::regclass;
```
Strategy values: `h`=hash, `l`=list, `r`=range — mapped to human-readable strings.

**Child partitions query** — list child partition names:
```sql
SELECT c.relname AS partition_name
FROM pg_catalog.pg_inherits i
JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = $1::regclass
ORDER BY c.relname;
```

**Parent table query** — for child partitions (regular tables that inherit from a partitioned table):
```sql
SELECT pc.relname AS parent_table,
       pn.nspname AS parent_schema
FROM pg_catalog.pg_inherits i
JOIN pg_catalog.pg_class pc ON pc.oid = i.inhparent
JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
WHERE i.inhrelid = $1::regclass;
```
If a parent exists and is a partitioned table, set `Partition.ParentTable` on the output.

---

## Phase 4: MCP Server Bridge

### 4.1 Tool Registration

**File: `mcp.go`**

```go
package pgmcp

import (
    "github.com/mark3labs/mcp-go/mcp"
    "github.com/mark3labs/mcp-go/server"
)

// RegisterMCPTools registers Query, ListTables, and DescribeTable
// as MCP tools on the given MCP server.
func RegisterMCPTools(mcpServer *server.MCPServer, pgMcp *PostgresMcp)
```

**Tool definitions:**

```go
// Query tool
queryTool := mcp.NewTool("query",
    mcp.WithDescription("Execute a SQL query against the PostgreSQL database. Returns results as JSON."),
    mcp.WithString("sql",
        mcp.Required(),
        mcp.Description("The SQL query to execute"),
    ),
)

// ListTables tool
listTablesTool := mcp.NewTool("list_tables",
    mcp.WithDescription("List all tables, views, materialized views, and foreign tables in the database that are accessible to the current user."),
    mcp.WithReadOnlyHintAnnotation(true),
)

// DescribeTable tool
describeTableTool := mcp.NewTool("describe_table",
    mcp.WithDescription("Describe the schema of a table including columns, types, indexes, constraints, and foreign keys."),
    mcp.WithString("table",
        mcp.Required(),
        mcp.Description("The table name to describe"),
    ),
    mcp.WithString("schema",
        mcp.Description("The schema name (defaults to 'public')"),
    ),
    mcp.WithReadOnlyHintAnnotation(true),
)
```

**Tool handlers** — thin wrappers that extract parameters from `mcp.CallToolRequest`, call the corresponding `PostgresMcp` method, and return `mcp.CallToolResult`:

```go
mcpServer.AddTool(queryTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
    sql, err := req.RequireString("sql")
    if err != nil {
        return mcp.NewToolResultError("sql parameter is required"), nil
    }
    // Query() returns only output — all errors are in output.Error
    // (already evaluated against error_prompts)
    output := pgMcp.Query(ctx, QueryInput{SQL: sql})
    if output.Error != "" {
        return mcp.NewToolResultError(output.Error), nil
    }
    jsonBytes, err := json.Marshal(output)
    if err != nil {
        return mcp.NewToolResultError("failed to marshal query result"), nil
    }
    return mcp.NewToolResultText(string(jsonBytes)), nil
})
```

---

## Phase 5: CLI Commands

### 5.1 Main Entrypoint

**File: `cmd/gopgmcp/main.go`**

Simple subcommand dispatch (no need for cobra — use `os.Args`):
- `gopgmcp serve` → run MCP server
- `gopgmcp configure` → run interactive config wizard
- No subcommand or `--help` → print usage

### 5.2 Serve Command

**File: `cmd/gopgmcp/serve.go`**

```go
func runServe() error {
    // 1. Load ServerConfig (env var path or .gopgmcp/config.json)
    serverConfig, err := loadServerConfig()

    // 2. Resolve connection string
    connString := os.Getenv("GOPGMCP_PG_CONNSTRING")
    if connString == "" {
        // Prompt for username and password interactively
        username := promptInput("Username: ")
        password := promptPassword("Password: ")
        connString = buildConnString(serverConfig.Connection, username, password)
    }

    // 3. Setup logger (zerolog)
    logger := setupLogger(serverConfig.Logging)

    // 4. Create PostgresMcp instance — extract embedded Config from ServerConfig.
    // Pass server hooks via WithServerHooks option (CLI-only, not in base Config).
    var opts []pgmcp.Option
    if len(serverConfig.ServerHooks.BeforeQuery) > 0 || len(serverConfig.ServerHooks.AfterQuery) > 0 {
        opts = append(opts, pgmcp.WithServerHooks(serverConfig.ServerHooks))
    }
    pgMcp, err := pgmcp.New(ctx, connString, serverConfig.Config, logger, opts...)
    defer pgMcp.Close(ctx)

    // 5. Create MCP server with session lifecycle logging.
    // Log when AI agents connect (initialize).
    // Use server.Hooks with AddAfterInitialize (mcp-go does not have OnInitialize method).
    hooks := &server.Hooks{}
    hooks.AddAfterInitialize(func(ctx context.Context, id any, req *mcp.InitializeRequest, result *mcp.InitializeResult) {
        clientName := req.Params.ClientInfo.Name
        clientVersion := req.Params.ClientInfo.Version
        logger.Info().
            Str("client_name", clientName).
            Str("client_version", clientVersion).
            Msg("AI agent connected (MCP initialize)")
    })

    mcpServer := server.NewMCPServer("gopgmcp", "1.0.0",
        server.WithToolCapabilities(true),
        server.WithHooks(hooks),
    )

    pgmcp.RegisterMCPTools(mcpServer, pgMcp)
    logger.Info().Int("port", serverConfig.Server.Port).Msg("starting gopgmcp server")
    return httpServer.Start(fmt.Sprintf(":%d", serverConfig.Server.Port))
}
```

**Health check implementation:**

**Critical mcp-go behavior (verified by test `mcphttp_test.go`):**

When `Start()` is called WITHOUT a custom `*http.Server` (i.e., `WithStreamableHTTPServer` not used), it creates its own `http.ServeMux`, registers the `StreamableHTTPServer` as a handler at `endpointPath`, and creates a new `http.Server`. However, when a custom `*http.Server` IS provided via `WithStreamableHTTPServer`, `Start()` **does NOT register any handler** — it only checks for address conflicts and calls `ListenAndServe()`. This means the MCP handler must be manually registered on the custom server's mux.

Source: [mcp-go `Start()` source code](https://github.com/mark3labs/mcp-go/blob/main/server/streamable_http.go) — the `if s.httpServer == nil` branch creates the mux and registers the handler; the `else` branch (custom server) skips handler registration entirely.

This was verified with two tests in `mcphttp_test.go`:
- `TestStreamableHTTP_CustomServer_DoesNotRegisterHandler` — confirms MCP endpoint returns 404 when custom server provided without manual registration
- `TestStreamableHTTP_ManualRegistration_Works` — confirms the correct approach works: manual `mux.Handle("/mcp", streamableServer)` + health check on the same mux

**Correct approach:** Create the mux, register both health check and the `StreamableHTTPServer` (which implements `http.Handler` via `ServeHTTP`), then pass the custom `http.Server` via `WithStreamableHTTPServer`. Order matters — the `StreamableHTTPServer` must be created before it can be registered on the mux, but the custom `http.Server` must be passed at construction time. Solution: create the `http.Server` first with the mux, pass it to the constructor, then register the handler on the mux.

```go
addr := fmt.Sprintf(":%d", serverConfig.Server.Port)

// Step 1: Create the mux.
mux := http.NewServeMux()

// Step 2: Register health check on the mux (if enabled).
// Health check confirms MCP server process is running and responsive.
// Does NOT check database connectivity (by design — documented in requirements).
if serverConfig.Server.HealthCheckEnabled {
    mux.HandleFunc(serverConfig.Server.HealthCheckPath, func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"status":"ok"}`))
    })
}

// CORS: Explicitly no CORS headers are set. CORS is only enforced by browsers —
// intended clients (AI agents, CLI tools, internal services) use plain HTTP.
// Not setting CORS headers is intentional: it prevents malicious webpages from
// making requests to an accidentally-exposed server (which has no auth).

// Step 3: Create the custom http.Server with the mux.
httpSrv := &http.Server{
    Addr:    addr,
    Handler: mux,
}

// Step 4: Create the StreamableHTTPServer with the custom http.Server.
streamableServer := server.NewStreamableHTTPServer(mcpServer,
    server.WithEndpointPath("/mcp"),
    server.WithStateLess(true),
    server.WithStreamableHTTPServer(httpSrv),
)

// Step 5: Manually register the StreamableHTTPServer on the mux.
// This is REQUIRED because Start() does NOT register the handler when
// a custom *http.Server is provided via WithStreamableHTTPServer.
mux.Handle("/mcp", streamableServer)

// Step 6: Start listening.
logger.Info().Int("port", serverConfig.Server.Port).Msg("starting gopgmcp server")
return streamableServer.Start(addr)
```

### 5.3 Configure Command

**File: `cmd/gopgmcp/configure.go`**

Delegates to `internal/configure.Run(configPath)`.

---

## Phase 6: Test Plan

### Test Helper

**File: `internal/testutil/testutil.go`** (or inline in test files)

```go
package pgmcp_test // or internal test package

import (
    "testing"
    "github.com/rickchristie/govner/pgflock/client"
)

const (
    pgflockLockerPort = 9776
    pgflockPassword   = "pgflock"
)

func acquireTestDB(t *testing.T) string {
    t.Helper()
    connStr, err := client.Lock(pgflockLockerPort, t.Name(), pgflockPassword)
    if err != nil {
        t.Fatalf("Failed to acquire test database: %v", err)
    }
    t.Cleanup(func() {
        _ = client.Unlock(pgflockLockerPort, pgflockPassword, connStr)
    })
    return connStr
}
```

### Test Hook Scripts

**File: `testdata/hooks/accept.sh`**
```bash
#!/bin/bash
cat /dev/stdin > /dev/null
echo '{"accept": true}'
```

**File: `testdata/hooks/reject.sh`**
```bash
#!/bin/bash
cat /dev/stdin > /dev/null
echo '{"accept": false, "error_message": "rejected by test hook"}'
```

**File: `testdata/hooks/modify_query.sh`**
```bash
#!/bin/bash
cat /dev/stdin > /dev/null
echo '{"accept": true, "modified_query": "SELECT 1 AS modified"}'
```

**File: `testdata/hooks/modify_result.sh`**
```bash
#!/bin/bash
cat /dev/stdin > /dev/null
echo '{"accept": true, "modified_result": "{\"columns\":[\"modified\"],\"rows\":[{\"modified\":true}]}"}'
```

**File: `testdata/hooks/slow.sh`**
```bash
#!/bin/bash
sleep 30
echo '{"accept": true}'
```

**File: `testdata/hooks/crash.sh`**
```bash
#!/bin/bash
exit 1
```

**File: `testdata/hooks/bad_json.sh`**
```bash
#!/bin/bash
cat /dev/stdin > /dev/null
echo 'this is not valid json'
```

**File: `testdata/hooks/echo_args.sh`**
```bash
#!/bin/bash
# Outputs the received arguments as JSON for verification
cat /dev/stdin > /dev/null
echo "{\"accept\": true, \"modified_query\": \"ARGS: $*\"}"
```

---

### 6.1 Unit Tests: Protection (`internal/protection/protection_test.go`)

All tests are pure unit tests — no database needed. Default config: all `Allow*` fields are `false` (blocked), `ReadOnly` is `false`. Each test asserts both the result (blocked/allowed) and the **exact error message content**. This ensures errors are easy to debug for both developers and AI agents.

#### Multi-Statement Detection (always enforced)

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestMultiStatement_TwoSelects` | `SELECT 1; SELECT 2` | default | `"multi-statement queries are not allowed: found 2 statements"` |
| `TestMultiStatement_SelectAndDrop` | `SELECT 1; DROP TABLE users` | default | `"multi-statement queries are not allowed: found 2 statements"` |
| `TestMultiStatement_ThreeStatements` | `SELECT 1; SELECT 2; SELECT 3` | default | `"multi-statement queries are not allowed: found 3 statements"` |
| `TestMultiStatement_CannotBeDisabled` | `SELECT 1; SELECT 2` | all Allow* = true | still blocked — `"multi-statement queries are not allowed: found 2 statements"` |
| `TestMultiStatement_SingleAllowed` | `SELECT 1` | default | allowed |
| `TestMultiStatement_EmptyStatements` | `;` or `;;` | default | error contains `"SQL parse error"` or `"multi-statement"` |

#### DROP Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestDrop_Table` | `DROP TABLE users` | default | `"DROP statements are not allowed"` |
| `TestDrop_Index` | `DROP INDEX idx_users` | default | `"DROP statements are not allowed"` |
| `TestDrop_Schema` | `DROP SCHEMA public` | default | `"DROP statements are not allowed"` |
| `TestDrop_Database` | `DROP DATABASE mydb` | default | `"DROP DATABASE is not allowed"` |
| `TestDrop_CaseInsensitive` | `drop table users` | default | `"DROP statements are not allowed"` |
| `TestDrop_WithComments` | `/* comment */ DROP TABLE users` | default | `"DROP statements are not allowed"` |
| `TestDrop_IfExists` | `DROP TABLE IF EXISTS users` | default | `"DROP statements are not allowed"` |
| `TestDrop_Cascade` | `DROP TABLE users CASCADE` | default | `"DROP statements are not allowed"` |
| `TestDrop_Allowed` | `DROP TABLE users` | AllowDrop=true | allowed |
| `TestDrop_DatabaseAllowed` | `DROP DATABASE mydb` | AllowDrop=true | allowed |

#### TRUNCATE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestTruncate_Basic` | `TRUNCATE users` | default | `"TRUNCATE statements are not allowed"` |
| `TestTruncate_Multiple` | `TRUNCATE users, orders` | default | `"TRUNCATE statements are not allowed"` |
| `TestTruncate_Cascade` | `TRUNCATE users CASCADE` | default | `"TRUNCATE statements are not allowed"` |
| `TestTruncate_Allowed` | `TRUNCATE users` | AllowTruncate=true | allowed |

#### SET Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestSet_SearchPath` | `SET search_path TO 'public'` | default | `"SET statements are not allowed: SET search_path"` |
| `TestSet_WorkMem` | `SET work_mem = '256MB'` | default | `"SET statements are not allowed: SET work_mem"` |
| `TestSet_ResetAll` | `RESET ALL` | default | `"RESET ALL is not allowed"` |
| `TestSet_ResetSingle` | `RESET work_mem` | default | `"RESET statements are not allowed: RESET work_mem"` |
| `TestSet_Allowed` | `SET work_mem = '256MB'` | AllowSet=true | allowed |
| `TestSet_ResetAllAllowed` | `RESET ALL` | AllowSet=true | allowed |
| `TestSet_ResetSingleAllowed` | `RESET work_mem` | AllowSet=true | allowed |

#### DO Block Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestDo_Simple` | `DO $$ BEGIN RAISE NOTICE 'hello'; END $$` | default | `"DO $$ blocks are not allowed: DO blocks can execute arbitrary SQL bypassing protection checks"` |
| `TestDo_WithDrop` | `DO $$ BEGIN EXECUTE 'DROP TABLE users'; END $$` | default | `"DO $$ blocks are not allowed"` |
| `TestDo_WithLanguage` | `DO LANGUAGE plpgsql $$ BEGIN NULL; END $$` | default | `"DO $$ blocks are not allowed"` |
| `TestDo_Allowed` | `DO $$ BEGIN NULL; END $$` | AllowDo=true | allowed |

#### COPY FROM Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCopyFrom_Basic` | `COPY users FROM '/tmp/data.csv'` | default | `"COPY FROM is not allowed"` |
| `TestCopyFrom_WithOptions` | `COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, HEADER true)` | default | `"COPY FROM is not allowed"` |
| `TestCopyFrom_Stdin` | `COPY users FROM STDIN` | default | `"COPY FROM is not allowed"` |
| `TestCopyFrom_Allowed` | `COPY users FROM '/tmp/data.csv'` | AllowCopyFrom=true | allowed |
| `TestCopyFrom_AllowedDoesNotAffectCopyTo` | `COPY users TO STDOUT` | AllowCopyFrom=true | `"COPY TO is not allowed"` (separate flag) |

#### CREATE FUNCTION / CREATE PROCEDURE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCreateFunction_Basic` | `CREATE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql` | default | `"CREATE FUNCTION is not allowed: can contain arbitrary SQL bypassing protection checks"` |
| `TestCreateFunction_OrReplace` | `CREATE OR REPLACE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql` | default | `"CREATE FUNCTION is not allowed"` |
| `TestCreateFunction_WithArgs` | `CREATE FUNCTION add(a int, b int) RETURNS int AS $$ BEGIN RETURN a + b; END $$ LANGUAGE plpgsql` | default | `"CREATE FUNCTION is not allowed"` |
| `TestCreateFunction_SQL` | `CREATE FUNCTION foo() RETURNS int AS 'SELECT 1' LANGUAGE sql` | default | `"CREATE FUNCTION is not allowed"` |
| `TestCreateProcedure_Basic` | `CREATE PROCEDURE do_stuff() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$` | default | `"CREATE PROCEDURE is not allowed: can contain arbitrary SQL bypassing protection checks"` |
| `TestCreateProcedure_OrReplace` | `CREATE OR REPLACE PROCEDURE do_stuff() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$` | default | `"CREATE PROCEDURE is not allowed"` |
| `TestCreateFunction_Allowed` | `CREATE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql` | AllowCreateFunction=true | allowed |
| `TestCreateProcedure_Allowed` | `CREATE PROCEDURE do_stuff() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$` | AllowCreateFunction=true | allowed |

#### PREPARE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestPrepare_Basic` | `PREPARE stmt AS SELECT 1` | default | `"PREPARE statements are not allowed: prepared statements can be executed later bypassing protection checks"` |
| `TestPrepare_WithParams` | `PREPARE stmt(int) AS SELECT * FROM users WHERE id = $1` | default | `"PREPARE statements are not allowed"` |
| `TestPrepare_WithDML` | `PREPARE stmt AS DELETE FROM users` | default | `"PREPARE statements are not allowed"` |
| `TestPrepare_Allowed` | `PREPARE stmt AS SELECT 1` | AllowPrepare=true | allowed |

#### EXPLAIN ANALYZE Protection (always recurses into inner statement)

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestExplain_SelectAllowed` | `EXPLAIN SELECT * FROM users` | default | allowed |
| `TestExplain_AnalyzeSelectAllowed` | `EXPLAIN ANALYZE SELECT * FROM users` | default | allowed |
| `TestExplain_DropParseError` | `EXPLAIN DROP TABLE users` | default | `"SQL parse error"` (DROP is not an ExplainableStmt — PostgreSQL rejects at parse level) |
| `TestExplain_AnalyzeDropParseError` | `EXPLAIN ANALYZE DROP TABLE users` | default | `"SQL parse error"` (same — non-explainable statement) |
| `TestExplain_DeleteWithoutWhereBlocked` | `EXPLAIN DELETE FROM users` | default | `"DELETE without WHERE clause is not allowed"` |
| `TestExplain_AnalyzeDeleteWithoutWhereBlocked` | `EXPLAIN ANALYZE DELETE FROM users` | default | `"DELETE without WHERE clause is not allowed"` |
| `TestExplain_DeleteWithWhereAllowed` | `EXPLAIN ANALYZE DELETE FROM users WHERE id = 1` | default | allowed |
| `TestExplain_UpdateWithoutWhereBlocked` | `EXPLAIN ANALYZE UPDATE users SET active = false` | default | `"UPDATE without WHERE clause is not allowed"` |
| `TestExplain_UpdateWithWhereAllowed` | `EXPLAIN ANALYZE UPDATE users SET active = false WHERE id = 1` | default | allowed |
| `TestExplain_TruncateParseError` | `EXPLAIN ANALYZE TRUNCATE users` | default | `"SQL parse error"` (TRUNCATE is not an ExplainableStmt) |
| `TestExplain_DropParseErrorEvenWhenAllowed` | `EXPLAIN DROP TABLE users` | AllowDrop=true | `"SQL parse error"` (parse error occurs before protection check) |
| `TestExplain_AnalyzeInsertAllowed` | `EXPLAIN ANALYZE INSERT INTO users (name) VALUES ('test')` | default | allowed |
| `TestExplain_AnalyzeMergeBlocked` | `EXPLAIN ANALYZE MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name` | default | `"MERGE statements are not allowed"` |
| `TestExplain_AnalyzeMergeAllowed` | `EXPLAIN ANALYZE MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name` | AllowMerge=true | allowed |
| `TestExplain_CTEDeleteWithoutWhere` | `EXPLAIN ANALYZE WITH d AS (DELETE FROM users RETURNING *) SELECT * FROM d` | default | `"DELETE without WHERE clause is not allowed"` |

#### DELETE/UPDATE with WHERE

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestDeleteWithoutWhere` | `DELETE FROM users` | default | `"DELETE without WHERE clause is not allowed"` |
| `TestDeleteWithWhere` | `DELETE FROM users WHERE id = 1` | default | allowed |
| `TestDeleteWithComplexWhere` | `DELETE FROM users WHERE id IN (SELECT id FROM banned)` | default | allowed |
| `TestDeleteWithExists` | `DELETE FROM users WHERE EXISTS (SELECT 1 FROM banned WHERE banned.uid = users.id)` | default | allowed |
| `TestDeleteWithoutWhere_Allowed` | `DELETE FROM users` | AllowDeleteWithoutWhere=true | allowed |
| `TestUpdateWithoutWhere` | `UPDATE users SET active = false` | default | `"UPDATE without WHERE clause is not allowed"` |
| `TestUpdateWithWhere` | `UPDATE users SET active = false WHERE id = 1` | default | allowed |
| `TestUpdateWithSubqueryWhere` | `UPDATE users SET active = false WHERE id IN (SELECT id FROM active_users)` | default | allowed |
| `TestUpdateWithoutWhere_Allowed` | `UPDATE users SET active = false` | AllowUpdateWithoutWhere=true | allowed |

#### Read-Only Mode

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestReadOnly_BlocksSetTransactionReadOnly` | `SET default_transaction_read_only = off` | ReadOnly=true, AllowSet=true | `"SET default_transaction_read_only is blocked in read-only mode: cannot change transaction read-only setting"` |
| `TestReadOnly_BlocksSetTransactionReadOnly2` | `SET transaction_read_only = false` | ReadOnly=true, AllowSet=true | `"SET transaction_read_only is blocked in read-only mode: cannot change transaction read-only setting"` |
| `TestReadOnly_BlocksResetAll` | `RESET ALL` | ReadOnly=true, AllowSet=true | `"RESET ALL is blocked in read-only mode: could disable read-only transaction setting"` |
| `TestReadOnly_BlocksResetTransactionReadOnly` | `RESET default_transaction_read_only` | ReadOnly=true, AllowSet=true | `"RESET default_transaction_read_only is blocked in read-only mode"` |
| `TestReadOnly_AllowsResetOther` | `RESET work_mem` | ReadOnly=true, AllowSet=true | allowed |
| `TestReadOnly_BlocksBeginReadWrite` | `BEGIN READ WRITE` | ReadOnly=true | `"BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction"` |
| `TestReadOnly_BlocksStartTransactionReadWrite` | `START TRANSACTION READ WRITE` | ReadOnly=true | `"BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction"` |
| `TestReadOnly_BeginReadOnlyStillBlocked` | `BEGIN READ ONLY` | ReadOnly=true | `"transaction control statements are not allowed"` (transaction control always blocked, regardless of read-only mode) |
| `TestReadOnly_BeginStillBlocked` | `BEGIN` | ReadOnly=true | `"transaction control statements are not allowed"` (transaction control always blocked) |
| `TestReadOnly_AllowsOtherSet` | `SET search_path = 'public'` | ReadOnly=true, AllowSet=true | allowed |
| `TestReadOnly_SetBlockedTakesPriority` | `SET default_transaction_read_only = off` | ReadOnly=true, AllowSet=false | `"SET default_transaction_read_only is blocked in read-only mode"` (readOnly check runs first) |

#### ALTER SYSTEM Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestAlterSystem_Set` | `ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements'` | default | `"ALTER SYSTEM is not allowed: can modify server-level configuration"` |
| `TestAlterSystem_Reset` | `ALTER SYSTEM RESET shared_preload_libraries` | default | `"ALTER SYSTEM is not allowed"` |
| `TestAlterSystem_ResetAll` | `ALTER SYSTEM RESET ALL` | default | `"ALTER SYSTEM is not allowed"` |
| `TestAlterSystem_ArchiveCommand` | `ALTER SYSTEM SET archive_command = '/bin/malicious'` | default | `"ALTER SYSTEM is not allowed"` |
| `TestAlterSystem_SSL` | `ALTER SYSTEM SET ssl = off` | default | `"ALTER SYSTEM is not allowed"` |
| `TestAlterSystem_Allowed` | `ALTER SYSTEM SET work_mem = '256MB'` | AllowAlterSystem=true | allowed |

#### MERGE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestMerge_Basic` | `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)` | default | `"MERGE statements are not allowed: MERGE can perform INSERT, UPDATE, and DELETE operations bypassing individual DML protection rules"` |
| `TestMerge_WithDelete` | `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN DELETE` | default | `"MERGE statements are not allowed"` |
| `TestMerge_Allowed` | `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name` | AllowMerge=true | allowed |

#### COPY TO Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCopyTo_Stdout` | `COPY users TO STDOUT` | default | `"COPY TO is not allowed: can export/exfiltrate data from tables"` |
| `TestCopyTo_File` | `COPY users TO '/tmp/data.csv'` | default | `"COPY TO is not allowed"` |
| `TestCopyTo_WithQuery` | `COPY (SELECT * FROM users) TO STDOUT` | default | `"COPY TO is not allowed"` |
| `TestCopyTo_Allowed` | `COPY users TO STDOUT` | AllowCopyTo=true | allowed |
| `TestCopyTo_AllowedDoesNotAffectCopyFrom` | `COPY users FROM '/tmp/data.csv'` | AllowCopyTo=true | `"COPY FROM is not allowed"` (separate flag) |

#### GRANT / REVOKE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestGrant_Table` | `GRANT SELECT ON users TO readonly_user` | default | `"GRANT statements are not allowed: can modify database permissions"` |
| `TestGrant_AllPrivileges` | `GRANT ALL PRIVILEGES ON users TO admin_user` | default | `"GRANT statements are not allowed"` |
| `TestRevoke_Table` | `REVOKE SELECT ON users FROM readonly_user` | default | `"REVOKE statements are not allowed: can modify database permissions"` |
| `TestGrantRole` | `GRANT admin TO bob` | default | `"GRANT ROLE is not allowed: can modify role memberships"` |
| `TestRevokeRole` | `REVOKE admin FROM bob` | default | `"REVOKE ROLE is not allowed: can modify role memberships"` |
| `TestGrant_Allowed` | `GRANT SELECT ON users TO readonly_user` | AllowGrantRevoke=true | allowed |
| `TestRevoke_Allowed` | `REVOKE SELECT ON users FROM readonly_user` | AllowGrantRevoke=true | allowed |
| `TestGrantRole_Allowed` | `GRANT admin TO bob` | AllowGrantRevoke=true | allowed |

#### Role Management Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCreateRole_Basic` | `CREATE ROLE testrole WITH LOGIN PASSWORD 'secret'` | default | `"CREATE ROLE/USER is not allowed: can create database roles with privileges"` |
| `TestCreateUser` | `CREATE USER testuser WITH PASSWORD 'secret'` | default | `"CREATE ROLE/USER is not allowed"` (CREATE USER is syntactic sugar for CREATE ROLE) |
| `TestAlterRole_Superuser` | `ALTER ROLE testrole WITH SUPERUSER` | default | `"ALTER ROLE/USER is not allowed: can modify role privileges including SUPERUSER"` |
| `TestAlterUser` | `ALTER USER testuser SET search_path = 'public'` | default | `"ALTER ROLE/USER is not allowed"` |
| `TestDropRole` | `DROP ROLE testrole` | default | `"DROP ROLE/USER is not allowed: can delete database roles"` |
| `TestDropUser` | `DROP USER testuser` | default | `"DROP ROLE/USER is not allowed"` (DROP USER is syntactic sugar for DROP ROLE) |
| `TestCreateRole_Allowed` | `CREATE ROLE testrole` | AllowManageRoles=true | allowed |
| `TestAlterRole_Allowed` | `ALTER ROLE testrole WITH SUPERUSER` | AllowManageRoles=true | allowed |
| `TestDropRole_Allowed` | `DROP ROLE testrole` | AllowManageRoles=true | allowed |

#### CREATE EXTENSION Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCreateExtension_Basic` | `CREATE EXTENSION pg_trgm` | default | `"CREATE EXTENSION is not allowed: can load arbitrary server-side code into PostgreSQL"` |
| `TestCreateExtension_IfNotExists` | `CREATE EXTENSION IF NOT EXISTS pgcrypto` | default | `"CREATE EXTENSION is not allowed"` |
| `TestCreateExtension_Allowed` | `CREATE EXTENSION pg_trgm` | AllowCreateExtension=true | allowed |

#### LOCK TABLE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestLockTable_Basic` | `LOCK TABLE users` | default | `"LOCK TABLE is not allowed: can acquire exclusive locks causing deadlocks or denial of service"` |
| `TestLockTable_ExclusiveMode` | `LOCK TABLE users IN EXCLUSIVE MODE` | default | `"LOCK TABLE is not allowed"` |
| `TestLockTable_Allowed` | `LOCK TABLE users` | AllowLockTable=true | allowed |

#### LISTEN / NOTIFY Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestListen_Basic` | `LISTEN my_channel` | default | `"LISTEN is not allowed: can be used for side-channel communication between sessions"` |
| `TestNotify_Basic` | `NOTIFY my_channel, 'hello'` | default | `"NOTIFY is not allowed: can send arbitrary payloads to listening sessions"` |
| `TestNotify_NoPayload` | `NOTIFY my_channel` | default | `"NOTIFY is not allowed"` |
| `TestListen_Allowed` | `LISTEN my_channel` | AllowListenNotify=true | allowed |
| `TestNotify_Allowed` | `NOTIFY my_channel, 'hello'` | AllowListenNotify=true | allowed |

#### Maintenance Command Protection (VACUUM, ANALYZE, CLUSTER, REINDEX)

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestVacuum_Basic` | `VACUUM users` | default | `"VACUUM/ANALYZE is not allowed: maintenance commands can acquire heavy locks and cause significant I/O load"` |
| `TestVacuum_Full` | `VACUUM FULL users` | default | `"VACUUM/ANALYZE is not allowed"` |
| `TestVacuum_Analyze` | `VACUUM ANALYZE users` | default | `"VACUUM/ANALYZE is not allowed"` |
| `TestAnalyze_Standalone` | `ANALYZE users` | default | `"VACUUM/ANALYZE is not allowed"` |
| `TestCluster_Basic` | `CLUSTER users USING users_pkey` | default | `"CLUSTER is not allowed: acquires ACCESS EXCLUSIVE lock and rewrites the entire table"` |
| `TestReindex_Basic` | `REINDEX TABLE users` | default | `"REINDEX is not allowed: can acquire ACCESS EXCLUSIVE lock on tables and indexes"` |
| `TestReindex_Index` | `REINDEX INDEX users_pkey` | default | `"REINDEX is not allowed"` |
| `TestVacuum_Allowed` | `VACUUM users` | AllowMaintenance=true | allowed |
| `TestAnalyze_Allowed` | `ANALYZE users` | AllowMaintenance=true | allowed |
| `TestCluster_Allowed` | `CLUSTER users USING users_pkey` | AllowMaintenance=true | allowed |
| `TestReindex_Allowed` | `REINDEX TABLE users` | AllowMaintenance=true | allowed |
| `TestRefreshMatView_Basic` | `REFRESH MATERIALIZED VIEW my_view` | default | `"REFRESH MATERIALIZED VIEW is not allowed: can acquire ACCESS EXCLUSIVE lock (without CONCURRENTLY) and cause significant I/O load"` |
| `TestRefreshMatView_Concurrently` | `REFRESH MATERIALIZED VIEW CONCURRENTLY my_view` | default | `"REFRESH MATERIALIZED VIEW is not allowed"` |
| `TestRefreshMatView_Allowed` | `REFRESH MATERIALIZED VIEW my_view` | AllowMaintenance=true | allowed |

#### DDL Protection (CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.)

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestDDL_CreateTable` | `CREATE TABLE test (id int)` | default | `"CREATE TABLE is not allowed: DDL operations are blocked"` |
| `TestDDL_AlterTable` | `ALTER TABLE users ADD COLUMN email text` | default | `"ALTER TABLE is not allowed: DDL operations are blocked"` |
| `TestDDL_CreateIndex` | `CREATE INDEX idx_name ON users (name)` | default | `"CREATE INDEX is not allowed: DDL operations are blocked"` |
| `TestDDL_CreateSchema` | `CREATE SCHEMA myschema` | default | `"CREATE SCHEMA is not allowed: DDL operations are blocked"` |
| `TestDDL_CreateView` | `CREATE VIEW active_users AS SELECT * FROM users WHERE active = true` | default | `"CREATE VIEW is not allowed: DDL operations are blocked"` |
| `TestDDL_CreateSequence` | `CREATE SEQUENCE user_id_seq` | default | `"CREATE SEQUENCE is not allowed: DDL operations are blocked"` |
| `TestDDL_CreateTableAs` | `CREATE TABLE summary AS SELECT COUNT(*) FROM users` | default | `"CREATE TABLE AS / CREATE MATERIALIZED VIEW is not allowed: DDL operations are blocked"` |
| `TestDDL_AlterSequence` | `ALTER SEQUENCE user_id_seq RESTART WITH 100` | default | `"ALTER SEQUENCE is not allowed: DDL operations are blocked"` |
| `TestDDL_Rename` | `ALTER TABLE users RENAME TO customers` | default | `"RENAME is not allowed: DDL operations are blocked"` |
| `TestDDL_CreateTable_Allowed` | `CREATE TABLE test (id int)` | AllowDDL=true | allowed |
| `TestDDL_AlterTable_Allowed` | `ALTER TABLE users ADD COLUMN email text` | AllowDDL=true | allowed |
| `TestDDL_CreateIndex_Allowed` | `CREATE INDEX idx_name ON users (name)` | AllowDDL=true | allowed |
| `TestDDL_DropNotAffectedByDDL` | `DROP TABLE users` | AllowDDL=true | `"DROP statements are not allowed"` (DROP has its own flag) |

#### DISCARD Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestDiscard_All` | `DISCARD ALL` | default | `"DISCARD is not allowed: resets session state including prepared statements and temporary tables"` |
| `TestDiscard_Plans` | `DISCARD PLANS` | default | `"DISCARD is not allowed"` |
| `TestDiscard_Temp` | `DISCARD TEMPORARY` | default | `"DISCARD is not allowed"` |
| `TestDiscard_Allowed` | `DISCARD ALL` | AllowDiscard=true | allowed |

#### COMMENT ON Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestComment_OnTable` | `COMMENT ON TABLE users IS 'User accounts'` | default | `"COMMENT ON is not allowed: modifies database object metadata"` |
| `TestComment_OnColumn` | `COMMENT ON COLUMN users.name IS 'Full name'` | default | `"COMMENT ON is not allowed"` |
| `TestComment_Allowed` | `COMMENT ON TABLE users IS 'User accounts'` | AllowComment=true | allowed |

#### CREATE TRIGGER Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCreateTrigger_Basic` | `CREATE TRIGGER trg_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()` | default | `"CREATE TRIGGER is not allowed: triggers execute arbitrary function calls on every DML operation, bypassing protection checks"` |
| `TestCreateTrigger_Before` | `CREATE TRIGGER trg_before BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION check_func()` | default | `"CREATE TRIGGER is not allowed"` |
| `TestCreateTrigger_OrReplace` | `CREATE OR REPLACE TRIGGER trg_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()` | default | `"CREATE TRIGGER is not allowed"` |
| `TestCreateTrigger_Statement` | `CREATE TRIGGER trg_stmt AFTER INSERT ON users FOR EACH STATEMENT EXECUTE FUNCTION notify_func()` | default | `"CREATE TRIGGER is not allowed"` |
| `TestCreateTrigger_Constraint` | `CREATE CONSTRAINT TRIGGER trg_fk AFTER INSERT ON orders FOR EACH ROW EXECUTE FUNCTION check_fk()` | default | `"CREATE TRIGGER is not allowed"` |
| `TestCreateTrigger_InsteadOf` | `CREATE TRIGGER trg_view INSTEAD OF INSERT ON my_view FOR EACH ROW EXECUTE FUNCTION view_insert()` | default | `"CREATE TRIGGER is not allowed"` |
| `TestCreateTrigger_Allowed` | `CREATE TRIGGER trg_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_func()` | AllowCreateTrigger=true | allowed |

#### CREATE RULE Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestCreateRule_Basic` | `CREATE RULE notify_insert AS ON INSERT TO users DO ALSO NOTIFY users_changed` | default | `"CREATE RULE is not allowed: rules rewrite queries at the parser level, can silently transform statements and bypass protection checks"` |
| `TestCreateRule_OrReplace` | `CREATE OR REPLACE RULE notify_insert AS ON INSERT TO users DO ALSO NOTIFY users_changed` | default | `"CREATE RULE is not allowed"` |
| `TestCreateRule_Instead` | `CREATE RULE protect_delete AS ON DELETE TO users DO INSTEAD NOTHING` | default | `"CREATE RULE is not allowed"` |
| `TestCreateRule_WithAction` | `CREATE RULE log_update AS ON UPDATE TO users DO ALSO INSERT INTO audit_log (action) VALUES ('update')` | default | `"CREATE RULE is not allowed"` |
| `TestCreateRule_Allowed` | `CREATE RULE notify_insert AS ON INSERT TO users DO ALSO NOTIFY users_changed` | AllowCreateRule=true | allowed |

#### REFRESH MATERIALIZED VIEW Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestRefreshMatView_Basic` | `REFRESH MATERIALIZED VIEW my_view` | default | `"REFRESH MATERIALIZED VIEW is not allowed: can acquire ACCESS EXCLUSIVE lock (without CONCURRENTLY) and cause significant I/O load"` |
| `TestRefreshMatView_Concurrently` | `REFRESH MATERIALIZED VIEW CONCURRENTLY my_view` | default | `"REFRESH MATERIALIZED VIEW is not allowed"` |
| `TestRefreshMatView_WithNoData` | `REFRESH MATERIALIZED VIEW my_view WITH NO DATA` | default | `"REFRESH MATERIALIZED VIEW is not allowed"` |
| `TestRefreshMatView_Allowed` | `REFRESH MATERIALIZED VIEW my_view` | AllowMaintenance=true | allowed |

#### ALTER EXTENSION Protection

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestAlterExtension_Update` | `ALTER EXTENSION pg_trgm UPDATE TO '1.6'` | default | `"ALTER EXTENSION is not allowed: can update extensions, loading new server-side code"` |
| `TestAlterExtension_UpdateNoVersion` | `ALTER EXTENSION pg_trgm UPDATE` | default | `"ALTER EXTENSION is not allowed"` |
| `TestAlterExtension_AddTable` | `ALTER EXTENSION pg_trgm ADD TABLE my_table` | default | `"ALTER EXTENSION is not allowed: can modify extension contents"` |
| `TestAlterExtension_DropFunction` | `ALTER EXTENSION pg_trgm DROP FUNCTION my_func()` | default | `"ALTER EXTENSION is not allowed"` |
| `TestAlterExtension_Allowed` | `ALTER EXTENSION pg_trgm UPDATE TO '1.6'` | AllowCreateExtension=true | allowed |
| `TestAlterExtension_AddAllowed` | `ALTER EXTENSION pg_trgm ADD TABLE my_table` | AllowCreateExtension=true | allowed |

#### Transaction Control Protection (always blocked)

| Test | SQL | Config | Expected Error Contains |
|---|---|---|---|
| `TestTransaction_Begin` | `BEGIN` | default | `"transaction control statements are not allowed: each query runs in a managed transaction with AfterQuery hooks as guardrails"` |
| `TestTransaction_StartTransaction` | `START TRANSACTION` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_Commit` | `COMMIT` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_End` | `END` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_Rollback` | `ROLLBACK` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_Abort` | `ABORT` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_Savepoint` | `SAVEPOINT my_savepoint` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_ReleaseSavepoint` | `RELEASE SAVEPOINT my_savepoint` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_RollbackToSavepoint` | `ROLLBACK TO SAVEPOINT my_savepoint` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_PrepareTransaction` | `PREPARE TRANSACTION 'my_tx'` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_CommitPrepared` | `COMMIT PREPARED 'my_tx'` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_RollbackPrepared` | `ROLLBACK PREPARED 'my_tx'` | default | `"transaction control statements are not allowed"` |
| `TestTransaction_CannotBeEnabled` | `BEGIN` | all Allow* = true | still blocked — `"transaction control statements are not allowed"` (always enforced, no toggle) |
| `TestTransaction_BeginReadWriteReadOnlyMode` | `BEGIN READ WRITE` | ReadOnly=true | `"BEGIN READ WRITE is blocked in read-only mode"` (more specific message takes priority) |
| `TestTransaction_BeginReadOnlyReadOnlyMode` | `BEGIN READ ONLY` | ReadOnly=true | `"transaction control statements are not allowed"` (read-only BEGIN still blocked — pipeline manages transactions) |

#### Allowed Statements

| Test | SQL | Config | Expected |
|---|---|---|---|
| `TestAllowSelect` | `SELECT * FROM users` | default | allowed |
| `TestAllowSelectComplex` | `WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id > 1` | default | allowed |
| `TestAllowInsert` | `INSERT INTO users (name) VALUES ('test')` | default | allowed |
| `TestAllowInsertReturning` | `INSERT INTO users (name) VALUES ('test') RETURNING *` | default | allowed |
| `TestAllowInsertOnConflict` | `INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = 'test'` | default | allowed |
| `TestAllowCreateTable` | `CREATE TABLE test (id int)` | AllowDDL=true | allowed (DDL must be explicitly enabled) |
| `TestAllowAlterTable` | `ALTER TABLE users ADD COLUMN email text` | AllowDDL=true | allowed (DDL must be explicitly enabled) |
| `TestAllowExplain` | `EXPLAIN ANALYZE SELECT * FROM users` | default | allowed |
| `TestAllowDeleteWithWhere` | `DELETE FROM users WHERE id = 1` | default | allowed |
| `TestAllowUpdateWithWhere` | `UPDATE users SET active = false WHERE id = 1` | default | allowed |

#### Complex SQL / Edge Cases

| Test | SQL | Config | Expected |
|---|---|---|---|
| `TestCTEWithDelete` | `WITH deleted AS (DELETE FROM users WHERE id = 1 RETURNING *) SELECT * FROM deleted` | default | allowed (has WHERE) |
| `TestCTEWithDeleteNoWhere` | `WITH deleted AS (DELETE FROM users RETURNING *) SELECT * FROM deleted` | default | `"DELETE without WHERE clause is not allowed"` |
| `TestCTEWithUpdateNoWhere` | `WITH updated AS (UPDATE users SET active = false RETURNING *) SELECT * FROM updated` | default | `"UPDATE without WHERE clause is not allowed"` |
| `TestCTEWithUpdateWithWhere` | `WITH updated AS (UPDATE users SET active = false WHERE id = 1 RETURNING *) SELECT * FROM updated` | default | allowed (has WHERE) |
| `TestCTENestedDML` | `WITH a AS (WITH b AS (DELETE FROM users RETURNING *) SELECT * FROM b) SELECT * FROM a` | default | `"DELETE without WHERE clause is not allowed"` (recursion into nested CTE) |
| `TestCTEOnInsert` | `WITH src AS (DELETE FROM old_users RETURNING *) INSERT INTO archive SELECT * FROM src` | default | `"DELETE without WHERE clause is not allowed"` (CTE on INSERT statement) |
| `TestCTEOnUpdate` | `WITH src AS (SELECT id FROM banned) UPDATE users SET active = false WHERE id IN (SELECT id FROM src)` | default | allowed (UPDATE has WHERE, CTE is SELECT) |
| `TestCTEOnDelete` | `WITH src AS (SELECT id FROM banned) DELETE FROM users WHERE id IN (SELECT id FROM src)` | default | allowed (DELETE has WHERE, CTE is SELECT) |
| `TestCTESelectOnly` | `WITH counts AS (SELECT department, COUNT(*) as cnt FROM employees GROUP BY department) SELECT * FROM counts` | default | allowed (CTE is SELECT, no DML) |
| `TestCTEMultipleDML` | `WITH d AS (DELETE FROM old_users WHERE expired = true RETURNING *), i AS (INSERT INTO archive SELECT * FROM d RETURNING *) SELECT * FROM i` | default | allowed (DELETE has WHERE, INSERT has no protection check) |
| `TestCTEOnMerge_DeleteNoWhere` | `WITH d AS (DELETE FROM users RETURNING *) MERGE INTO target t USING d ON t.id = d.id WHEN MATCHED THEN UPDATE SET name = d.name` | AllowMerge=true | `"DELETE without WHERE clause is not allowed"` (CTE on MERGE checked recursively) |
| `TestCTEOnMerge_SelectCTE` | `WITH src AS (SELECT * FROM staging) MERGE INTO target t USING src ON t.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name` | AllowMerge=true | allowed (CTE is SELECT, MERGE is allowed) |
| `TestCTEOnMerge_UpdateNoWhere` | `WITH u AS (UPDATE users SET active = false RETURNING *) MERGE INTO target t USING u ON t.id = u.id WHEN NOT MATCHED THEN INSERT (id) VALUES (u.id)` | AllowMerge=true | `"UPDATE without WHERE clause is not allowed"` (CTE on MERGE checked recursively) |
| `TestNestedSubquerySelect` | `SELECT * FROM (SELECT * FROM (SELECT id FROM users) AS a) AS b` | default | allowed |
| `TestComplexJoins` | `SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id LEFT JOIN items i ON o.id = i.order_id WHERE u.active = true` | default | allowed |
| `TestWindowFunction` | `SELECT id, name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees` | default | allowed |
| `TestRecursiveCTE` | `WITH RECURSIVE tree AS (SELECT id, parent_id FROM categories WHERE parent_id IS NULL UNION ALL SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree` | default | allowed |
| `TestJSONBQuery` | `SELECT data->>'name' AS name, data->'address'->>'city' AS city FROM users WHERE data @> '{"active": true}'` | default | allowed |
| `TestArrayQuery` | `SELECT * FROM users WHERE tags @> ARRAY['admin']::text[]` | default | allowed |
| `TestLateralJoin` | `SELECT * FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY created_at DESC LIMIT 5) recent_orders` | default | allowed |
| `TestParseError` | `NOT VALID SQL @#$` | default | `"SQL parse error"` |
| `TestAllProtectionsAllowed` | `DROP TABLE users` | all Allow* = true | allowed |
| `TestSQLInjection_UnionBased` | `SELECT * FROM users WHERE id = 1 UNION SELECT * FROM pg_shadow` | default | allowed (single statement, no protection rule against UNION) |
| `TestSQLInjection_CommentBased` | `SELECT * FROM users -- WHERE admin = true` | default | allowed (single statement, comment is valid SQL) |
| `TestSQLInjection_MultiStatement` | `SELECT * FROM users; DROP TABLE users` | default | `"multi-statement queries are not allowed: found 2 statements"` |
| `TestSQLInjection_Stacked` | `SELECT 1; DELETE FROM users; --` | default | `"multi-statement queries are not allowed: found 2 statements"` (pg_query sees 2 statements: trailing `; --` is a comment, not a third statement) |
| `TestEmptySQL` | `` (empty string) | default | `"SQL parse error"` |
| `TestWhitespaceOnlySQL` | `   ` | default | `"SQL parse error"` |

### 6.2 Unit Tests: Sanitization (`internal/sanitize/sanitize_test.go`)

| Test | Input | Rules | Expected |
|---|---|---|---|
| `TestSanitizePhoneNumber` | `+62821233447` | phone regex → `${1}xxx${4}` | `+62xxx447` |
| `TestSanitizeKTP` | `3201234567890001` | KTP regex | masked |
| `TestNoMatch` | `hello world` | phone regex | `hello world` (unchanged) |
| `TestMultipleRulesOrdering` | value matching both rules | rule1, rule2 | both applied in order |
| `TestSanitizeJSONBField` | `map[string]interface{}{"phone": "+62821233447"}` | phone regex | phone field sanitized |
| `TestSanitizeNestedJSONB` | `{"contact": {"phone": "+62821233447"}}` | phone regex | deeply nested value sanitized |
| `TestSanitizeArrayField` | `[]interface{}{"+62821233447", "+62899887766"}` | phone regex | both elements sanitized |
| `TestSanitizeNullField` | `nil` | any rule | `nil` (unchanged) |
| `TestSanitizeNumericField` | `int64(12345)` | any rule | `12345` (unchanged, not a string) |
| `TestSanitizeJsonNumber` | `json.Number("9007199254740993")` | any rule | unchanged (not a string type for sanitization) |
| `TestSanitizeBooleanField` | `true` | any rule | `true` (unchanged) |
| `TestSanitizeEmptyRules` | any value | no rules | unchanged |
| `TestSanitizeRows` | full result set with mixed types | phone regex | only string fields sanitized |

### 6.3 Unit Tests: Hooks (`internal/hooks/hooks_test.go`)

Tests use the shell scripts in `testdata/hooks/`. All scripts must be `chmod +x`.

| Test | Hook Config | Expected |
|---|---|---|
| `TestBeforeQuery_Accept` | accept.sh, pattern `.*` | query passes through unchanged, no error |
| `TestBeforeQuery_Reject` | reject.sh, pattern `.*` | error returned: `"rejected by test hook"` |
| `TestBeforeQuery_ModifyQuery` | modify_query.sh, pattern `.*` | query changed to `"SELECT 1 AS modified"` |
| `TestBeforeQuery_PatternNoMatch` | accept.sh, pattern `NEVER_MATCH` | hook not executed, query passes through unchanged |
| `TestBeforeQuery_Chaining` | [modify_query.sh, accept.sh] | second hook receives `"SELECT 1 AS modified"` as input |
| `TestBeforeQuery_ChainPatternReEval` | [modify_query.sh (pattern `.*`), reject.sh (pattern `modified`)] | second hook matches modified query, error: `"rejected by test hook"` |
| `TestBeforeQuery_Timeout` | slow.sh, timeout=1s | error returned: `"before_query hook error: hook timed out: ..."` — pipeline stops |
| `TestBeforeQuery_Crash` | crash.sh, pattern `.*` | error returned: `"before_query hook error: hook failed (command: ...)"` — pipeline stops |
| `TestBeforeQuery_UnparseableResponse` | script that outputs `not json`, pattern `.*` | error returned: `"before_query hook returned unparseable response (command: ...)"` — pipeline stops |
| `TestAfterQuery_Accept` | accept.sh | result passes through unchanged |
| `TestAfterQuery_Reject` | reject.sh | error returned: `"rejected by test hook"` |
| `TestAfterQuery_ModifyResult` | modify_result.sh | result changed to modified JSON |
| `TestAfterQuery_Chaining` | [modify_result.sh, accept.sh] | second hook receives modified result as input |
| `TestAfterQuery_Timeout` | slow.sh, timeout=1s | error returned: `"after_query hook error: hook timed out: ..."` — pipeline stops |
| `TestAfterQuery_Crash` | crash.sh | error returned: `"after_query hook error: hook failed (command: ...)"` — pipeline stops |
| `TestAfterQuery_UnparseableResponse` | script that outputs `not json`, pattern `.*` | error returned: `"after_query hook returned unparseable response (command: ...)"` — pipeline stops |
| `TestHookStdinInput` | custom script that echoes stdin back | verify raw SQL query string passed as stdin for BeforeQuery |
| `TestHookWithArgs` | echo_args.sh, args: `["--flag", "value"]` | modified_query contains `"ARGS: --flag value"` |
| `TestHookWithEmptyArgs` | accept.sh, args: `[]` | works same as no args, query passes through |
| `TestHookDefaultTimeout` | slow.sh with no per-hook timeout, default=1s | error returned — uses default timeout, hook times out, pipeline stops |
| `TestHookPerHookTimeoutOverridesDefault` | slow.sh with per-hook timeout=2s, default=1s | error returned — uses per-hook timeout (2s), hook still times out (sleep 30), pipeline stops |
| `TestHookPanicOnZeroDefaultTimeout` | config with hooks but default_timeout=0 | NewRunner panics |
| `TestHasAfterQueryHooks_True` | config with AfterQuery hooks | `HasAfterQueryHooks()` returns `true` |
| `TestHasAfterQueryHooks_False` | config with no AfterQuery hooks | `HasAfterQueryHooks()` returns `false` |

### 6.3.1 Unit Tests: Go Hooks (`query_gohooks_test.go`)

These test the `runGoBeforeHooks` and `runGoAfterHooks` methods directly, using mock implementations of the hook interfaces. No database needed — tests create `PostgresMcp` structs with pre-populated hook slices and call the methods.

| Test | Hook Config | Expected |
|---|---|---|
| `TestGoBeforeHooks_PassThrough` | Single BeforeQueryHook that returns query unchanged | query passes through, no error |
| `TestGoBeforeHooks_Reject` | Single BeforeQueryHook that returns error `"blocked"` | error: `"before_query hook error: hook rejected query (name: ...): blocked"` |
| `TestGoBeforeHooks_ModifyQuery` | Single BeforeQueryHook that changes query | modified query returned |
| `TestGoBeforeHooks_Chaining` | Two hooks: first modifies, second receives modified | second hook receives output of first |
| `TestGoBeforeHooks_ChainStopsOnReject` | Two hooks: first rejects | second hook never called, error returned |
| `TestGoBeforeHooks_Timeout` | Hook that sleeps 2s, timeout=1s | error: `"before_query hook error: hook timed out (name: ..., timeout: 1s)"` |
| `TestGoBeforeHooks_PerHookTimeoutOverridesDefault` | Hook with entry.Timeout=3s, default=1s, hook sleeps 2s | succeeds (uses per-hook 3s, not default 1s) |
| `TestGoBeforeHooks_Empty` | No hooks configured | query passes through unchanged |
| `TestGoAfterHooks_PassThrough` | Single AfterQueryHook that returns result unchanged | result passes through, no error |
| `TestGoAfterHooks_Reject` | Single AfterQueryHook that returns error | error: `"after_query hook error: hook rejected result (name: ...): ..."` |
| `TestGoAfterHooks_ModifyResult` | Hook that adds a column to result | modified result returned |
| `TestGoAfterHooks_Chaining` | Two hooks: first modifies, second receives modified | second hook receives output of first |
| `TestGoAfterHooks_Timeout` | Hook that sleeps 2s, timeout=1s | error: `"after_query hook error: hook timed out (name: ..., timeout: 1s)"` |
| `TestGoAfterHooks_Empty` | No hooks configured | result passes through unchanged |
| `TestGoAfterHooks_PreservesTypes` | Hook that inspects result types via type assertion | int64, string, etc. preserved — no serialization occurred |

### 6.4 Unit Tests: Error Prompts (`internal/errprompt/errprompt_test.go`)

| Test | Error Message | Rules | Expected |
|---|---|---|---|
| `TestMatchPermissionDenied` | `permission denied for table users` | `(?i)permission denied` → message | message returned |
| `TestMatchRelationNotExist` | `relation "foo" does not exist` | `(?i)relation.*does not exist` → message | message returned |
| `TestNoMatch` | `some other error` | both rules | empty string |
| `TestMultipleMatches` | `permission denied for table users` | `(?i)permission denied` + `(?i)denied.*table` | both messages concatenated with `\n` separator |
| `TestEmptyRules` | any error | no rules | empty string |
| `TestMatchHookError` | `rejected by test hook` | `(?i)rejected` → message | message returned |
| `TestNewMatcherPanicsOnInvalidRegex` | N/A | `[invalid` (bad regex) | panics with invalid regex error |

### 6.5 Unit Tests: Timeout (`internal/timeout/timeout_test.go`)

| Test | SQL | Rules | Expected |
|---|---|---|---|
| `TestMatchFirstRule` | `SELECT * FROM pg_stat_activity` | pg_stat→5s, JOIN→60s | 5s |
| `TestStopOnFirstMatch` | `SELECT * FROM pg_stat JOIN x JOIN y JOIN z` | pg_stat→5s, JOIN×3→60s | 5s (first match wins) |
| `TestDefaultTimeout` | `SELECT 1` | no matching rules, default=30s | 30s |
| `TestNoRules` | `SELECT 1` | empty rules, default=30s | 30s |

### 6.6 Unit Tests: Config

Config loading/validation tests are in `cmd/gopgmcp/serve_test.go` (CLI-level tests with `loadServerConfig()`). Config struct tests (protection defaults, JSON tags, Go hooks validation) are in `config_test.go` (root package). Configure wizard tests are in `internal/configure/configure_test.go`.

| Test | Scenario | Expected |
|---|---|---|
| `TestLoadConfigValid` | valid JSON config file | parsed correctly, all fields populated |
| `TestLoadConfigFromEnvPath` | `GOPGMCP_CONFIG_PATH` set | reads from env path, not default location |
| `TestLoadConfigMissing` | no config file | returns error containing config path |
| `TestLoadConfigInvalidJSON` | malformed JSON | returns error containing `"invalid"` or `"unmarshal"` |
| `TestLoadConfigInvalidRegex` | invalid regex in sanitization rules | panics with message containing `"regex"` or `"compile"` and the invalid pattern |
| `TestLoadConfigDefaults_MaxResultLength` | config with `max_result_length` omitted (0) | defaults to `100000` |
| `TestLoadConfigValidation_NoPort` | config with server.port = 0 | verifies loaded config has port 0 (actual panic validation happens in `runServe()`, not tested here) |
| `TestLoadConfigValidation_ZeroMaxConns` | config with pool.max_conns = 0 | panics with message containing `"pool.max_conns"` |
| `TestLoadConfigValidation_ZeroDefaultTimeout` | config with `default_timeout_seconds` = 0 | panics with message containing `"default_timeout_seconds"` |
| `TestLoadConfigValidation_MissingDefaultTimeout` | config without `default_timeout_seconds` | panics with message containing `"default_timeout_seconds"` (no default, must be set) |
| `TestLoadConfigValidation_ZeroListTablesTimeout` | config with `list_tables_timeout_seconds` = 0 | panics with message containing `"list_tables_timeout_seconds"` |
| `TestLoadConfigValidation_ZeroDescribeTableTimeout` | config with `describe_table_timeout_seconds` = 0 | panics with message containing `"describe_table_timeout_seconds"` |
| `TestLoadConfigValidation_NegativeTimeout` | negative timeout value | panics with message containing `"timeout"` |
| `TestLoadConfigValidation_ZeroHookDefaultTimeout` | hooks configured but `default_hook_timeout_seconds` = 0 | panics with message containing `"default_hook_timeout_seconds"` |
| `TestLoadConfigValidation_MissingHookDefaultTimeout` | hooks configured but `default_hook_timeout_seconds` omitted | panics with message containing `"default_hook_timeout_seconds"` (no default, must be set) |
| `TestLoadConfigValidation_HookDefaultTimeoutNotRequiredWithoutHooks` | no hooks configured, `default_hook_timeout_seconds` omitted | no panic (validation only applies when hooks exist) |
| `TestLoadConfigValidation_HookTimeoutFallback` | hook with `timeout_seconds` = 0, `default_hook_timeout_seconds` = 10 | hook uses default (10s) |
| `TestLoadConfigValidation_HealthCheckPathEmpty` | `health_check_enabled` = true, `health_check_path` = "" | verifies loaded config has empty path with health check enabled (actual validation happens in `runServe()`, not tested here) |
| `TestLoadConfigValidation_HealthCheckPathNotRequiredWhenDisabled` | `health_check_enabled` = false, `health_check_path` = "" | no panic (path not needed when disabled) |
| `TestLoadConfigDefaults_MaxSQLLength` | config with `max_sql_length` omitted (0) | defaults to `100000` |
| `TestLoadConfigProtectionDefaults` | minimal config, no protection fields | all `Allow*` fields are `false` (Go zero-value = blocked), including `AllowCopyFrom`, `AllowCopyTo`, `AllowCreateFunction`, `AllowPrepare`, `AllowMerge`, `AllowGrantRevoke`, `AllowManageRoles`, `AllowCreateExtension`, `AllowLockTable`, `AllowListenNotify`, `AllowMaintenance`, `AllowDDL`, `AllowDiscard`, `AllowComment`, `AllowCreateTrigger`, `AllowCreateRule` |
| `TestLoadConfigProtectionExplicitAllow` | config with `allow_drop: true` | `AllowDrop` is `true`, all others remain `false` |
| `TestLoadConfigProtectionNewFields` | config with `allow_copy_from: true, allow_copy_to: true, allow_create_function: true, allow_prepare: true, allow_merge: true, allow_grant_revoke: true, allow_manage_roles: true, allow_create_extension: true, allow_lock_table: true, allow_listen_notify: true, allow_maintenance: true, allow_ddl: true, allow_discard: true, allow_comment: true, allow_create_trigger: true, allow_create_rule: true` | respective fields are `true`, others remain `false` |
| `TestLoadConfigSSLMode` | config with `sslmode: "verify-full"` | `Connection.SSLMode` is `"verify-full"` |
| `TestLoadConfigValidation_GoHooksAndCmdHooksMutuallyExclusive` | config with both `BeforeQueryHooks` (Go) and `WithServerHooks` option (command) | panics with message about mutual exclusivity |
| `TestLoadConfigValidation_GoHooksRequireDefaultTimeout` | config with `BeforeQueryHooks` set but `default_hook_timeout_seconds` = 0 | panics with message containing `"default_hook_timeout_seconds"` |
| `TestLoadConfigValidation_GoHooksOnlyNoCmd` | config with only `BeforeQueryHooks` (Go), no command hooks | no panic (valid configuration) |

---

### 6.7 Integration Tests (`integration_test.go`)

All integration tests use pgflock to acquire a real database. Build tag: `//go:build integration`

Run with: `go test -tags=integration -race -v ./...`

#### Query Tool Integration Tests

| Test | Setup | Action | Assert |
|---|---|---|---|
| `TestQuery_SelectBasic` | Create table with sample data | `SELECT * FROM users` | Returns correct JSON rows with matching column names and values |
| `TestQuery_SelectJSONB` | Table with JSONB column | `SELECT data FROM items` | JSONB returned as proper JSON object (map), not string |
| `TestQuery_JSONBReturnType` | Table with JSONB column containing nested objects, arrays, nulls, numbers, booleans | `SELECT data FROM items` | Validates that JSONB is returned as parsed Go map/slice (not string), even with `QueryExecModeExec`. Numbers inside JSONB preserved via `UseNumber()`. Tests the JSONB handling concern with real pgflock database. |
| `TestQuery_JSONBNumericPrecision` | Table with JSONB containing large integers e.g. `{"id": 9007199254740993}` (2^53+1) | `SELECT data FROM items` | **Known limitation:** large integer loses precision to float64 (`9.007199254740992e+15`) — this is a pgx limitation, not ours. Test documents the behavior. Users needing exact large integers in JSONB should store them as JSON strings. |
| `TestQuery_SelectArray` | Table with integer[] column | `SELECT tags FROM posts` | Array returned as JSON array |
| `TestQuery_SelectCTE` | Table with data | `WITH cte AS (SELECT ...) SELECT * FROM cte` | Correct results |
| `TestQuery_SelectNestedSubquery` | Multiple tables | Query with subqueries | Correct results |
| `TestQuery_Insert` | Empty table | `INSERT INTO users ... RETURNING *` | Returns inserted row with correct values |
| `TestQuery_Update` | Table with data | `UPDATE users SET ... WHERE ... RETURNING *` | Returns updated row with new values |
| `TestQuery_Delete` | Table with data | `DELETE FROM users WHERE ... RETURNING *` | Returns deleted row |
| `TestQuery_Transaction` | Table | `SELECT * FROM users` | Verify runs in transaction (data consistency) |
| `TestQuery_Timeout` | Table | `SELECT pg_sleep(10)` with 1s timeout | Error contains `"context deadline exceeded"` or `"canceling statement"` |
| `TestQuery_TimeoutRuleMatch` | Config with timeout rule matching query | Slow query | Uses rule timeout, not default |
| `TestQuery_ProtectionEndToEnd` | Table | `DROP TABLE users` | Error: `"DROP statements are not allowed"` |
| `TestQuery_HooksEndToEnd` | Config with real hook scripts | Query matching hook pattern | Hook executed, result correct |
| `TestQuery_HookCrashStopsPipeline` | Config with crash.sh hook | Any query matching hook | Error contains `"hook failed"` — query not executed |
| `TestQuery_HookTimeoutStopsPipeline` | Config with slow.sh, timeout=1s | Any query matching hook | Error contains `"hook timed out"` — query not executed |
| `TestQuery_HookBadJsonStopsPipeline` | Config with bad_json.sh hook | Any query matching hook | Error contains `"unparseable response"` — query not executed |
| `TestQuery_SanitizationEndToEnd` | Table with phone numbers | `SELECT phone FROM contacts` | Phone numbers sanitized per regex rules |
| `TestQuery_ErrorPromptEndToEnd` | No table | `SELECT * FROM nonexistent` | Error contains both Postgres error and appended prompt message |
| `TestQuery_MaxResultLength` | Table with many rows | `SELECT * FROM large_table` | Result truncated, error contains `"[truncated] Result is too long!"` |
| `TestQuery_ReadOnlyMode` | Config with read_only=true | `INSERT INTO users ...` | Error contains `"read-only transaction"` or `"cannot execute"` |
| `TestQuery_ReadOnlyModeBlocksSetBypass` | Config with read_only=true | `SET default_transaction_read_only = off` | Error: `"SET default_transaction_read_only is blocked in read-only mode"` |
| `TestQuery_Timezone` | Config with timezone="America/New_York" | `SELECT NOW()::timestamptz` | Returned timestamp contains "-05:00" or "-04:00" (EST/EDT) offset, not server default |
| `TestQuery_TimezoneUTC` | Config with timezone="UTC" | `SELECT NOW()::timestamptz` | Returned timestamp contains "+00:00" offset |
| `TestQuery_TimezoneEmpty` | Config with timezone="" (default) | `SELECT current_setting('timezone')` | Returns server's default timezone (not overridden) |
| `TestQuery_TimezoneWithReadOnly` | Config with timezone="Asia/Jakarta", read_only=true | `SELECT current_setting('timezone')` | Returns "Asia/Jakarta" — both AfterConnect settings applied |
| `TestQuery_NullValues` | Table with NULL columns | `SELECT * FROM ...` | NULL returned as JSON null (Go `nil`) |
| `TestQuery_UUIDColumn` | Table with UUID column | `SELECT id FROM ...` | UUID as formatted string `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"` |
| `TestQuery_TimestampColumn` | Table with timestamp column | `SELECT created_at FROM ...` | Timestamp as RFC3339Nano string |
| `TestQuery_NumericColumn` | Table with numeric(10,2) | `SELECT price FROM ...` | Numeric value as string (preserves precision, e.g. `"123.45"`) |
| `TestQuery_BigIntColumn` | Table with bigint column, value `9007199254740993` (2^53+1) | `SELECT big_id FROM ...` | Value preserved as exact integer (not float64-truncated) |
| `TestQuery_ByteaColumn` | Table with bytea column containing binary data | `SELECT avatar FROM ...` | Binary data returned as base64-encoded string |
| `TestQuery_EmptyResult` | Empty table | `SELECT * FROM empty_table` | Empty rows array, columns present |
| `TestQuery_NumericPrecisionWithHooks` | Config with AfterQuery accept.sh hook, table with bigint 2^53+1 | `SELECT big_id FROM ...` | Value survives JSON round-trip through hooks via `UseNumber()`, exact integer preserved |
| `TestQuery_NumericPrecisionWithoutHooks` | Config with no hooks, table with bigint 2^53+1 | `SELECT big_id FROM ...` | Value preserved (no JSON round-trip occurs) |
| `TestQuery_RowsAffected_Insert` | Empty table | `INSERT INTO users (name) VALUES ('a'), ('b'), ('c')` | `RowsAffected` = 3, Rows is nil (no RETURNING) |
| `TestQuery_RowsAffected_Update` | Table with 5 rows | `UPDATE users SET active = true WHERE id <= 3` | `RowsAffected` = 3 |
| `TestQuery_RowsAffected_Delete` | Table with 5 rows | `DELETE FROM users WHERE id <= 2` | `RowsAffected` = 2 |
| `TestQuery_RowsAffected_Select` | Table with 5 rows | `SELECT * FROM users` | `RowsAffected` = 5 (SELECT returns row count too) |
| `TestQuery_RowsAffected_InsertReturning` | Empty table | `INSERT INTO users (name) VALUES ('a') RETURNING *` | `RowsAffected` = 1, Rows has 1 row |
| `TestQuery_InetColumn` | Table with inet column | `SELECT ip FROM servers` | inet value returned as string (verify actual type from pgx with QueryExecModeExec) |
| `TestQuery_CidrColumn` | Table with cidr column | `SELECT network FROM subnets` | cidr value returned as string |
| `TestQuery_SemaphoreContention` | Config max_conns=1, hold semaphore via slow query | Attempt second query with short context timeout | Error contains `"failed to acquire query slot"` and `"connection slots are in use"` |
| `TestQuery_AfterHookRejectRollbacksWrite` | Config with AfterQuery hook that rejects, table with data | `INSERT INTO users (name) VALUES ('rejected_row')` | Error from hook rejection. Verify table does NOT contain 'rejected_row' — transaction was rolled back. |
| `TestQuery_AfterHookRejectSelectNoSideEffect` | Config with AfterQuery hook that rejects | `SELECT * FROM users` | Error from hook rejection. No side effect — SELECT is read-only, rollback called before hooks. |
| `TestQuery_AfterHookAcceptCommitsWrite` | Config with AfterQuery hook that accepts, table with data | `INSERT INTO users (name) VALUES ('accepted_row') RETURNING *` | Rows returned with accepted_row. Verify table contains 'accepted_row' — transaction was committed after hooks. |
| `TestQuery_ReadOnlyStatementRollbacksBeforeHooks` | Config with AfterQuery hook that tracks invocation time vs commit | `SELECT * FROM users` | Hook is called, but no commit occurs (rollback before hooks for SELECT). |
| `TestQuery_ExplainAnalyzeProtection` | Table | `EXPLAIN ANALYZE DELETE FROM users` | Error: `"DELETE without WHERE clause is not allowed"` |
| `TestQuery_UTF8Truncation` | Table with multi-byte UTF-8 data (e.g. emoji, CJK characters) | SELECT with low max_result_length | Truncated output is valid UTF-8 (no broken multi-byte sequences) |
| `TestQuery_MaxSQLLength` | Config with max_sql_length=100 | SQL string of 200 bytes | Error: `"SQL query too long: 200 bytes exceeds maximum of 100 bytes"` |
| `TestQuery_MaxSQLLength_ExactLimit` | Config with max_sql_length=20 | `SELECT 1` (under 20 bytes) | Query succeeds |
| `TestQuery_DDLBlocked` | Default config | `CREATE TABLE test (id int)` | Error: `"CREATE TABLE is not allowed: DDL operations are blocked"` |
| `TestQuery_DDLAllowed` | Config with AllowDDL=true | `CREATE TABLE test (id int)` | Table created successfully |
| `TestQuery_CreateExtensionBlocked` | Default config | `CREATE EXTENSION IF NOT EXISTS pg_trgm` | Error: `"CREATE EXTENSION is not allowed"` |
| `TestQuery_MaintenanceBlocked` | Default config | `ANALYZE users` | Error: `"VACUUM/ANALYZE is not allowed"` |
| `TestQuery_CreateTriggerBlocked` | Default config | `CREATE TRIGGER trg AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit()` | Error: `"CREATE TRIGGER is not allowed"` |
| `TestQuery_CreateRuleBlocked` | Default config | `CREATE RULE r AS ON INSERT TO users DO ALSO NOTIFY users_changed` | Error: `"CREATE RULE is not allowed"` |
| `TestQuery_TransactionControlBlocked` | Default config | `BEGIN` | Error: `"transaction control statements are not allowed"` |
| `TestQuery_CommitBlocked` | Default config | `COMMIT` | Error: `"transaction control statements are not allowed"` |
| `TestQuery_AlterExtensionBlocked` | Default config | `ALTER EXTENSION pg_trgm UPDATE` | Error: `"ALTER EXTENSION is not allowed"` |

#### Go Hook Integration Tests (Library Mode)

These tests verify the Go-interface hook pipeline used in library mode. No shell scripts — hooks are Go functions implementing `BeforeQueryHook`/`AfterQueryHook` interfaces. All use pgflock.

| Test | Setup | Action | Assert |
|---|---|---|---|
| `TestQuery_GoBeforeHook_Accept` | Config with BeforeQueryHook that returns query unchanged | `SELECT 1` | Query succeeds, result correct |
| `TestQuery_GoBeforeHook_Reject` | Config with BeforeQueryHook that returns error | `SELECT 1` | Error contains hook name and rejection message |
| `TestQuery_GoBeforeHook_ModifyQuery` | Config with BeforeQueryHook that changes query to `SELECT 2` | `SELECT 1` | Result contains `2`, not `1` |
| `TestQuery_GoBeforeHook_Chaining` | Config with two BeforeQueryHooks: first appends ` AS a`, second appends ` -- tagged` | `SELECT 1` | Modified query includes both modifications in order |
| `TestQuery_GoBeforeHook_Timeout` | Config with BeforeQueryHook that sleeps longer than timeout | `SELECT 1` | Error contains `"before_query hook error: hook timed out"` and hook name |
| `TestQuery_GoBeforeHook_PerHookTimeout` | Config with BeforeQueryHook: entry.Timeout=2s, default=1s, hook sleeps 1.5s | `SELECT 1` | Query succeeds (per-hook timeout of 2s > 1.5s sleep) |
| `TestQuery_GoBeforeHook_ProtectionStillApplied` | Config with BeforeQueryHook that modifies query to `DROP TABLE users` | `SELECT 1` | Error: `"DROP statements are not allowed"` — protection runs after hooks |
| `TestQuery_GoAfterHook_Accept` | Config with AfterQueryHook that returns result unchanged | `SELECT 1` | Result correct, no modification |
| `TestQuery_GoAfterHook_Reject` | Config with AfterQueryHook that returns error | `SELECT 1` | Error contains hook name and rejection message |
| `TestQuery_GoAfterHook_ModifyResult` | Config with AfterQueryHook that adds a column to result | `SELECT 1` | Result contains the added column |
| `TestQuery_GoAfterHook_Chaining` | Config with two AfterQueryHooks: first adds column, second appends row | `SELECT 1` | Result contains both modifications |
| `TestQuery_GoAfterHook_Timeout` | Config with AfterQueryHook that sleeps longer than timeout | `SELECT 1` | Error contains `"after_query hook error: hook timed out"` and hook name |
| `TestQuery_GoAfterHook_NoPrecisionLoss` | Config with AfterQueryHook (passthrough), table with bigint 2^53+1 | `SELECT big_id FROM ...` | Value preserved as exact int64 — no JSON round-trip, no float64 loss |
| `TestQuery_GoAfterHook_ReceivesNativeTypes` | Config with AfterQueryHook that type-asserts result fields | `SELECT 1::bigint, 'hello'::text` | Hook receives int64 and string (not json.Number or interface{}), confirms no serialization |
| `TestQuery_GoAfterHook_RejectRollbacksWrite` | Config with AfterQueryHook that rejects, table | `INSERT INTO users (name) VALUES ('go_rejected')` | Error from hook. Verify table does NOT contain 'go_rejected' — write rolled back. |
| `TestQuery_GoAfterHook_AcceptCommitsWrite` | Config with AfterQueryHook that accepts, table | `INSERT INTO users (name) VALUES ('go_accepted') RETURNING *` | Row returned. Verify table contains 'go_accepted' — write committed. |
| `TestQuery_GoAfterHook_SelectRollbacksBeforeHooks` | Config with AfterQueryHook that accepts | `SELECT * FROM users` | Result correct. Transaction rolled back before hook (read-only path). |
| `TestQuery_GoHooksMutualExclusion` | Config with both Go hooks and command hooks configured | `New()` call | Panics with message about mutual exclusivity |
| `TestQuery_GoHooksDefaultTimeoutRequired` | Config with Go hooks but `default_hook_timeout_seconds` = 0 | `New()` call | Panics with message containing `"default_hook_timeout_seconds"` |

#### pgx Type Verification Tests (pgflock)

These tests verify actual types returned by `rows.Values()` with `QueryExecModeExec` (simple protocol), validating all assumptions in `convertValue`. All use pgflock. Each test logs the actual Go type and verifies `convertValue` produces the expected output for every value variant (NULL, edge cases, typical values).

**File: `pgxtype_verification_test.go`** — build tag `integration`.

Each test function:
1. Creates a table with the target column type
2. Inserts multiple values covering edge cases
3. Queries with `QueryExecModeExec`
4. Logs the actual Go type via `%T`
5. Runs the value through `convertValue` and asserts the expected output

All Go types verified empirically. Assert columns reflect actual test results, not guesses.

##### Integer Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_SmallInt` | `smallint` | `0, 1, -1, 32767, -32768, NULL` | `int16` | Pass through. NULL → `nil`. |
| `TestPgxTypes_Integer` | `integer` | `0, 1, -1, 2147483647, -2147483648, NULL` | `int32` | Pass through. |
| `TestPgxTypes_BigInt` | `bigint` | `0, 1, -1, 9007199254740993 (2^53+1), max, min, NULL` | `int64` | Pass through. 2^53+1 preserved exactly. |
| `TestPgxTypes_Serial` | `serial` | INSERT DEFAULT then SELECT | `int32` | Same as integer. |
| `TestPgxTypes_BigSerial` | `bigserial` | INSERT DEFAULT then SELECT | `int64` | Same as bigint. |

##### Numeric / Decimal Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Numeric` | `numeric(20,5)` | `12345.67890, 0, -99999.99999, 0.00001, NULL` | `pgtype.Numeric` | String via MarshalJSON. NULL → `nil`. |
| `TestPgxTypes_NumericNoPrecision` | `numeric` | `123456789012345678901234567890, 0.000000000000000001` | `pgtype.Numeric` | Large/small values preserved as string. |
| `TestPgxTypes_NumericSpecial` | `numeric` | `'NaN', 'Infinity', '-Infinity', NULL` | `pgtype.Numeric` | NaN → `"NaN"`, Inf → `"Infinity"`. **json.Marshal fails on raw Inf — convertValue handles this.** |
| `TestPgxTypes_Real` | `real` | `0, 1.5, -1.5, max, min, NaN, +Inf, -Inf, NULL` | `float32` | Pass through. **NaN/Inf → string (json.Marshal fails on raw NaN/Inf).** |
| `TestPgxTypes_DoublePrecision` | `double precision` | `0, 1.5, -1.5, max, min, NaN, +Inf, -Inf, NULL` | `float64` | Pass through. **NaN/Inf → string.** |

##### Monetary Type

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Money` | `money` | `$0.00, $1234.56, -$99.99, $999999999.99, NULL` | `string` | Pass through. Returns locale-formatted string (e.g., `"$1,234.56"`). |

##### Character Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Char` | `char(10)` | `'hello', '', 'ñ', '日本語', NULL` | `string` | Pass through. Space-padded to 10 chars. |
| `TestPgxTypes_Varchar` | `varchar(255)` | `'hello', '', 'ñoño', '日本語テスト', NULL` | `string` | Pass through. |
| `TestPgxTypes_Text` | `text` | `'hello', '', 10000-char string, multi-line, special chars, emoji '🎉🚀', NULL` | `string` | Pass through. |

##### Binary Type

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_ByteA` | `bytea` | `decode('deadbeef','hex'), decode('','hex'), NULL` | `[]uint8` | base64 encode. Empty → `""`. |
| `TestPgxTypes_ByteA_JsonLookalike` | `bytea` | `'{"not":"json"}'::bytea` | `[]uint8` | base64 encode (no JSON auto-parsing — bytea is always base64, even if content looks like JSON). |

##### Date/Time Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_TimestampTZ` | `timestamptz` | `'2024-01-15 10:30:00+05:30', epoch, far future, NULL` | `time.Time` | RFC3339Nano string. All values normalized to UTC. |
| `TestPgxTypes_Timestamp` | `timestamp` | `'2024-01-15 10:30:00', epoch, far future, NULL` | `time.Time` | RFC3339Nano string. UTC location. |
| `TestPgxTypes_Date` | `date` | `'2024-01-15', epoch, '9999-12-31', 'epoch', NULL` | `time.Time` | RFC3339Nano string. Time component is midnight UTC. |
| `TestPgxTypes_Time` | `time` | `'10:30:00', '00:00:00', '23:59:59.999999', NULL` | `pgtype.Time` | Format as `"HH:MM:SS"` or `"HH:MM:SS.ffffff"`. Microseconds field. |
| `TestPgxTypes_TimeTZ` | `timetz` | `'10:30:00+05:30', '00:00:00+00', '23:59:59.999999-07', NULL` | `string` | Pass through (e.g., `"10:30:00+05:30"`). |
| `TestPgxTypes_Interval` | `interval` | `'1y 2m 3d 4h5m6s', '-3d -2h', '0s', '1m -5d 3h', '1y 2m', '1d 0.000001s', NULL` | `pgtype.Interval` | Human-readable string. Negative values preserved. |

##### Boolean Type

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Boolean` | `boolean` | `true, false, NULL` | `bool` | Pass through. |

##### UUID Type

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_UUID` | `uuid` | `uuid_generate_v4(), all-zeros, all-ff, NULL` | `[16]uint8` | Formatted `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`. |

##### Network Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Inet` | `inet` | `'192.168.1.1/24', host-only, ::1/128, fe80::1/64, 0.0.0.0/0, NULL` | `netip.Prefix` | `.String()` → `"192.168.1.1/24"`. Host address → `/32`. |
| `TestPgxTypes_Cidr` | `cidr` | `'10.0.0.0/8', '192.168.0.0/16', ::1/128, 2001:db8::/32, NULL` | `netip.Prefix` | `.String()`. |
| `TestPgxTypes_MacAddr` | `macaddr` | `'08:00:2b:01:02:03', 'ff:ff:ff:ff:ff:ff', NULL` | `net.HardwareAddr` | `.String()` → `"08:00:2b:01:02:03"`. **Must match before `[]byte` case.** |
| `TestPgxTypes_MacAddr8` | `macaddr8` | `'08:00:2b:01:02:03:04:05', NULL` | `net.HardwareAddr` | `.String()` → `"08:00:2b:01:02:03:04:05"`. |

##### JSON Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_JSONB_Object` | `jsonb` | `'{"name":"test","age":30}'` | `map[string]interface{}` | Recurse into values. |
| `TestPgxTypes_JSONB_Array` | `jsonb` | `'[1,2,3]'` | `[]interface{}` | Recurse into elements. |
| `TestPgxTypes_JSONB_Nested` | `jsonb` | `'{"a":{"b":{"c":[1,true,null]}}}'` | `map[string]interface{}` | Deep nesting preserved. |
| `TestPgxTypes_JSONB_Null` | `jsonb` | `'null'::jsonb` | `nil` | Return nil. |
| `TestPgxTypes_JSONB_ScalarString` | `jsonb` | `'"just a string"'::jsonb` | `string` | Pass through. |
| `TestPgxTypes_JSONB_ScalarNumber` | `jsonb` | `'42'::jsonb` | `float64` | Pass through (NaN/Inf check). |
| `TestPgxTypes_JSONB_ScalarBool` | `jsonb` | `'true'::jsonb, 'false'::jsonb` | `bool` | Pass through. |
| `TestPgxTypes_JSONB_LargeInt` | `jsonb` | `'{"id":9007199254740993}'` (2^53+1) | `map[string]interface{}` with `float64` | **Known limitation:** `9007199254740993` → `9.007199254740992e+15`. Precision lost inside pgx. |
| `TestPgxTypes_JSONB_Empty` | `jsonb` | `'{}'::jsonb, '[]'::jsonb` | `map[string]interface{}` / `[]interface{}` | Empty structures preserved. |
| `TestPgxTypes_JSONB_ColumnNull` | `jsonb` | SQL `NULL` column | `nil` | Return nil. |
| `TestPgxTypes_JSON` | `json` | `'{"key":"value"}', '[1,2]', 'null', NULL` | `map[string]interface{}` / `[]interface{}` / `nil` | **Same behavior as JSONB.** No difference in Go types. |

##### Array Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_TextArray` | `text[]` | `ARRAY['a','b','c'], empty, special chars, NULL` | `[]interface{}` | Recurse. Elements are `string`. |
| `TestPgxTypes_IntArray` | `int[]` | `ARRAY[1,2,3], empty, max/min, NULL` | `[]interface{}` | Recurse. Elements are `int32`. |
| `TestPgxTypes_BigIntArray` | `bigint[]` | `ARRAY[9007199254740993], NULL` | `[]interface{}` | Recurse. Element is `int64`, preserves precision. |
| `TestPgxTypes_BoolArray` | `boolean[]` | `ARRAY[true,false,true], NULL` | `[]interface{}` | Recurse. Elements are `bool`. |
| `TestPgxTypes_UUIDArray` | `uuid[]` | `ARRAY[uuid, uuid], NULL` | `[]interface{}` | Recurse. **Elements are `[16]uint8` — recursive convertValue formats as UUID strings.** |
| `TestPgxTypes_ArrayWithNulls` | `text[]` | `ARRAY['a',NULL,'c']` | `[]interface{}` | Recurse. NULL element → `nil` in slice. |
| `TestPgxTypes_2DArray` | `int[][]` | `ARRAY[[1,2],[3,4]]` | `[]interface{}` | **Flattened to 1D by pgx:** `[1,2,3,4]`. pgx limitation. |

##### Enum Type

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Enum` | `CREATE TYPE mood AS ENUM (...)` | `'happy', 'sad', 'neutral', NULL` | `string` | Pass through. |

##### Range Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Int4Range` | `int4range` | `'[1,10)', '(,)', '[5,5]', 'empty', NULL` | `pgtype.Range[interface{}]` | Format as `"[1,10)"`. Empty → `"empty"`. Unbounded → `"(,)"`. Lower/Upper are `int32`. |
| `TestPgxTypes_Int8Range` | `int8range` | `'[1,9223372036854775807)', NULL` | `pgtype.Range[interface{}]` | Format as range string. Lower/Upper are `int64`. |
| `TestPgxTypes_NumRange` | `numrange` | `'[1.5,10.5)', NULL` | `pgtype.Range[interface{}]` | Lower/Upper are `pgtype.Numeric` — recursive convertValue produces strings. |
| `TestPgxTypes_TsRange` | `tsrange` | `'[2024-01-01,2024-12-31)', NULL` | `pgtype.Range[interface{}]` | Lower/Upper are `time.Time` — recursive convertValue produces RFC3339. |
| `TestPgxTypes_TsTzRange` | `tstzrange` | `'[2024-01-01 00:00:00+00,2024-12-31 23:59:59+00)', NULL` | `pgtype.Range[interface{}]` | Same as tsrange with timezone normalization. |
| `TestPgxTypes_DateRange` | `daterange` | `'[2024-01-01,2024-12-31)', NULL` | `pgtype.Range[interface{}]` | Lower/Upper are `time.Time`. |

##### Geometric Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Point` | `point` | `'(1.5,2.5)', '(0,0)', '(-1.5,-2.5)', NULL` | `pgtype.Point` | `"(1.5,2.5)"`. Has `P` (Vec2) and `Valid` fields. |
| `TestPgxTypes_Line` | `line` | `'{1,2,3}', NULL` | `pgtype.Line` | `"{1,2,3}"`. Has `A`, `B`, `C`, `Valid` fields. |
| `TestPgxTypes_Lseg` | `lseg` | `'[(0,0),(1,1)]', NULL` | `pgtype.Lseg` | `"[(0,0),(1,1)]"`. Has `P` ([2]Vec2) and `Valid`. |
| `TestPgxTypes_Box` | `box` | `'(1,1),(0,0)', NULL` | `pgtype.Box` | `"(1,1),(0,0)"`. Has `P` ([2]Vec2) and `Valid`. |
| `TestPgxTypes_Path` | `path` | `'((0,0),(1,1),(2,0))', '[(0,0),(1,1)]', NULL` | `pgtype.Path` | Closed: `"((0,0),(1,1),(2,0))"`. Open: `"[(0,0),(1,1)]"`. Has `Points`, `Closed`, `Valid`. |
| `TestPgxTypes_Polygon` | `polygon` | `'((0,0),(1,0),(1,1),(0,1))', NULL` | `pgtype.Polygon` | `"((0,0),(1,0),(1,1),(0,1))"`. Has `Points` and `Valid`. |
| `TestPgxTypes_Circle` | `circle` | `'<(1,1),5>', NULL` | `pgtype.Circle` | `"<(1,1),5>"`. Has `Center` (Vec2), `Radius`, `Valid`. |

##### Bit String Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_Bit` | `bit(8)` | `B'10101010', B'00000000', B'11111111', NULL` | `pgtype.Bits` | Bit string `"10101010"`. Has `Bytes`, `Len` (int32), `Valid`. |
| `TestPgxTypes_VarBit` | `bit varying(16)` | `B'1', B'10101010', B'1010101010101010', NULL` | `pgtype.Bits` | Same handling. `Len` tracks exact bit count. |

##### Text Search Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_TsVector` | `tsvector` | `to_tsvector('english', 'the quick brown fox'), NULL` | `string` | Pass through (e.g., `"'brown':3 'fox':4 'quick':2"`). |
| `TestPgxTypes_TsQuery` | `tsquery` | `to_tsquery('english', 'quick & fox'), NULL` | `string` | Pass through (e.g., `"'quick' & 'fox'"`). |

##### XML Type

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_XML` | `xml` | `'<root><item>test</item></root>'::xml, NULL` | `[]uint8` | **Same Go type as bytea — base64 encoded.** Users needing text should cast: `v::text`. |

##### Special / Composite Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_CompositeType` | `CREATE TYPE address AS (...)` | `ROW('123 Main St','Springfield','62701')::address, NULL` | `string` | Pass through. Returns text representation: `"(\"123 Main St\",Springfield,62701)"`. |
| `TestPgxTypes_Domain` | `CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)` | `1, 100, NULL` | `int32` | Maps to underlying type. Same as integer. |

##### NULL Handling Across Types

| Test | Postgres Type | Test Values | Verified Go Type | convertValue |
|---|---|---|---|---|
| `TestPgxTypes_AllNulls` | Table with 16 columns of different types, all NULL | `SELECT *` | All `nil` | `convertValue` returns `nil` for all. |

##### json.Marshal Round-Trip Verification

Each test verifies that every returned value can be serialized via `json.Marshal` without error. The test helper `queryAndLog` calls `json.Marshal` on each value and reports failures. Known json.Marshal failures that require convertValue handling:
- `float32`/`float64` NaN, +Inf, -Inf → `"json: unsupported value"` — convertValue converts to string
- `pgtype.Numeric` Infinity/-Infinity → `MarshalJSON()` produces invalid bytes — convertValue converts to string
- All other types pass json.Marshal after convertValue processing

#### ListTables Integration Tests

| Test | Setup | Assert |
|---|---|---|
| `TestListTables_Basic` | Create 3 tables | Returns all 3 with correct names/types |
| `TestListTables_IncludesViews` | Create table + view | View included with type "view" |
| `TestListTables_IncludesMaterializedViews` | Create mat view | Included with type "materialized_view" |
| `TestListTables_ExcludesSystemTables` | Default DB | No pg_catalog/information_schema tables |
| `TestListTables_IncludesPartitionedTables` | Create partitioned table | Included with type "partitioned_table" |
| `TestListTables_SchemaAccessLimited` | Create schema, grant SELECT on table but revoke USAGE on schema | Table listed with `SchemaAccessLimited: true` |
| `TestListTables_SchemaAccessNormal` | Table in public schema | `SchemaAccessLimited: false` |
| `TestListTables_Empty` | Empty database (no user tables) | Empty list |
| `TestListTables_Timeout` | Config with list_tables_timeout=1s, `pg_sleep` in custom view or slow DB | Error contains context deadline exceeded |
| `TestListTables_AcquiresSemaphore` | Config max_conns=1, hold semaphore in another goroutine | ListTables blocks, then succeeds when semaphore released; or times out if held too long |
| `TestListTables_SemaphoreContention` | Config max_conns=1, hold semaphore, short context timeout | Error contains `"failed to acquire query slot"` |

#### DescribeTable Integration Tests

| Test | Setup | Assert |
|---|---|---|
| `TestDescribeTable_Columns` | Table with various types | All columns with correct types, nullability |
| `TestDescribeTable_PrimaryKey` | Table with PK | PK column marked, constraint listed |
| `TestDescribeTable_Indexes` | Table with indexes | All indexes listed with definitions |
| `TestDescribeTable_ForeignKeys` | Two tables with FK | FK listed with referenced table/columns |
| `TestDescribeTable_UniqueConstraint` | Table with UNIQUE | Constraint listed |
| `TestDescribeTable_CheckConstraint` | Table with CHECK | Constraint listed |
| `TestDescribeTable_DefaultValues` | Table with defaults | Default values shown |
| `TestDescribeTable_SchemaQualified` | Table in custom schema | schema parameter works |
| `TestDescribeTable_NotFound` | No such table | Error contains `"does not exist"` or `"not found"` |
| `TestDescribeTable_View` | View over users table | Type="view", columns listed, Definition contains view SQL, no indexes/constraints/FKs |
| `TestDescribeTable_MaterializedView` | Materialized view over users | Type="materialized_view", columns from pg_attribute (not information_schema), Definition contains SQL, indexes listed (matviews can have indexes) |
| `TestDescribeTable_MaterializedViewWithIndex` | Matview + CREATE INDEX | Index listed in indexes array |
| `TestDescribeTable_ForeignTable` | Foreign table (if pg_fdw available) | Type="foreign_table", columns listed |
| `TestDescribeTable_PartitionedTable` | Partitioned table (RANGE on created_at) with 2 child partitions | Type="partitioned_table", Partition.Strategy="range", Partition.PartitionKey contains "created_at", Partition.Partitions lists child names |
| `TestDescribeTable_PartitionedTableList` | Partitioned table (LIST on region) | Partition.Strategy="list" |
| `TestDescribeTable_PartitionedTableHash` | Partitioned table (HASH on id) | Partition.Strategy="hash" |
| `TestDescribeTable_ChildPartition` | Child partition of a partitioned table | Type="table", Partition.ParentTable set to parent name |
| `TestDescribeTable_DefaultSchemaPublic` | Table in public schema, schema input omitted | Correctly describes the table (defaults to "public") |
| `TestDescribeTable_Timeout` | Config with describe_table_timeout=1s, table with slow function-based defaults or slow DB | Error contains context deadline exceeded |
| `TestDescribeTable_AcquiresSemaphore` | Config max_conns=1, hold semaphore in another goroutine | DescribeTable blocks, then succeeds when semaphore released; or times out if held too long |
| `TestDescribeTable_SemaphoreContention` | Config max_conns=1, hold semaphore, short context timeout | Error contains `"failed to acquire query slot"` |

#### Full Pipeline Integration Test

| Test | Description |
|---|---|
| `TestFullPipeline` | Configure BeforeQuery hook (modify query) + protection + real query + AfterQuery hook + sanitization + error prompts. Assert each stage applied in correct order. |

#### MCP Server Integration Tests

| Test | Description |
|---|---|
| `TestMCPServer_QueryTool` | Start MCP HTTP server, send JSON-RPC `tools/call` for query, verify response |
| `TestMCPServer_ListTablesTool` | Same for list_tables |
| `TestMCPServer_DescribeTableTool` | Same for describe_table |
| `TestMCPServer_HealthCheck` | GET health check endpoint, verify 200 OK |
| `TestMCPServer_HealthCheckAndMCPCoexist` | Both health check AND MCP endpoint respond correctly on the same port (verifies manual mux registration approach) |
| `TestMCPServer_ToolsList` | `tools/list` returns all 3 tools with correct schemas |

---

### 6.8 Stress Tests (`stress_test.go`)

Build tag: `//go:build integration`

| Test | Description |
|---|---|
| `TestStress_ConcurrentQueries` | Spawn 50 goroutines each running 20 queries. Assert all complete without error. Verify total time is reasonable (bounded by pool size, not sequential). |
| `TestStress_SemaphoreLimit` | Config max_conns=3. Spawn 20 goroutines with `SELECT pg_sleep(0.1)`. Smoke test: validates no deadlocks or errors under contention. Note: the atomic counter tracks goroutines inside `Query()` (including pre/post-DB work), not actual concurrent DB connections, so it does not assert `maxConcurrent <= MaxConns`. |
| `TestStress_LargeResultTruncation` | Insert 10,000 rows. `SELECT *` with max_result_length=1000. Assert error contains `"[truncated] Result is too long! Add limits in your query!"`. |
| `TestStress_ConcurrentHooks` | Config with hooks. Spawn 20 goroutines querying. Assert hooks run correctly under concurrency (no data races, no cross-contamination). |
| `TestStress_MixedOperations` | Concurrent mix of Query, ListTables, DescribeTable. Assert all complete correctly. |

### 6.9 Race Condition Tests

All tests above run with `-race` flag. Additionally:

| Test | Description |
|---|---|
| `TestRace_ConcurrentSanitization` | 10 goroutines sanitizing different data with same Sanitizer instance. Assert no races. |
| `TestRace_ConcurrentProtectionCheck` | 10 goroutines checking different SQL with same Checker. Assert no races. |
| `TestRace_ConcurrentErrorPrompt` | 10 goroutines matching different errors with same Matcher. Assert no races. |
| `TestRace_ConcurrentTimeout` | 10 goroutines resolving timeouts with same Manager. Assert no races. |

These tests verify that all shared components are safe for concurrent use. Each goroutine must use its own copy of mutable data (maps/slices) since `SanitizeRows` mutates values in-place:
```go
func TestRace_ConcurrentSanitization(t *testing.T) {
    s, _ := sanitize.NewSanitizer(rules)
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                // Each iteration gets a fresh copy — SanitizeRows mutates in-place
                data := copyTestData(testData)
                s.SanitizeRows(data)
            }
        }()
    }
    wg.Wait()
}
```

---

## Phase 7: Implementation Order

Strict implementation order to ensure testability at each step:

| Step | What | Depends On | Tests |
|---|---|---|---|
| 1 | `go mod init`, add dependencies | — | — |
| 2 | `config.go` — config structs + loading + validation | Step 1 | Config unit tests |
| 3 | `internal/protection/` — SQL protection checker | Step 1 | Protection unit tests (no DB) |
| 4 | `internal/sanitize/` — sanitization engine | Step 1 | Sanitization unit tests (no DB) |
| 5 | `internal/errprompt/` — error prompt matcher | Step 1 | Error prompt unit tests (no DB) |
| 6 | `internal/timeout/` — timeout manager | Step 1 | Timeout unit tests (no DB) |
| 7 | `internal/hooks/` — hook runner + testdata scripts | Step 1 | Hook unit tests (no DB, uses test scripts) |
| 8 | `pgmcp.go` — PostgresMcp struct, New(), Close() | Steps 2-7 | — |
| 9 | `query.go` — Query tool with full pipeline | Step 8 | Query integration tests (pgflock) |
| 10 | `listtables.go` — ListTables tool | Step 8 | ListTables integration tests (pgflock) |
| 11 | `describetable.go` — DescribeTable tool | Step 8 | DescribeTable integration tests (pgflock) |
| 12 | `mcp.go` — MCP tool registration | Steps 9-11 | MCP server integration tests |
| 13 | `cmd/gopgmcp/serve.go` — serve command | Step 12 | Manual testing |
| 14 | `internal/configure/` + `cmd/gopgmcp/configure.go` | Step 2 | Configure unit tests |
| 15 | `cmd/gopgmcp/main.go` — CLI entrypoint | Steps 13-14 | — |
| 16 | Full pipeline integration tests | Steps 9-12 | `TestFullPipeline` |
| 17 | Stress tests | Steps 9-12 | Stress tests (pgflock) |
| 18 | Race condition tests | Steps 3-7 | Race tests with `-race` |
| 19 | Documentation | Steps 1-18 | Code documentation (see Phase 8) |

---

## Verification

### Running Unit Tests (no DB required)
```bash
go test ./internal/... -v -race
```

### Running Integration Tests (requires pgflock)
```bash
# Start pgflock first
cd /home/ricky/Personal/postgres-mcp && pgflock up

# In another terminal
go test -tags=integration -v -race ./...
```

### Running Stress Tests
```bash
go test -tags=integration -run TestStress -v -race -timeout 120s ./...
```

### Manual MCP Server Testing
```bash
# Start server
GOPGMCP_PG_CONNSTRING="postgres://user:pass@localhost:5432/mydb" gopgmcp serve

# Test with curl (JSON-RPC)
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 1 AS test"}}}'

# Test health check
curl http://localhost:8080/health-check
```

### All Tests
```bash
# Unit + integration + stress + race detection
go test -tags=integration -v -race -timeout 180s ./...
```

---

## Phase 8: Documentation

Documentation is written **after all tests pass**. All documentation lives in code comments and a single README. The following information must be clearly documented:

### README.md

Must include:

1. **Security warning (prominent, at the top):**
   - This MCP server has **no authentication**. It is designed for local or trusted environments only (local machine, internal network services).
   - **Never expose to the public internet.** No CORS headers are set intentionally — CORS is only enforced by browsers, and intended clients (AI agents, CLI tools) use plain HTTP. Not setting CORS prevents malicious webpages from accessing an accidentally-exposed server.

2. **Quick start** — installation, config file creation, running the server.

3. **Configuration reference** — all config fields with types, defaults, and descriptions:
   - Connection (host, port, dbname, sslmode) — used when `GOPGMCP_PG_CONNSTRING` env var is not set
   - Pool settings (mirrors pgxpool config)
   - Server settings (port, read_only, timezone, health check)
   - Protection rules (SET, DROP, TRUNCATE, DO blocks, DELETE/UPDATE WHERE, DDL, CREATE EXTENSION/ALTER EXTENSION, CREATE TRIGGER, CREATE RULE, LOCK TABLE, LISTEN/NOTIFY, maintenance (VACUUM/ANALYZE/CLUSTER/REINDEX/REFRESH MATERIALIZED VIEW), DISCARD, COMMENT ON, MERGE, GRANT/REVOKE, role management, transaction control) — togglable ones blocked by default (`false` = blocked, `true` = allowed); transaction control always blocked (no toggle)
   - Query settings (default timeout, max SQL length, max result length, timeout rules)
   - Error prompts (regex → message, evaluated against ALL errors including hook/Go errors)
   - Sanitization rules (regex → replacement, applied per-field recursively into JSONB/arrays)
   - Hooks (before_query, after_query) with command, args, pattern, and timeout

4. **Hook security documentation:**
   - Go's `exec.Command` passes no shell context. The hook binary receives raw bytes on stdin. No injection possible at the transport level.
   - Hook commands are executed directly (not through a shell). The `command` field is the executable path, `args` is an array of arguments passed separately.
   - If a hook author does something reckless like `eval $(cat /dev/stdin)`, that's on them. The MCP server itself doesn't create the vulnerability.
   - Hook timeout is enforced. Default hook timeout must be > 0 — server refuses to start otherwise.
   - When a hook crashes, times out, returns non-zero exit code, or returns unparseable content, the entire query pipeline is stopped and the query is rejected with a descriptive error. Hooks are critical guardrails — a failing hook means the guardrail cannot verify the query/result.

5. **Health check documentation:**
   - Health check endpoint confirms the MCP server process is running and HTTP is responsive.
   - It does **NOT** check database connectivity. Use it for k8s liveness probes, not readiness probes.

6. **Library mode documentation:**
   - How to use `pgmcp.New()` to create an instance and call `Query()`, `ListTables()`, `DescribeTable()` directly.
   - `Query()` returns only `*QueryOutput` (no Go error) — all errors are in `output.Error`, already evaluated against error_prompts.
   - Config must be built and passed programmatically in library mode.

7. **Tool reference:**
   - Query: full pipeline (SQL length check → hooks → protection → timeout → execute → hooks → sanitization → truncation → logging), returns JSON with `rows_affected` count
   - ListTables: lists tables, views, materialized views, foreign tables, partitioned tables accessible to the connected user. Flags tables with restricted schema access.
   - DescribeTable: returns schema details including columns, indexes, constraints, foreign keys, partition info, and view definitions. Defaults schema to "public".

8. **Logging documentation:**
   - Query logging: all successful queries logged at Info with SQL (truncated), duration, row count, rows affected. Errors logged at Error with SQL and error message.
   - ListTables/DescribeTable: logged at Info with duration and result counts.
   - AI agent sessions: MCP `initialize` calls logged at Info with client name/version.
   - SET commands through the Query tool are classified as read-only and rolled back — they have no lasting session effect. Use `Config.Timezone` and `Config.ReadOnly` for session-level settings.

9. **Known limitations:**
   - `QueryExecModeExec` (simple protocol) prevents SQL injection and multi-statement queries but may affect type mapping for some Postgres types
   - Multi-statement queries are always rejected (cannot be toggled off)
   - Transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, etc.) are always blocked — each query runs in its own managed transaction with AfterQuery hooks as guardrails
   - Protection checks work at the SQL AST level — they cannot inspect dynamic SQL inside PL/pgSQL functions (DO blocks, CREATE FUNCTION/PROCEDURE, and CREATE TRIGGER are blocked by default)
   - EXPLAIN ANALYZE always checks its inner statement against protection rules
   - SET commands through the Query tool are blocked by default (AllowSet=false). Even when allowed, SET is classified as read-only and rolled back — session settings should use Config.Timezone/ReadOnly via AfterConnect
   - Query timeout (queryCtx) covers the entire pipeline including commit — if AfterQuery hooks consume significant time, the remaining budget for commit may be insufficient. This is by design: no write persists unless the full pipeline completes within the timeout. Users should set timeouts that account for both query execution and hook processing

### Code Comments

- All exported types and functions must have godoc comments
- Internal packages should have package-level doc comments explaining their purpose
- Complex logic (AST walking, JSONB handling, hook middleware chain) should have inline comments
