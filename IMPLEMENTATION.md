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

type Config struct {
    Connection   ConnectionConfig   `json:"connection"`
    Pool         PoolConfig         `json:"pool"`
    Server       ServerConfig       `json:"server"`
    Logging      LoggingConfig      `json:"logging"`
    Protection   ProtectionConfig   `json:"protection"`
    Query        QueryConfig        `json:"query"`
    ErrorPrompts []ErrorPromptRule  `json:"error_prompts"`
    Sanitization []SanitizationRule `json:"sanitization"`
    Hooks        HooksConfig        `json:"hooks"`
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

type ServerConfig struct {
    Port               int    `json:"port"`
    HealthCheckEnabled bool   `json:"health_check_enabled"`
    HealthCheckPath    string `json:"health_check_path"`
    ReadOnly           bool   `json:"read_only"`
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
    AllowDeleteWithoutWhere bool `json:"allow_delete_without_where"`
    AllowUpdateWithoutWhere bool `json:"allow_update_without_where"`
}

type QueryConfig struct {
    DefaultTimeoutSeconds        int            `json:"default_timeout_seconds"`
    ListTablesTimeoutSeconds     int            `json:"list_tables_timeout_seconds"`
    DescribeTableTimeoutSeconds  int            `json:"describe_table_timeout_seconds"`
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

type HooksConfig struct {
    DefaultTimeoutSeconds int         `json:"default_timeout_seconds"`
    BeforeQuery           []HookEntry `json:"before_query"`
    AfterQuery            []HookEntry `json:"after_query"`
}

type HookEntry struct {
    Pattern        string   `json:"pattern"`
    Command        string   `json:"command"`
    Args           []string `json:"args"`
    TimeoutSeconds int      `json:"timeout_seconds"`
}
```

**Config loading logic** (internal to `pgmcp.go` or `cmd/`):
1. Check `GOPGMCP_CONFIG_PATH` env var → use that path
2. Otherwise use `<cwd>/.gopgmcp/config.json`
3. Parse JSON → `Config` struct
4. Validate: compile all regex patterns, check required fields (server.port), check timeout values > 0
5. Return descriptive errors on validation failure

**Config defaults** (applied before validation, when fields are zero-value):
- `protection.*` → all `false` (Go zero-value = blocked, safe default; set to `true` to allow specific operations)
- `query.max_result_length` → `100000` (when 0; cannot be disabled — there is no "no limit" option)

**Config validation** (server panics on start if any fail — runs after defaults):
- `server.port` must be specified and > 0
- `pool.max_conns` must be > 0 — a zero-capacity semaphore would deadlock all queries
- `query.default_timeout_seconds` must be > 0 — no default, user must explicitly set this
- `query.list_tables_timeout_seconds` must be > 0 — no default, user must explicitly set this
- `query.describe_table_timeout_seconds` must be > 0 — no default, user must explicitly set this
- `query.max_result_length` must be > 0 (guaranteed after defaults, but explicit validation for safety)
- `hooks.default_timeout_seconds` must be > 0 if any hooks are configured — no default, user must explicitly set this
- `server.health_check_path` must be non-empty if `server.health_check_enabled` is true — no default, user must explicitly set this
- All regex patterns must compile successfully
- All per-hook and per-rule timeout values must be > 0

---

## Phase 2: Internal Packages

### 2.1 Protection Checker

**File: `internal/protection/protection.go`**

Each internal package defines its own config type to avoid circular imports with the parent `pgmcp` package. The `pgmcp` package maps its config to internal configs when constructing components.

```go
package protection

// Config is the protection checker's own config type.
// The pgmcp package maps ProtectionConfig + ServerConfig.ReadOnly → this.
type Config struct {
    AllowSet                bool
    AllowDrop               bool
    AllowTruncate           bool
    AllowDo                 bool
    AllowDeleteWithoutWhere bool
    AllowUpdateWithoutWhere bool
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

    case *pg_query.Node_TransactionStmt:
        // readOnly: block BEGIN READ WRITE / START TRANSACTION READ WRITE
        if c.config.ReadOnly {
            txStmt := n.TransactionStmt
            for _, opt := range txStmt.Options {
                if defElem, ok := opt.Node.(*pg_query.Node_DefElem); ok {
                    if defElem.DefElem.Defname == "transaction_read_only" {
                        // Check if the value is false (= READ WRITE)
                        if intVal, ok := defElem.DefElem.Arg.Node.(*pg_query.Node_Integer); ok {
                            if intVal.Integer.Ival == 0 { // 0 = false = READ WRITE
                                return fmt.Errorf("BEGIN READ WRITE is blocked in read-only mode: cannot start a read-write transaction")
                            }
                        }
                    }
                }
            }
        }
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
// The pgmcp package maps HooksConfig → this, converting seconds to time.Duration.
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

func NewRunner(config Config, logger zerolog.Logger) (*Runner, error)
// Compiles all regex patterns, returns error on invalid regex.
// For each hook: if Timeout > 0, uses that; otherwise falls back to config.DefaultTimeout.
// Panics if config.DefaultTimeout == 0 and any hook exists (validated at config load, but defense-in-depth).

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
    output, err := cmd.Output()
    if err != nil {
        // Hooks are critical guardrails — any failure stops the pipeline.
        // This covers: non-zero exit code, crash, timeout (context deadline exceeded).
        if ctx.Err() == context.DeadlineExceeded {
            return nil, fmt.Errorf("hook timed out: %s", hook.command)
        }
        return nil, fmt.Errorf("hook failed (command: %s): %w", hook.command, err)
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

func NewSanitizer(rules []Rule) (*Sanitizer, error)
// Compiles all regex patterns.

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

func NewMatcher(rules []Rule) (*Matcher, error)

// Match checks error message against all rules (top to bottom).
// Returns concatenation of all matching prompt messages.
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

func NewManager(config Config) (*Manager, error)

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
- For each array field (error_prompts, sanitization, timeout_rules, hooks.before_query, hooks.after_query):
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
    hooks      *hooks.Runner
    sanitizer  *sanitize.Sanitizer
    errPrompts *errprompt.Matcher
    timeoutMgr *timeout.Manager
    logger     zerolog.Logger
}

// New creates a new PostgresMcp instance.
// connString is the PostgreSQL connection string.
// If empty, connection details are read from config.Connection.
// The username and password must be embedded in connString or config when using library mode.
func New(ctx context.Context, connString string, config Config, logger zerolog.Logger) (*PostgresMcp, error)

// Close closes the connection pool.
func (p *PostgresMcp) Close()
```

**New() logic:**
1. Validate: if `config.Pool.MaxConns <= 0`, panic (`"pool.max_conns must be > 0"`).
2. Build connection string: if `connString` is non-empty, use it. Otherwise build from `config.Connection` fields (host, port, dbname, sslmode).
3. Configure `pgxpool.Config`: apply pool settings, set `DefaultQueryExecMode` to `pgx.QueryExecModeExec`.
4. If `config.Server.ReadOnly`, set `AfterConnect` hook to run `SET default_transaction_read_only = on` on each connection.
5. Create `pgxpool.Pool`.
6. Create semaphore: `make(chan struct{}, config.Pool.MaxConns)` — bounds concurrent query pipelines.
7. Initialize all internal components, mapping pgmcp config types to internal package config types:
   - `protection.NewChecker(protection.Config{AllowSet: config.Protection.AllowSet, ..., ReadOnly: config.Server.ReadOnly})`
   - `hooks.NewRunner(hooks.Config{DefaultTimeout: time.Duration(config.Hooks.DefaultTimeoutSeconds) * time.Second, ...}, logger)`
   - `sanitize.NewSanitizer(mapSanitizationRules(config.Sanitization))`
   - `errprompt.NewMatcher(mapErrorPromptRules(config.ErrorPrompts))`
   - `timeout.NewManager(timeout.Config{DefaultTimeout: time.Duration(config.Query.DefaultTimeoutSeconds) * time.Second, ListTablesTimeout: time.Duration(config.Query.ListTablesTimeoutSeconds) * time.Second, DescribeTableTimeout: time.Duration(config.Query.DescribeTableTimeoutSeconds) * time.Second, ...})`
8. Return `*PostgresMcp`.

### 3.2 Query Tool

**File: `query.go`**

```go
type QueryInput struct {
    SQL string `json:"sql"`
}

type QueryOutput struct {
    Columns []string                 `json:"columns"`
    Rows    []map[string]interface{} `json:"rows"`
    Error   string                   `json:"error,omitempty"`
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
    sql := input.SQL

    // 1. Acquire semaphore (respects context cancellation to prevent deadlock)
    select {
    case p.semaphore <- struct{}{}:
    case <-ctx.Done():
        return p.handleError(ctx.Err())
    }
    defer func() { <-p.semaphore }()

    // 2. Run BeforeQuery hooks (middleware chain)
    var err error
    sql, err = p.hooks.RunBeforeQuery(ctx, sql)
    if err != nil {
        return p.handleError(err)
    }

    // 3. Protection check (on potentially modified query)
    if err := p.protection.Check(sql); err != nil {
        return p.handleError(err)
    }

    // 4. Determine timeout
    timeout := p.timeoutMgr.GetTimeout(sql)
    queryCtx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    // 5. Acquire connection and execute in transaction
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

    // 6. Collect results
    result, err := p.collectRows(rows)
    if err != nil {
        return p.handleError(err)
    }

    if err := tx.Commit(queryCtx); err != nil {
        return p.handleError(err)
    }

    // 7-9. AfterQuery hooks — only serialize/deserialize if hooks are configured.
    // This avoids unnecessary JSON round-trip that would lose numeric precision.
    var finalResult *QueryOutput
    if p.hooks.HasAfterQueryHooks() {
        // Serialize to JSON for AfterQuery hooks (complete result: columns + rows + error)
        resultJSON, err := json.Marshal(result)
        if err != nil {
            return p.handleError(err)
        }

        // Run AfterQuery hooks (middleware chain)
        modifiedJSON, err := p.hooks.RunAfterQuery(ctx, string(resultJSON))
        if err != nil {
            return p.handleError(err)
        }

        // Parse back modified result — use json.NewDecoder with UseNumber()
        // to preserve numeric precision (prevents int64 → float64 lossy conversion).
        finalResult = &QueryOutput{}
        dec := json.NewDecoder(strings.NewReader(modifiedJSON))
        dec.UseNumber()
        if err := dec.Decode(finalResult); err != nil {
            return p.handleError(err)
        }
    } else {
        finalResult = result
    }

    // 10. Apply sanitization (per-field, recursive into JSONB/arrays)
    finalResult.Rows = p.sanitizer.SanitizeRows(finalResult.Rows)

    // 11. Apply max result length truncation (keeps partial data — may be garbled JSON but still useful for agents)
    p.truncateIfNeeded(finalResult)

    return finalResult
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

    var resultRows []map[string]interface{}
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

    return &QueryOutput{Columns: columns, Rows: resultRows}, rows.Err()
}
```

**convertValue logic** — ensures all Postgres types are properly converted to JSON-friendly Go types. Handles:
- `nil` → JSON null
- `time.Time` → ISO 8601 string
- `net.IPNet` → string
- `pgtype.Numeric` → string (preserves full precision for financial/scientific data)
- `[16]byte` (UUID) → formatted UUID string
- `[]byte` → base64-encoded string (for bytea columns; also checks if content is valid JSON first for JSONB)
- `map[string]interface{}` / `[]interface{}` → pass through (JSONB/arrays already parsed by pgx)
- `string` that looks like JSON (for JSONB columns) → attempt `json.NewDecoder` with `UseNumber()` to preserve numeric precision; if fails, keep as string
- Other types → let `json.Marshal` handle

```go
func convertValue(v interface{}) interface{} {
    switch val := v.(type) {
    case nil:
        return nil
    case time.Time:
        return val.Format(time.RFC3339Nano)
    case net.IPNet:
        return val.String()
    case pgtype.Numeric:
        // Always use string representation to preserve full precision.
        // float64 silently loses precision for high-precision numeric values
        // (e.g., financial data with many decimal places).
        return val.String()
    case [16]byte:
        // UUID
        return fmt.Sprintf("%x-%x-%x-%x-%x", val[0:4], val[4:6], val[6:8], val[8:10], val[10:16])
    case []byte:
        // Check if it's JSONB content first (pgx may return JSONB as []byte with QueryExecModeExec)
        if len(val) > 0 && (val[0] == '{' || val[0] == '[') {
            dec := json.NewDecoder(bytes.NewReader(val))
            dec.UseNumber()
            var parsed interface{}
            if err := dec.Decode(&parsed); err == nil {
                return parsed
            }
        }
        // Binary data (bytea) → base64-encoded string
        return base64.StdEncoding.EncodeToString(val)
    case string:
        // For JSONB columns returned as strings by QueryExecModeExec
        if len(val) > 0 && (val[0] == '{' || val[0] == '[') {
            dec := json.NewDecoder(strings.NewReader(val))
            dec.UseNumber()
            var parsed interface{}
            if err := dec.Decode(&parsed); err == nil {
                return parsed
            }
        }
        return val
    case map[string]interface{}, []interface{}:
        return val
    default:
        return val
    }
}
```

**JSONB handling concern:** With `pgx.QueryExecModeExec` (simple protocol), pgx may return JSONB columns as raw `string` or `[]byte` instead of parsed Go maps, because the simple protocol skips the type description step that pgx uses for automatic type mapping. The `convertValue` function handles this by checking if a `string` or `[]byte` value is valid JSON and unmarshaling it with `UseNumber()` to preserve numeric precision. This concern is validated by integration tests using pgflock (see `TestQuery_JSONBReturnType`).

**[]byte / bytea handling:** Binary data that is not valid JSON is encoded as base64, which is the standard representation of binary data in JSON. This applies to bytea columns.

**handleError logic:**
```go
// handleError converts any error into a QueryOutput with error message.
// The error message is always evaluated against error_prompts — matching
// prompt messages are appended. This applies to ALL errors: Postgres errors,
// protection rejections, hook rejections, hook error messages, Go errors.
func (p *PostgresMcp) handleError(err error) *QueryOutput {
    errMsg := err.Error()
    // Check error prompts (evaluated against ALL error messages)
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
    if p.config.Query.MaxResultLength <= 0 {
        return
    }
    jsonBytes, _ := json.Marshal(output.Rows)
    if len(jsonBytes) > p.config.Query.MaxResultLength {
        // Re-serialize truncated
        truncated := string(jsonBytes[:p.config.Query.MaxResultLength])
        output.Rows = nil
        output.Error = truncated + "...[truncated] Result is too long! Add limits in your query!"
    }
}
```

### 3.3 ListTables Tool

**File: `listtables.go`**

```go
type ListTablesInput struct{}

type TableEntry struct {
    Schema string `json:"schema"`
    Name   string `json:"name"`
    Type   string `json:"type"` // "table", "view", "materialized_view", "foreign_table"
    Owner  string `json:"owner"`
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
    // 1. Acquire semaphore (same as Query — bounds total concurrent operations to pool size)
    select {
    case p.semaphore <- struct{}{}:
    case <-ctx.Done():
        return nil, ctx.Err()
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
    END AS type,
    pg_catalog.pg_get_userbyid(c.relowner) AS owner
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'f')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND has_table_privilege(c.oid, 'SELECT')
ORDER BY n.nspname, c.relname;
```

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

type DescribeTableOutput struct {
    Schema      string           `json:"schema"`
    Name        string           `json:"name"`
    Type        string           `json:"type"`                   // "table", "view", "materialized_view", "foreign_table"
    Definition  string           `json:"definition,omitempty"`   // view/matview SQL definition
    Columns     []ColumnInfo     `json:"columns"`
    Indexes     []IndexInfo      `json:"indexes"`
    Constraints []ConstraintInfo `json:"constraints"`
    ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
    Error       string           `json:"error,omitempty"`
}

// DescribeTable returns (*DescribeTableOutput, error). Unlike Query(), this returns a Go error
// because it doesn't go through the hook/protection/sanitization/error_prompts pipeline.
func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error)
```

**DescribeTable implementation:**

```go
func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error) {
    // 1. Acquire semaphore (same as Query — bounds total concurrent operations to pool size)
    select {
    case p.semaphore <- struct{}{}:
    case <-ctx.Done():
        return nil, ctx.Err()
    }
    defer func() { <-p.semaphore }()

    // 2. Apply configurable timeout
    queryCtx, cancel := context.WithTimeout(ctx, time.Duration(p.config.Query.DescribeTableTimeoutSeconds)*time.Second)
    defer cancel()

    // 3. Acquire connection and execute
    conn, err := p.pool.Acquire(queryCtx)
    if err != nil {
        return nil, fmt.Errorf("failed to acquire connection: %w", err)
    }
    defer conn.Release()

    // ... run multiple pg_catalog queries ...
}
```

**Implementation:** Runs multiple `pg_catalog` queries for columns, indexes, constraints, and foreign keys. Uses parameterized queries with schema and table name. Does NOT go through the hook/protection/sanitization pipeline. Acquires the semaphore and uses `query.describe_table_timeout_seconds` for its timeout.

Must fully support **tables, views, materialized views, and foreign tables** — the goal is for AI agents to have complete information to craft queries. The approach differs by object type:

**Object type detection** — first query determines the `relkind` (`r`=table, `v`=view, `m`=materialized view, `f`=foreign table) from `pg_class`. This determines which subsequent queries to run.

**Columns query:**
- For tables, views, foreign tables: uses `information_schema.columns` joined with `pg_constraint` for primary key detection.
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
Add `Definition` field to `DescribeTableOutput`:
```go
type DescribeTableOutput struct {
    Schema      string           `json:"schema"`
    Name        string           `json:"name"`
    Type        string           `json:"type"` // "table", "view", "materialized_view", "foreign_table"
    Definition  string           `json:"definition,omitempty"` // view/matview SQL definition
    Columns     []ColumnInfo     `json:"columns"`
    Indexes     []IndexInfo      `json:"indexes"`
    Constraints []ConstraintInfo `json:"constraints"`
    ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
    Error       string           `json:"error,omitempty"`
}
```

**Indexes query** — uses `pg_indexes` system view. Applicable to tables and materialized views (views don't have indexes).

**Constraints query** — uses `pg_constraint` with `pg_get_constraintdef()`. Applicable to tables (views/matviews don't have constraints).

**Foreign keys query** — uses `pg_constraint` where `contype = 'f'`, joining `pg_attribute` for column names. Applicable to tables only.

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
    return mcp.NewToolResultJSON(output)
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
    // 1. Load config (env var path or .gopgmcp/config.json)
    config, err := loadConfig()

    // 2. Resolve connection string
    connString := os.Getenv("GOPGMCP_PG_CONNSTRING")
    if connString == "" {
        // Prompt for username and password interactively
        username := promptInput("Username: ")
        password := promptPassword("Password: ")
        connString = buildConnString(config.Connection, username, password)
    }

    // 3. Setup logger (zerolog)
    logger := setupLogger(config.Logging)

    // 4. Create PostgresMcp instance
    pgMcp, err := pgmcp.New(ctx, connString, config, logger)
    defer pgMcp.Close()

    // 5. Create MCP server
    mcpServer := server.NewMCPServer("gopgmcp", "1.0.0",
        server.WithToolCapabilities(true),
    )
    pgmcp.RegisterMCPTools(mcpServer, pgMcp)

    // 6. Start HTTP server
    httpServer := server.NewStreamableHTTPServer(mcpServer,
        server.WithStateLess(),
    )

    // 7. Optionally register health check (separate http handler)
    // The mcp-go StreamableHTTP handles /mcp endpoint.
    // Health check is a separate endpoint on the same port.
    // We may need a custom http.ServeMux to serve both.

    // 8. Start listening
    logger.Info().Int("port", config.Server.Port).Msg("starting gopgmcp server")
    return httpServer.Start(fmt.Sprintf(":%d", config.Server.Port))
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
addr := fmt.Sprintf(":%d", config.Server.Port)

// Step 1: Create the mux.
mux := http.NewServeMux()

// Step 2: Register health check on the mux (if enabled).
// Health check confirms MCP server process is running and responsive.
// Does NOT check database connectivity (by design — documented in requirements).
if config.Server.HealthCheckEnabled {
    mux.HandleFunc(config.Server.HealthCheckPath, func(w http.ResponseWriter, r *http.Request) {
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
    server.WithStateLess(),
    server.WithStreamableHTTPServer(httpSrv),
)

// Step 5: Manually register the StreamableHTTPServer on the mux.
// This is REQUIRED because Start() does NOT register the handler when
// a custom *http.Server is provided via WithStreamableHTTPServer.
mux.Handle("/mcp", streamableServer)

// Step 6: Start listening.
logger.Info().Int("port", config.Server.Port).Msg("starting gopgmcp server")
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
| `TestReadOnly_AllowsBeginReadOnly` | `BEGIN READ ONLY` | ReadOnly=true | allowed |
| `TestReadOnly_AllowsBeginDefault` | `BEGIN` | ReadOnly=true | allowed |
| `TestReadOnly_AllowsOtherSet` | `SET search_path = 'public'` | ReadOnly=true, AllowSet=true | allowed |
| `TestReadOnly_SetBlockedTakesPriority` | `SET default_transaction_read_only = off` | ReadOnly=true, AllowSet=false | `"SET default_transaction_read_only is blocked in read-only mode"` (readOnly check runs first) |

#### Allowed Statements

| Test | SQL | Config | Expected |
|---|---|---|---|
| `TestAllowSelect` | `SELECT * FROM users` | default | allowed |
| `TestAllowSelectComplex` | `WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id > 1` | default | allowed |
| `TestAllowInsert` | `INSERT INTO users (name) VALUES ('test')` | default | allowed |
| `TestAllowInsertReturning` | `INSERT INTO users (name) VALUES ('test') RETURNING *` | default | allowed |
| `TestAllowInsertOnConflict` | `INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = 'test'` | default | allowed |
| `TestAllowCreateTable` | `CREATE TABLE test (id int)` | default | allowed (not blocked by protection) |
| `TestAllowAlterTable` | `ALTER TABLE users ADD COLUMN email text` | default | allowed |
| `TestAllowExplain` | `EXPLAIN ANALYZE SELECT * FROM users` | default | allowed |
| `TestAllowGrant` | `GRANT SELECT ON users TO readonly_user` | default | allowed |

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
| `TestSQLInjection_Stacked` | `SELECT 1; DELETE FROM users; --` | default | `"multi-statement queries are not allowed: found 3 statements"` |
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

### 6.4 Unit Tests: Error Prompts (`internal/errprompt/errprompt_test.go`)

| Test | Error Message | Rules | Expected |
|---|---|---|---|
| `TestMatchPermissionDenied` | `permission denied for table users` | `(?i)permission denied` → message | message returned |
| `TestMatchRelationNotExist` | `relation "foo" does not exist` | `(?i)relation.*does not exist` → message | message returned |
| `TestNoMatch` | `some other error` | both rules | empty string |
| `TestMultipleMatches` | `permission denied, relation does not exist` | both rules | both messages concatenated |
| `TestEmptyRules` | any error | no rules | empty string |
| `TestMatchHookError` | `rejected by test hook` | `(?i)rejected` → message | message returned |

### 6.5 Unit Tests: Timeout (`internal/timeout/timeout_test.go`)

| Test | SQL | Rules | Expected |
|---|---|---|---|
| `TestMatchFirstRule` | `SELECT * FROM pg_stat_activity` | pg_stat→5s, JOIN→60s | 5s |
| `TestStopOnFirstMatch` | `SELECT * FROM pg_stat JOIN x JOIN y JOIN z` | pg_stat→5s, JOIN×3→60s | 5s (first match wins) |
| `TestDefaultTimeout` | `SELECT 1` | no matching rules, default=30s | 30s |
| `TestNoRules` | `SELECT 1` | empty rules, default=30s | 30s |

### 6.6 Unit Tests: Config (`config_test.go` or `internal/configure/configure_test.go`)

| Test | Scenario | Expected |
|---|---|---|
| `TestLoadConfigValid` | valid JSON config file | parsed correctly, all fields populated |
| `TestLoadConfigFromEnvPath` | `GOPGMCP_CONFIG_PATH` set | reads from env path, not default location |
| `TestLoadConfigMissing` | no config file | returns error containing config path |
| `TestLoadConfigInvalidJSON` | malformed JSON | returns error containing `"invalid"` or `"unmarshal"` |
| `TestLoadConfigInvalidRegex` | invalid regex in sanitization rules | returns error containing `"regex"` or `"compile"` and the invalid pattern |
| `TestLoadConfigDefaults_MaxResultLength` | config with `max_result_length` omitted (0) | defaults to `100000` |
| `TestLoadConfigValidation_NoPort` | config without server.port | panics with message containing `"server.port"` |
| `TestLoadConfigValidation_ZeroMaxConns` | config with pool.max_conns = 0 | panics with message containing `"pool.max_conns"` |
| `TestLoadConfigValidation_ZeroDefaultTimeout` | config with `default_timeout_seconds` = 0 | panics with message containing `"default_timeout_seconds"` |
| `TestLoadConfigValidation_MissingDefaultTimeout` | config without `default_timeout_seconds` | panics with message containing `"default_timeout_seconds"` (no default, must be set) |
| `TestLoadConfigValidation_ZeroListTablesTimeout` | config with `list_tables_timeout_seconds` = 0 | panics with message containing `"list_tables_timeout_seconds"` |
| `TestLoadConfigValidation_ZeroDescribeTableTimeout` | config with `describe_table_timeout_seconds` = 0 | panics with message containing `"describe_table_timeout_seconds"` |
| `TestLoadConfigValidation_NegativeTimeout` | negative timeout value | panics with message containing `"timeout"` |
| `TestLoadConfigValidation_ZeroHookDefaultTimeout` | hooks configured but `hooks.default_timeout_seconds` = 0 | panics with message containing `"hooks.default_timeout_seconds"` |
| `TestLoadConfigValidation_MissingHookDefaultTimeout` | hooks configured but `hooks.default_timeout_seconds` omitted | panics with message containing `"hooks.default_timeout_seconds"` (no default, must be set) |
| `TestLoadConfigValidation_HookDefaultTimeoutNotRequiredWithoutHooks` | no hooks configured, `hooks.default_timeout_seconds` omitted | no panic (validation only applies when hooks exist) |
| `TestLoadConfigValidation_HookTimeoutFallback` | hook with `timeout_seconds` = 0, `hooks.default_timeout_seconds` = 10 | hook uses default (10s) |
| `TestLoadConfigValidation_HealthCheckPathEmpty` | `health_check_enabled` = true, `health_check_path` = "" | panics with message containing `"health_check_path"` |
| `TestLoadConfigValidation_HealthCheckPathNotRequiredWhenDisabled` | `health_check_enabled` = false, `health_check_path` = "" | no panic (path not needed when disabled) |
| `TestLoadConfigProtectionDefaults` | minimal config, no protection fields | all `Allow*` fields are `false` (Go zero-value = blocked) |
| `TestLoadConfigProtectionExplicitAllow` | config with `allow_drop: true` | `AllowDrop` is `true`, all others remain `false` |
| `TestLoadConfigSSLMode` | config with `sslmode: "verify-full"` | `Connection.SSLMode` is `"verify-full"` |

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
| `TestQuery_JSONBNumericPrecision` | Table with JSONB containing large integers e.g. `{"id": 9007199254740993}` (2^53+1) | `SELECT data FROM items` | Large integer preserved exactly as `json.Number("9007199254740993")`, not truncated to float64 |
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
| `TestQuery_NullValues` | Table with NULL columns | `SELECT * FROM ...` | NULL returned as JSON null (Go `nil`) |
| `TestQuery_UUIDColumn` | Table with UUID column | `SELECT id FROM ...` | UUID as formatted string `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"` |
| `TestQuery_TimestampColumn` | Table with timestamp column | `SELECT created_at FROM ...` | Timestamp as RFC3339Nano string |
| `TestQuery_NumericColumn` | Table with numeric(10,2) | `SELECT price FROM ...` | Numeric value as string (preserves precision, e.g. `"123.45"`) |
| `TestQuery_BigIntColumn` | Table with bigint column, value `9007199254740993` (2^53+1) | `SELECT big_id FROM ...` | Value preserved as exact integer (not float64-truncated) |
| `TestQuery_ByteaColumn` | Table with bytea column containing binary data | `SELECT avatar FROM ...` | Binary data returned as base64-encoded string |
| `TestQuery_EmptyResult` | Empty table | `SELECT * FROM empty_table` | Empty rows array, columns present |
| `TestQuery_NumericPrecisionWithHooks` | Config with AfterQuery accept.sh hook, table with bigint 2^53+1 | `SELECT big_id FROM ...` | Value survives JSON round-trip through hooks via `UseNumber()`, exact integer preserved |
| `TestQuery_NumericPrecisionWithoutHooks` | Config with no hooks, table with bigint 2^53+1 | `SELECT big_id FROM ...` | Value preserved (no JSON round-trip occurs) |

#### ListTables Integration Tests

| Test | Setup | Assert |
|---|---|---|
| `TestListTables_Basic` | Create 3 tables | Returns all 3 with correct names/types |
| `TestListTables_IncludesViews` | Create table + view | View included with type "view" |
| `TestListTables_IncludesMaterializedViews` | Create mat view | Included with type "materialized_view" |
| `TestListTables_ExcludesSystemTables` | Default DB | No pg_catalog/information_schema tables |
| `TestListTables_Empty` | Empty database (no user tables) | Empty list |
| `TestListTables_Timeout` | Config with list_tables_timeout=1s, `pg_sleep` in custom view or slow DB | Error contains context deadline exceeded |
| `TestListTables_AcquiresSemaphore` | Config max_conns=1, hold semaphore in another goroutine | ListTables blocks, then succeeds when semaphore released; or times out if held too long |

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
| `TestDescribeTable_Timeout` | Config with describe_table_timeout=1s, table with slow function-based defaults or slow DB | Error contains context deadline exceeded |
| `TestDescribeTable_AcquiresSemaphore` | Config max_conns=1, hold semaphore in another goroutine | DescribeTable blocks, then succeeds when semaphore released; or times out if held too long |

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
| `TestStress_SemaphoreLimit` | Config max_conns=3. Spawn 20 goroutines with `SELECT pg_sleep(0.1)`. Assert max 3 concurrent queries (instrument with atomic counter). |
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
   - Server settings (port, read_only, health check)
   - Protection rules (SET, DROP, TRUNCATE, DO blocks, DELETE/UPDATE WHERE) — all blocked by default (`false` = blocked, `true` = allowed)
   - Query settings (default timeout, max result length, timeout rules)
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
   - Query: full pipeline (hooks → protection → timeout → execute → hooks → sanitization → truncation), returns JSON
   - ListTables: lists tables, views, materialized views, foreign tables accessible to the connected user
   - DescribeTable: returns schema details including columns, indexes, constraints, foreign keys, and view definitions

8. **Known limitations:**
   - `QueryExecModeExec` (simple protocol) prevents SQL injection and multi-statement queries but may affect type mapping for some Postgres types
   - Multi-statement queries are always rejected (cannot be toggled off)
   - Protection checks work at the SQL AST level — they cannot inspect dynamic SQL inside PL/pgSQL functions (only DO blocks are blocked)

### Code Comments

- All exported types and functions must have godoc comments
- Internal packages should have package-level doc comments explaining their purpose
- Complex logic (AST walking, JSONB handling, hook middleware chain) should have inline comments
