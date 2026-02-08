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
│       └── crash.sh                  # Test hook: exits with error
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
    Host   string `json:"host"`
    Port   int    `json:"port"`
    DBName string `json:"dbname"`
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
    BlockSet             bool `json:"block_set"`
    BlockDrop            bool `json:"block_drop"`
    BlockTruncate        bool `json:"block_truncate"`
    RequireWhereOnDelete bool `json:"require_where_on_delete"`
    RequireWhereOnUpdate bool `json:"require_where_on_update"`
}

type QueryConfig struct {
    DefaultTimeoutSeconds int            `json:"default_timeout_seconds"`
    MaxResultLength       int            `json:"max_result_length"`
    TimeoutRules          []TimeoutRule  `json:"timeout_rules"`
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
    Pattern        string `json:"pattern"`
    Command        string `json:"command"`
    TimeoutSeconds int    `json:"timeout_seconds"`
}
```

**Config loading logic** (internal to `pgmcp.go` or `cmd/`):
1. Check `GOPGMCP_CONFIG_PATH` env var → use that path
2. Otherwise use `<cwd>/.gopgmcp/config.json`
3. Parse JSON → `Config` struct
4. Validate: compile all regex patterns, check required fields (server.port), check timeout values > 0
5. Return descriptive errors on validation failure

**Config defaults** (applied when fields are zero-value):
- `protection.*` → all `true` (defaults on)
- `query.default_timeout_seconds` → `30`
- `query.max_result_length` → `100000`
- `server.health_check_path` → `/health-check`
- `hooks.default_timeout_seconds` → `10`

---

## Phase 2: Internal Packages

### 2.1 Protection Checker

**File: `internal/protection/protection.go`**

```go
package protection

type Checker struct {
    blockSet             bool
    blockDrop            bool
    blockTruncate        bool
    requireWhereOnDelete bool
    requireWhereOnUpdate bool
    readOnly             bool
}

func NewChecker(config pgmcp.ProtectionConfig, readOnly bool) *Checker

// Check parses SQL with pg_query_go and walks the AST.
// Returns nil if allowed, descriptive error if blocked.
func (c *Checker) Check(sql string) error
```

**AST walking logic:**

```go
result, err := pg_query.Parse(sql)
// err → return parse error

for _, rawStmt := range result.Stmts {
    switch node := rawStmt.Stmt.Node.(type) {
    case *pg_query.Node_VariableSetStmt:
        // If blockSet → block all SET
        // If readOnly → always block SET default_transaction_read_only
        //               and SET transaction_read_only regardless of blockSet
        varSetStmt := node.VariableSetStmt
        if c.readOnly && isTransactionReadOnlyVar(varSetStmt.Name) {
            return error
        }
        if c.blockSet {
            return error
        }

    case *pg_query.Node_DropStmt:
        if c.blockDrop { return error }

    case *pg_query.Node_TruncateStmt:
        if c.blockTruncate { return error }

    case *pg_query.Node_DeleteStmt:
        if c.requireWhereOnDelete && node.DeleteStmt.WhereClause == nil {
            return error
        }

    case *pg_query.Node_UpdateStmt:
        if c.requireWhereOnUpdate && node.UpdateStmt.WhereClause == nil {
            return error
        }
    }
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
    timeout time.Duration
}

type Runner struct {
    beforeQuery    []compiledHook
    afterQuery     []compiledHook
    defaultTimeout time.Duration
    logger         zerolog.Logger
}

func NewRunner(config pgmcp.HooksConfig, logger zerolog.Logger) (*Runner, error)
// Compiles all regex patterns, returns error on invalid regex.

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

    cmd := exec.CommandContext(ctx, hook.command)
    cmd.Stdin = strings.NewReader(input)
    output, err := cmd.Output()
    // err → log and return nil, nil (continue)
    return output, nil
}
```

**Middleware chain logic (BeforeQuery):**
```go
func (r *Runner) RunBeforeQuery(ctx context.Context, query string) (string, error) {
    current := query
    for _, hook := range r.beforeQuery {
        if !hook.pattern.MatchString(current) {
            continue
        }
        output, err := r.executeHook(ctx, hook, current)
        if err != nil {
            r.logger.Error().Err(err).Str("command", hook.command).Msg("hook failed")
            continue
        }
        if output == nil {
            continue
        }
        var result BeforeQueryResult
        if err := json.Unmarshal(output, &result); err != nil {
            r.logger.Error().Err(err).Msg("hook returned invalid JSON")
            continue
        }
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

AfterQuery follows the same pattern but with `AfterQueryResult` and result string.

### 2.3 Sanitization Engine

**File: `internal/sanitize/sanitize.go`**

```go
package sanitize

type compiledRule struct {
    pattern     *regexp.Regexp
    replacement string
}

type Sanitizer struct {
    rules []compiledRule
}

func NewSanitizer(rules []pgmcp.SanitizationRule) (*Sanitizer, error)
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
        // Numeric, bool, nil — return as-is
        return v
    }
}
```

### 2.4 Error Prompt Matcher

**File: `internal/errprompt/errprompt.go`**

```go
package errprompt

type compiledRule struct {
    pattern *regexp.Regexp
    message string
}

type Matcher struct {
    rules []compiledRule
}

func NewMatcher(rules []pgmcp.ErrorPromptRule) (*Matcher, error)

// Match checks error message against all rules (top to bottom).
// Returns concatenation of all matching prompt messages.
// Returns empty string if no match.
func (m *Matcher) Match(errMsg string) string
```

### 2.5 Timeout Manager

**File: `internal/timeout/timeout.go`**

```go
package timeout

type compiledRule struct {
    pattern *regexp.Regexp
    timeout time.Duration
}

type Manager struct {
    rules          []compiledRule
    defaultTimeout time.Duration
}

func NewManager(config pgmcp.QueryConfig) (*Manager, error)

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
1. Build connection string: if `connString` is non-empty, use it. Otherwise build from `config.Connection` fields.
2. Configure `pgxpool.Config`: apply pool settings, set `DefaultQueryExecMode` to `pgx.QueryExecModeExec`.
3. If `config.Server.ReadOnly`, set `AfterConnect` hook to run `SET default_transaction_read_only = on` on each connection.
4. Create `pgxpool.Pool`.
5. Create semaphore: `make(chan struct{}, config.Pool.MaxConns)` — bounds concurrent query pipelines.
6. Initialize all internal components: `protection.NewChecker`, `hooks.NewRunner`, `sanitize.NewSanitizer`, `errprompt.NewMatcher`, `timeout.NewManager`.
7. Return `*PostgresMcp`.

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

func (p *PostgresMcp) Query(ctx context.Context, input QueryInput) (*QueryOutput, error)
```

**Full pipeline:**

```go
func (p *PostgresMcp) Query(ctx context.Context, input QueryInput) (*QueryOutput, error) {
    sql := input.SQL

    // 1. Acquire semaphore
    p.semaphore <- struct{}{}
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
    defer tx.Rollback(queryCtx) // no-op if committed

    rows, err := tx.Query(queryCtx, sql, pgx.QueryExecModeExec)
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

    // 7. Serialize to JSON for AfterQuery hooks
    resultJSON, err := json.Marshal(result)
    if err != nil {
        return p.handleError(err)
    }

    // 8. Run AfterQuery hooks (middleware chain)
    modifiedJSON, err := p.hooks.RunAfterQuery(ctx, string(resultJSON))
    if err != nil {
        return p.handleError(err)
    }

    // 9. Parse back modified result if hooks changed it
    var finalResult QueryOutput
    if err := json.Unmarshal([]byte(modifiedJSON), &finalResult); err != nil {
        return p.handleError(err)
    }

    // 10. Apply sanitization (per-field, recursive into JSONB/arrays)
    finalResult.Rows = p.sanitizer.SanitizeRows(finalResult.Rows)

    // 11. Apply max result length truncation
    p.truncateIfNeeded(&finalResult)

    return &finalResult, nil
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

**convertValue logic** — ensures JSONB/arrays are proper Go types (pgx returns them as `map[string]interface{}` / `[]interface{}` already via `rows.Values()`). Handles:
- `nil` → JSON null
- `time.Time` → ISO 8601 string
- `net.IPNet` → string
- `pgtype.Numeric` → float64 or string
- `[16]byte` (UUID) → formatted UUID string
- Other types → let `json.Marshal` handle

**handleError logic:**
```go
func (p *PostgresMcp) handleError(err error) (*QueryOutput, error) {
    errMsg := err.Error()
    // Check error prompts
    prompt := p.errPrompts.Match(errMsg)
    if prompt != "" {
        errMsg = errMsg + "\n\n" + prompt
    }
    return &QueryOutput{Error: errMsg}, nil
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

func (p *PostgresMcp) ListTables(ctx context.Context, input ListTablesInput) (*ListTablesOutput, error)
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

ListTables does NOT go through the hook/protection/sanitization pipeline — it's a read-only metadata query using a hardcoded SQL. It uses a separate connection acquire with the default timeout.

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
    Columns     []ColumnInfo     `json:"columns"`
    Indexes     []IndexInfo      `json:"indexes"`
    Constraints []ConstraintInfo `json:"constraints"`
    ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
    Error       string           `json:"error,omitempty"`
}

func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error)
```

**Implementation:** Runs multiple `pg_catalog` queries for columns, indexes, constraints, and foreign keys. Uses parameterized queries with schema and table name. Does NOT go through the hook/protection/sanitization pipeline.

**Columns query** — uses `information_schema.columns` joined with `pg_constraint` for primary key detection.

**Indexes query** — uses `pg_indexes` system view.

**Constraints query** — uses `pg_constraint` with `pg_get_constraintdef()`.

**Foreign keys query** — uses `pg_constraint` where `contype = 'f'`, joining `pg_attribute` for column names.

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
    output, err := pgMcp.Query(ctx, QueryInput{SQL: sql})
    if err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
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

Since mcp-go's `StreamableHTTPServer` manages its own HTTP server, we need to either:
- Use `server.WithStreamableHTTPServer(customHTTPServer)` to pass a custom `http.Server` with a mux that also handles the health check path, OR
- Use the built-in mcp-go health check if available

Approach: Create a custom `http.ServeMux`, register the health check handler, then use `WithStreamableHTTPServer` to pass a custom `*http.Server` that uses this mux. The mcp-go StreamableHTTP handler is mounted at its endpoint path on this mux.

```go
mux := http.NewServeMux()

// Health check
if config.Server.HealthCheckEnabled {
    mux.HandleFunc(config.Server.HealthCheckPath, func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"status":"ok"}`))
    })
}

// MCP endpoint — mount the StreamableHTTP handler
httpServer := server.NewStreamableHTTPServer(mcpServer,
    server.WithEndpointPath("/mcp"),
    server.WithStateLess(),
    server.WithStreamableHTTPServer(&http.Server{
        Addr:    fmt.Sprintf(":%d", config.Server.Port),
        Handler: mux,
    }),
)
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

---

### 6.1 Unit Tests: Protection (`internal/protection/protection_test.go`)

All tests are pure unit tests — no database needed.

| Test | SQL | Expected |
|---|---|---|
| `TestBlockDrop_Table` | `DROP TABLE users` | blocked |
| `TestBlockDrop_Index` | `DROP INDEX idx_users` | blocked |
| `TestBlockDrop_Schema` | `DROP SCHEMA public` | blocked |
| `TestBlockDrop_CaseInsensitive` | `drop table users` | blocked |
| `TestBlockDrop_WithComments` | `/* comment */ DROP TABLE users` | blocked |
| `TestBlockDrop_Disabled` | `DROP TABLE users` (blockDrop=false) | allowed |
| `TestBlockTruncate` | `TRUNCATE users` | blocked |
| `TestBlockTruncate_Disabled` | `TRUNCATE users` (blockTruncate=false) | allowed |
| `TestBlockSet` | `SET search_path TO 'public'` | blocked |
| `TestBlockSet_Reset` | `RESET ALL` | blocked (VariableSetStmt with VAR_RESET_ALL) |
| `TestBlockSet_Disabled` | `SET work_mem = '256MB'` (blockSet=false) | allowed |
| `TestBlockDeleteWithoutWhere` | `DELETE FROM users` | blocked |
| `TestAllowDeleteWithWhere` | `DELETE FROM users WHERE id = 1` | allowed |
| `TestAllowDeleteWithComplexWhere` | `DELETE FROM users WHERE id IN (SELECT id FROM banned)` | allowed |
| `TestBlockUpdateWithoutWhere` | `UPDATE users SET active = false` | blocked |
| `TestAllowUpdateWithWhere` | `UPDATE users SET active = false WHERE id = 1` | allowed |
| `TestAllowSelect` | `SELECT * FROM users` | allowed |
| `TestAllowSelectComplex` | `WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id > 1` | allowed |
| `TestAllowInsert` | `INSERT INTO users (name) VALUES ('test')` | allowed |
| `TestReadOnly_BlocksSetTransactionReadOnly` | `SET default_transaction_read_only = off` (readOnly=true, blockSet=false) | blocked |
| `TestReadOnly_BlocksSetTransactionReadOnly2` | `SET transaction_read_only = false` (readOnly=true, blockSet=false) | blocked |
| `TestReadOnly_AllowsOtherSet` | `SET search_path = 'public'` (readOnly=true, blockSet=false) | allowed |
| `TestParseError` | `NOT VALID SQL @#$` | returns parse error |
| `TestAllProtectionsDisabled` | `DROP TABLE users; DELETE FROM x; TRUNCATE y; SET z = 1; UPDATE a SET b = 1` | all allowed |
| `TestCTEWithDelete` | `WITH deleted AS (DELETE FROM users RETURNING *) SELECT * FROM deleted` | blocked (DeleteStmt in CTE) |
| `TestDoBlock` | `DO $$ BEGIN EXECUTE 'DROP TABLE users'; END $$` | Note: pg_query_go parses DO block as DoStmt, not the inner string. Document this as known limitation. |

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
| `TestSanitizeBooleanField` | `true` | any rule | `true` (unchanged) |
| `TestSanitizeEmptyRules` | any value | no rules | unchanged |
| `TestSanitizeRows` | full result set with mixed types | phone regex | only string fields sanitized |

### 6.3 Unit Tests: Hooks (`internal/hooks/hooks_test.go`)

Tests use the shell scripts in `testdata/hooks/`. All scripts must be `chmod +x`.

| Test | Hook Config | Expected |
|---|---|---|
| `TestBeforeQuery_Accept` | accept.sh, pattern `.*` | query passes through unchanged |
| `TestBeforeQuery_Reject` | reject.sh, pattern `.*` | error returned with "rejected by test hook" |
| `TestBeforeQuery_ModifyQuery` | modify_query.sh, pattern `.*` | query changed to "SELECT 1 AS modified" |
| `TestBeforeQuery_PatternNoMatch` | accept.sh, pattern `NEVER_MATCH` | hook not executed, query passes through |
| `TestBeforeQuery_Chaining` | [modify_query.sh, accept.sh] | second hook receives modified query |
| `TestBeforeQuery_ChainPatternReEval` | [modify_query.sh (pattern `.*`), reject.sh (pattern `modified`)] | second hook matches modified query, rejects |
| `TestBeforeQuery_Timeout` | slow.sh, timeout=1s | hook times out, logged, query continues |
| `TestBeforeQuery_Crash` | crash.sh, pattern `.*` | hook fails, logged, query continues |
| `TestAfterQuery_Accept` | accept.sh | result passes through |
| `TestAfterQuery_Reject` | reject.sh | error returned |
| `TestAfterQuery_ModifyResult` | modify_result.sh | result changed |
| `TestAfterQuery_Chaining` | [modify_result.sh, accept.sh] | second hook receives modified result |
| `TestAfterQuery_Timeout` | slow.sh, timeout=1s | times out, logged, continues |
| `TestAfterQuery_Crash` | crash.sh | fails, logged, continues |
| `TestHookStdinInput` | custom script that echoes stdin back | verify correct input passed |

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
| `TestLoadConfigValid` | valid JSON config file | parsed correctly |
| `TestLoadConfigFromEnvPath` | `GOPGMCP_CONFIG_PATH` set | reads from env path |
| `TestLoadConfigMissing` | no config file | returns error |
| `TestLoadConfigInvalidJSON` | malformed JSON | returns parse error |
| `TestLoadConfigInvalidRegex` | invalid regex in protection/sanitization/hooks | returns descriptive error |
| `TestLoadConfigDefaults` | minimal config (only required fields) | defaults applied |
| `TestLoadConfigValidation_NoPort` | config without server.port | returns error |
| `TestLoadConfigValidation_NegativeTimeout` | negative timeout value | returns error |

---

### 6.7 Integration Tests (`integration_test.go`)

All integration tests use pgflock to acquire a real database. Build tag: `//go:build integration`

Run with: `go test -tags=integration -race -v ./...`

#### Query Tool Integration Tests

| Test | Setup | Action | Assert |
|---|---|---|---|
| `TestQuery_SelectBasic` | Create table with sample data | `SELECT * FROM users` | Returns correct JSON rows |
| `TestQuery_SelectJSONB` | Table with JSONB column | `SELECT data FROM items` | JSONB returned as proper JSON object, not string |
| `TestQuery_SelectArray` | Table with integer[] column | `SELECT tags FROM posts` | Array returned as JSON array |
| `TestQuery_SelectCTE` | Table with data | `WITH cte AS (SELECT ...) SELECT * FROM cte` | Correct results |
| `TestQuery_SelectNestedSubquery` | Multiple tables | Query with subqueries | Correct results |
| `TestQuery_Insert` | Empty table | `INSERT INTO users ... RETURNING *` | Returns inserted row |
| `TestQuery_Update` | Table with data | `UPDATE users SET ... WHERE ... RETURNING *` | Returns updated row |
| `TestQuery_Delete` | Table with data | `DELETE FROM users WHERE ... RETURNING *` | Returns deleted row |
| `TestQuery_Transaction` | Table | `SELECT * FROM users` | Verify runs in transaction (data consistency) |
| `TestQuery_Timeout` | Table | `SELECT pg_sleep(10)` with 1s timeout | Timeout error returned |
| `TestQuery_TimeoutRuleMatch` | Config with timeout rule matching query | Slow query | Uses rule timeout, not default |
| `TestQuery_ProtectionEndToEnd` | Table | `DROP TABLE users` | Error: blocked by protection |
| `TestQuery_HooksEndToEnd` | Config with real hook scripts | Query matching hook pattern | Hook executed, result correct |
| `TestQuery_SanitizationEndToEnd` | Table with phone numbers | `SELECT phone FROM contacts` | Phone numbers sanitized |
| `TestQuery_ErrorPromptEndToEnd` | No table | `SELECT * FROM nonexistent` | Error includes prompt message |
| `TestQuery_MaxResultLength` | Table with many rows | `SELECT * FROM large_table` | Result truncated with message |
| `TestQuery_ReadOnlyMode` | Config with read_only=true | `INSERT INTO users ...` | Error: read-only transaction |
| `TestQuery_ReadOnlyModeBlocksSetBypass` | Config with read_only=true | `SET default_transaction_read_only = off` | Error: blocked |
| `TestQuery_NullValues` | Table with NULL columns | `SELECT * FROM ...` | NULL returned as JSON null |
| `TestQuery_UUIDColumn` | Table with UUID column | `SELECT id FROM ...` | UUID as string |
| `TestQuery_TimestampColumn` | Table with timestamp column | `SELECT created_at FROM ...` | Timestamp as ISO 8601 |
| `TestQuery_NumericColumn` | Table with numeric(10,2) | `SELECT price FROM ...` | Proper numeric value |
| `TestQuery_EmptyResult` | Empty table | `SELECT * FROM empty_table` | Empty rows array, columns present |

#### ListTables Integration Tests

| Test | Setup | Assert |
|---|---|---|
| `TestListTables_Basic` | Create 3 tables | Returns all 3 with correct names/types |
| `TestListTables_IncludesViews` | Create table + view | View included with type "view" |
| `TestListTables_IncludesMaterializedViews` | Create mat view | Included with type "materialized_view" |
| `TestListTables_ExcludesSystemTables` | Default DB | No pg_catalog/information_schema tables |
| `TestListTables_Empty` | Empty database (no user tables) | Empty list |

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
| `TestDescribeTable_NotFound` | No such table | Descriptive error |

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
| `TestMCPServer_ToolsList` | `tools/list` returns all 3 tools with correct schemas |

---

### 6.8 Stress Tests (`stress_test.go`)

Build tag: `//go:build integration`

| Test | Description |
|---|---|
| `TestStress_ConcurrentQueries` | Spawn 50 goroutines each running 20 queries. Assert all complete without error. Verify total time is reasonable (bounded by pool size, not sequential). |
| `TestStress_SemaphoreLimit` | Config max_conns=3. Spawn 20 goroutines with `SELECT pg_sleep(0.1)`. Assert max 3 concurrent queries (instrument with atomic counter). |
| `TestStress_LargeResultTruncation` | Insert 10,000 rows. `SELECT *` with max_result_length=1000. Assert truncation message present. |
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

These tests verify that all shared components are safe for concurrent use. They are structured as:
```go
func TestRace_ConcurrentSanitization(t *testing.T) {
    s, _ := sanitize.NewSanitizer(rules)
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                s.SanitizeRows(testData)
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
