<p align="center">
  <img src="assets/banner.svg" alt="gopgmcp" width="580">
</p>

# postgres-mcp

A production-grade **PostgreSQL MCP server** written in Go. Gives AI agents and LLMs (Claude, GPT, Cursor, Windsurf, and others) safe, controlled access to PostgreSQL databases through the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/).

Use it as a **standalone MCP server** or embed it as a **Go library** in your own application. Connect any MCP-compatible AI coding assistant or autonomous agent to your Postgres database with fine-grained SQL protection, query hooks, data masking, and read-only mode.

> **Important:** postgres-mcp is designed for **local or internal network use only**. It has no authentication and no CORS headers by design. Do not expose it to the public internet. Run it on your local machine, behind a VPN, or on an internal server that is not publicly accessible.

## Why postgres-mcp?

- **No SQL injection** — uses pgx extended query protocol (`QueryExecModeExec`), which only allows single statements at the protocol level. On top of that, 23 AST-based protection rules (all blocked by default) using PostgreSQL's actual C parser via `pg_query`.
- **Production-ready** — connection pooling, concurrency control, configurable timeouts, query result truncation, structured logging.
- **Extensible** — BeforeQuery/AfterQuery hooks (command-based or Go interface), data sanitization, dynamic error-based agent steering.
- **Comprehensive schema introspection** — tables, views, materialized views, foreign tables, partitioned tables, indexes, constraints, foreign keys, partition metadata.
- **Handles 30+ PostgreSQL types** — JSONB, arrays, UUID, geometric types, ranges, intervals, network types, and more — all converted to clean JSON.
- **Dual-mode** — run as an HTTP server (`gopgmcp serve`) or import as a Go library (`go get`).

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
  - [Server Mode](#server-mode)
  - [Library Mode](#library-mode)
- [MCP Tools](#mcp-tools)
  - [query](#query)
  - [list_tables](#list_tables)
  - [describe_table](#describe_table)
- [Configuration Reference](#configuration-reference)
  - [Full Example](#full-example)
  - [Connection](#connection)
  - [Connection Pool](#connection-pool)
  - [Server](#server)
  - [Logging](#logging)
  - [Query Settings](#query-settings)
  - [Protection Rules](#protection-rules)
  - [Read-Only Mode](#read-only-mode)
  - [Timezone](#timezone)
  - [Timeout Rules](#timeout-rules)
  - [Result Truncation](#result-truncation)
  - [Sanitization](#sanitization)
  - [Error Prompts](#error-prompts)
  - [Hooks (Server Mode)](#hooks-server-mode)
  - [Hooks (Library Mode)](#hooks-library-mode)
- [Query Execution Pipeline](#query-execution-pipeline)
- [SQL Protection Rules](#sql-protection-rules)
- [Type Handling](#type-handling)
- [Recommended Configurations](#recommended-configurations)
  - [Analytics / Read-Only Exploration](#analytics--read-only-exploration)
  - [Development Assistant](#development-assistant)
  - [Production with PII Protection](#production-with-pii-protection)
- [Security Model](#security-model)
  - [What postgres-mcp does](#what-postgres-mcp-does)
  - [What postgres-mcp does NOT do](#what-postgres-mcp-does-not-do)
- [CLI Reference](#cli-reference)
  - [Environment Variables](#environment-variables)
- [Library API](#library-api)
  - [Constructor](#constructor)
  - [Methods](#methods)
  - [Options](#options)
  - [MCP Tool Registration](#mcp-tool-registration)
  - [Example: OpenAI Tool Calling](#example-openai-tool-calling)
  - [Example: Custom MCP Server with Go Hooks](#example-custom-mcp-server-with-go-hooks)
- [Contributing](#contributing)
- [License](#license)

## Features

### MCP Tools
| Tool | Description |
|---|---|
| `query` | Execute SQL queries. Returns JSON results with columns, rows, rows_affected. Full pipeline: hooks, protection, sanitization, error prompts. |
| `list_tables` | List all tables, views, materialized views, foreign tables, and partitioned tables accessible to the current user. |
| `describe_table` | Full schema introspection: columns, types, indexes, constraints, foreign keys, partition info, view definitions. |

### No SQL Injection + 23 Protection Rules
SQL injection is impossible at the protocol level — pgx extended query protocol (`QueryExecModeExec`) only allows single statements, enforced by PostgreSQL itself. On top of that, 23 AST-based protection rules (all blocked by default) using PostgreSQL's actual C parser via [pg_query_go](https://github.com/pganalyze/pg_query_go). Walks the AST to detect disallowed operations — including inside CTEs and EXPLAIN statements. Transaction control is always blocked.

### Query Pipeline
Every query goes through: semaphore acquisition, SQL length check, BeforeQuery hooks, protection checks, timeout resolution, managed transaction, AfterQuery hooks, sanitization, result truncation, error-based agent steering.

### Hooks (Middleware)
BeforeQuery and AfterQuery hooks run as a middleware chain. Available as **command-based hooks** (CLI mode — external binaries via stdin/stdout) or **Go interface hooks** (library mode — native Go types, no serialization overhead). AfterQuery hooks run before transaction commit for write queries, enabling true guardrails (e.g., rollback if too many rows affected).

### Data Sanitization
Regex-based field-level sanitization with replacement strings. Recursive into JSONB objects and arrays. Mask phone numbers, credit cards, national IDs, or any PII pattern.

### Error Prompt Injection
Regex patterns matched against error messages. Appends contextual guidance for AI agents (e.g., "permission denied" → "try querying the read-only view instead"). Multiple matching prompts are concatenated.

### Additional Features
- **[Read-only mode](#read-only-mode)** — enforced at connection level (`SET default_transaction_read_only = on`) and protection level (blocks `RESET`, `BEGIN READ WRITE`).
- **[Timezone management](#timezone)** — set session timezone on every connection (IANA timezone names).
- **[Configurable timeouts](#timeout-rules)** — default timeout + regex-based per-query timeout rules. Separate timeouts for `list_tables` and `describe_table`.
- **[Connection pooling](#connection-pool)** — pgxpool with configurable max/min connections, lifetime, idle time, health checks. Semaphore bounds total concurrent operations.
- **[30+ PostgreSQL type conversions](#type-handling)** — timestamps, intervals, numerics (arbitrary precision), UUID, bytea, geometric types, ranges, network types, bit strings.
- **[Result truncation](#result-truncation)** — enforced max result length with truncation notice. Prevents oversized responses to AI agents.
- **[Structured logging](#logging)** — zerolog with JSON or text output to stdout, stderr, or file. Logs MCP client name/version on connect, query SQL/duration/row count on every execution.
- **[Interactive configuration wizard](#option-a-interactive-configuration)** — `gopgmcp configure` walks through every config option.

## Quick Start

### Server Mode

#### Install

```bash
go install github.com/rickchristie/postgres-mcp/cmd/gopgmcp@latest
```

Make sure `$HOME/go/bin` is in your `PATH`. If `gopgmcp` is not found after installing, add this to your `~/.bashrc` (or `~/.zshrc`):

```bash
export PATH="$HOME/go/bin:$PATH"
```

Then restart your shell or run `source ~/.bashrc`.

#### Option A: Interactive Configuration

Run the configuration wizard to create a config file:

```bash
gopgmcp configure
```

This creates `.gopgmcp/config.json` with all settings. The wizard walks through every option, shows current values, and lets you press Enter to keep defaults.

Then start the server:

```bash
gopgmcp serve
```

You will be prompted for database username and password on startup.

#### Option B: Connection String from Environment

`GOPGMCP_PG_CONNSTRING` takes **highest priority** — when set, the `connection` fields in `config.json` and the interactive credential prompts are ignored entirely. Supports both URI and DSN format (both accepted by pgx):

```bash
# URI format
export GOPGMCP_PG_CONNSTRING="postgres://myuser:mypass@localhost:5432/mydb?sslmode=prefer"

# DSN format
export GOPGMCP_PG_CONNSTRING="host=localhost port=5432 dbname=mydb user=myuser password=mypass sslmode=prefer"

gopgmcp serve
```

#### Option C: Custom Config Path

Override the default config file location:

```bash
export GOPGMCP_CONFIG_PATH="/path/to/my/config.json"
gopgmcp serve
```

#### Connecting an AI Agent

Run `gopgmcp doctor` to validate your config and get ready-to-copy connection snippets for your AI agent:

```bash
gopgmcp doctor
```

This validates your config file (checks JSON, required fields, regex patterns) and prints connection snippets for:

- **Claude Code** — CLI command (`claude mcp add`) or `.mcp.json`
- **Cursor** — `.cursor/mcp.json`
- **Windsurf** — `~/.codeium/windsurf/mcp_config.json`
- **Copilot CLI** — `~/.copilot/mcp-config.json`
- **Gemini CLI** — `~/.gemini/settings.json`
- **OpenCode** — `opencode.json`

Example output snippet for Claude Code:

```
claude mcp add --transport http postgres http://localhost:8080/mcp
```

Transport: Streamable HTTP (stateless). No SSE, no CORS headers.

### Library Mode

Import the library and use it directly in your Go application:

```bash
go get github.com/rickchristie/postgres-mcp@latest
```

```go
package main

import (
    "context"
    "fmt"
    "os"

    pgmcp "github.com/rickchristie/postgres-mcp"
    "github.com/rs/zerolog"
)

func main() {
    ctx := context.Background()
    logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

    config := pgmcp.Config{
        Pool: pgmcp.PoolConfig{
            MaxConns: 5,
        },
        Query: pgmcp.QueryConfig{
            DefaultTimeoutSeconds:       30,
            ListTablesTimeoutSeconds:    10,
            DescribeTableTimeoutSeconds: 10,
        },
        ReadOnly: true,
    }

    p, err := pgmcp.New(ctx, os.Getenv("GOPGMCP_PG_CONNSTRING"), config, logger)
    if err != nil {
        panic(err)
    }
    defer p.Close(ctx)

    // Execute a query
    result := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT id, name FROM users LIMIT 5"})
    if result.Error != "" {
        fmt.Println("Error:", result.Error)
        return
    }
    fmt.Printf("Columns: %v\n", result.Columns)
    fmt.Printf("Rows: %d\n", len(result.Rows))

    // List all tables
    tables, err := p.ListTables(ctx, pgmcp.ListTablesInput{})
    if err != nil {
        panic(err)
    }
    for _, t := range tables.Tables {
        fmt.Printf("%s.%s (%s)\n", t.Schema, t.Name, t.Type)
    }

    // Describe a table
    desc, err := p.DescribeTable(ctx, pgmcp.DescribeTableInput{Table: "users"})
    if err != nil {
        panic(err)
    }
    for _, col := range desc.Columns {
        fmt.Printf("  %s %s (nullable: %v)\n", col.Name, col.Type, col.Nullable)
    }
}
```

#### Registering as MCP Tools

To register the tools on an MCP server (using [mcp-go](https://github.com/mark3labs/mcp-go)):

```go
import (
    pgmcp "github.com/rickchristie/postgres-mcp"
    "github.com/mark3labs/mcp-go/server"
)

mcpServer := server.NewMCPServer("my-app", "1.0.0",
    server.WithToolCapabilities(true),
)
pgmcp.RegisterMCPTools(mcpServer, p)
```

## MCP Tools

### query

Execute a SQL query against the PostgreSQL database. Returns results as JSON.

**Parameters:**
| Name | Type | Required | Description |
|---|---|---|---|
| `sql` | string | Yes | The SQL query to execute |

**Response fields:**
| Field | Type | Description |
|---|---|---|
| `columns` | string[] | Column names |
| `rows` | object[] | Array of row objects (column name → value) |
| `rows_affected` | int64 | Row count for INSERT/UPDATE/DELETE (even without RETURNING) |
| `error` | string | Error message (protection rejection, hook rejection, Postgres error, etc.) |

All errors are returned in the `error` field — the tool never returns a Go error. Error messages are evaluated against [error prompts](#error-prompts) and matching guidance is appended.

Queries run through the full [execution pipeline](#query-execution-pipeline): hooks → protection → managed transaction → sanitization → truncation → error prompts.

### list_tables

List all tables, views, materialized views, foreign tables, and partitioned tables accessible to the current user. Does **not** go through the hook/protection/sanitization pipeline.

**Parameters:** None

**Response fields:**
| Field | Type | Description |
|---|---|---|
| `tables` | TableEntry[] | Array of table entries |
| `error` | string | Error message if query fails |

Each `TableEntry` contains:
| Field | Type | Description |
|---|---|---|
| `schema` | string | Schema name |
| `name` | string | Table/view name |
| `type` | string | `"table"`, `"view"`, `"materialized_view"`, `"foreign_table"`, or `"partitioned_table"` |
| `owner` | string | Table owner username |
| `schema_access_limited` | bool | `true` if user has SELECT but not schema USAGE privilege |

System schemas (`pg_catalog`, `information_schema`, `pg_toast`) are excluded.

### describe_table

Describe the schema of a table, view, materialized view, foreign table, or partitioned table. Does **not** go through the hook/protection/sanitization pipeline.

**Parameters:**
| Name | Type | Required | Description |
|---|---|---|---|
| `table` | string | Yes | The table name to describe |
| `schema` | string | No | Schema name (defaults to `"public"`) |

**Response fields:**
| Field | Type | Description |
|---|---|---|
| `schema` | string | Schema name |
| `name` | string | Table name |
| `type` | string | Object type |
| `definition` | string | SQL definition (views and materialized views only) |
| `columns` | ColumnInfo[] | Column details: name, type, nullable, default, is_primary_key |
| `indexes` | IndexInfo[] | Index details: name, definition, is_unique, is_primary |
| `constraints` | ConstraintInfo[] | Constraint details: name, type (PRIMARY KEY/FOREIGN KEY/UNIQUE/CHECK/EXCLUSION), definition |
| `foreign_keys` | ForeignKeyInfo[] | Foreign key details: columns, referenced_table, referenced_columns, on_update, on_delete |
| `partition` | PartitionInfo | Partition metadata: strategy (range/list/hash), partition_key, child partitions, parent_table |
| `error` | string | Error message |

## Configuration Reference

### Full Example

```json
{
  "pool": {
    "max_conns": 5,
    "min_conns": 0,
    "max_conn_lifetime": "1h",
    "max_conn_idle_time": "5m",
    "health_check_period": "1m"
  },
  "protection": {
    "allow_set": false,
    "allow_drop": false,
    "allow_truncate": false,
    "allow_do": false,
    "allow_copy_from": false,
    "allow_copy_to": false,
    "allow_create_function": false,
    "allow_prepare": false,
    "allow_delete_without_where": false,
    "allow_update_without_where": false,
    "allow_alter_system": false,
    "allow_merge": false,
    "allow_grant_revoke": false,
    "allow_manage_roles": false,
    "allow_create_extension": false,
    "allow_lock_table": false,
    "allow_listen_notify": false,
    "allow_maintenance": false,
    "allow_ddl": false,
    "allow_discard": false,
    "allow_comment": false,
    "allow_create_trigger": false,
    "allow_create_rule": false
  },
  "query": {
    "default_timeout_seconds": 30,
    "list_tables_timeout_seconds": 10,
    "describe_table_timeout_seconds": 10,
    "max_sql_length": 100000,
    "max_result_length": 100000,
    "timeout_rules": [
      {
        "pattern": "(?i)\\bslow_table\\b",
        "timeout_seconds": 120
      }
    ]
  },
  "error_prompts": [
    {
      "pattern": "permission denied",
      "message": "You don't have permission. Try using the read-only view instead."
    },
    {
      "pattern": "does not exist",
      "message": "Table not found. Use the list_tables tool to see available tables."
    }
  ],
  "sanitization": [
    {
      "pattern": "(\\+62)\\d{6,}(\\d{3})",
      "replacement": "${1}xxx${2}",
      "description": "Mask Indonesian phone numbers"
    },
    {
      "pattern": "(\\d{4})\\d{8}(\\d{4})",
      "replacement": "${1}********${2}",
      "description": "Mask credit card numbers"
    }
  ],
  "read_only": true,
  "timezone": "Asia/Jakarta",
  "default_hook_timeout_seconds": 5,
  "connection": {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "sslmode": "prefer"
  },
  "server": {
    "port": 8080,
    "health_check_enabled": true,
    "health_check_path": "/health"
  },
  "logging": {
    "level": "info",
    "format": "json",
    "output": "stderr"
  },
  "server_hooks": {
    "before_query": [
      {
        "pattern": ".*",
        "command": "/usr/local/bin/my-query-validator",
        "args": ["--strict"],
        "timeout_seconds": 3
      }
    ],
    "after_query": []
  }
}
```

### Connection

Server mode only. Username and password are prompted interactively on startup (or use `GOPGMCP_PG_CONNSTRING` to skip prompts).

| Field | Type | Description |
|---|---|---|
| `connection.host` | string | PostgreSQL host |
| `connection.port` | int | PostgreSQL port |
| `connection.dbname` | string | Database name |
| `connection.sslmode` | string | SSL mode (`disable`, `prefer`, `require`, etc.) |

### Connection Pool

| Field | Type | Required | Description |
|---|---|---|---|
| `pool.max_conns` | int | Yes (> 0) | Maximum number of connections. Also determines the semaphore size for bounding concurrent operations. |
| `pool.min_conns` | int | No | Minimum idle connections (default: 0) |
| `pool.max_conn_lifetime` | string | No | Max connection lifetime (Go duration, e.g., `"1h"`) |
| `pool.max_conn_idle_time` | string | No | Max idle time before connection is closed (e.g., `"5m"`) |
| `pool.health_check_period` | string | No | How often to health-check idle connections (e.g., `"1m"`) |

### Server

Server mode only.

| Field | Type | Required | Description |
|---|---|---|---|
| `server.port` | int | Yes (> 0) | HTTP server port |
| `server.health_check_enabled` | bool | No | Enable health check endpoint |
| `server.health_check_path` | string | If enabled | Health check endpoint path (e.g., `"/health"`) |

The health check endpoint returns `{"status":"ok"}` (HTTP 200). It is a liveness probe only — does not check database connectivity.

### Logging

Server mode only.

| Field | Type | Options | Description |
|---|---|---|---|
| `logging.level` | string | `debug`, `info`, `warn`, `error` | Log level |
| `logging.format` | string | `json`, `text` | Log output format |
| `logging.output` | string | `stdout`, `stderr`, or file path | Log destination |

### Query Settings

| Field | Type | Required | Description |
|---|---|---|---|
| `query.default_timeout_seconds` | int | Yes (> 0) | Default query timeout. Panics on start if not set. |
| `query.list_tables_timeout_seconds` | int | Yes (> 0) | Timeout for list_tables operations. Panics on start if not set. |
| `query.describe_table_timeout_seconds` | int | Yes (> 0) | Timeout for describe_table operations. Panics on start if not set. |
| `query.max_sql_length` | int | No | Max SQL query length in bytes (default: 100,000) |
| `query.max_result_length` | int | No | Max result JSON length in characters (default: 100,000). Truncates with notice. |
| `query.timeout_rules` | array | No | Pattern-based timeout overrides (see [Timeout Rules](#timeout-rules)) |

### Protection Rules

All protection rules default to `false` (blocked). Set to `true` to allow.

| Field | What it blocks |
|---|---|
| `allow_ddl` | CREATE TABLE, ALTER TABLE, CREATE INDEX, CREATE SCHEMA, CREATE VIEW, CREATE SEQUENCE, ALTER SEQUENCE, RENAME, CREATE TABLE AS, CREATE MATERIALIZED VIEW |
| `allow_drop` | DROP TABLE, DROP DATABASE, DROP SCHEMA, DROP INDEX, etc. |
| `allow_truncate` | TRUNCATE TABLE |
| `allow_delete_without_where` | DELETE without a WHERE clause |
| `allow_update_without_where` | UPDATE without a WHERE clause |
| `allow_merge` | MERGE statements (can do INSERT/UPDATE/DELETE in one statement) |
| `allow_copy_from` | COPY FROM (bulk data import) |
| `allow_copy_to` | COPY TO (data export/exfiltration) |
| `allow_create_function` | CREATE FUNCTION, CREATE PROCEDURE |
| `allow_create_trigger` | CREATE TRIGGER |
| `allow_create_rule` | CREATE RULE (query rewriting at parser level) |
| `allow_create_extension` | CREATE EXTENSION, ALTER EXTENSION |
| `allow_set` | SET, RESET session variables (note: SET runs inside a transaction that is rolled back, so it has no lasting effect — use `timezone` and `read_only` config instead) |
| `allow_prepare` | PREPARE, EXECUTE, DEALLOCATE |
| `allow_discard` | DISCARD (session state reset) |
| `allow_grant_revoke` | GRANT, REVOKE permissions |
| `allow_manage_roles` | CREATE/ALTER/DROP ROLE/USER |
| `allow_alter_system` | ALTER SYSTEM (server-level config) |
| `allow_maintenance` | VACUUM, ANALYZE, CLUSTER, REINDEX, REFRESH MATERIALIZED VIEW |
| `allow_do` | DO $$ blocks (inline PL/pgSQL) |
| `allow_listen_notify` | LISTEN, NOTIFY, UNLISTEN |
| `allow_lock_table` | LOCK TABLE |
| `allow_comment` | COMMENT ON |

**Always blocked (cannot be toggled):**
- Multi-statement queries (only single statements allowed)
- Transaction control: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, PREPARE TRANSACTION, COMMIT PREPARED, ROLLBACK PREPARED
- EXPLAIN/EXPLAIN ANALYZE validates the inner statement against all protection rules

### Read-Only Mode

When `read_only` is `true`:

- Sets `default_transaction_read_only = on` on every connection
- Blocks `SET default_transaction_read_only`, `SET transaction_read_only`
- Blocks `RESET ALL`, `RESET default_transaction_read_only`
- Blocks `BEGIN READ WRITE`, `START TRANSACTION READ WRITE`

### Timezone

Set `timezone` to an IANA timezone name (e.g., `"America/New_York"`, `"Asia/Jakarta"`, `"UTC"`). Applied via `SET timezone` on every connection. Just like humans, AI agents sometimes forget to check what timezone a timestamp is in — this becomes a real problem when query results are combined with other datasets (like application logs) that use a different timezone. It's less headache to configure one timezone for your entire setup and never think about it again.

### Timeout Rules

Pattern-based timeout overrides. First matching rule wins; falls back to `default_timeout_seconds`. The query timeout covers the entire pipeline (execution + commit), so if you use hooks, set timeouts that account for hook processing time.

```json
{
  "query": {
    "timeout_rules": [
      {
        "pattern": "(?i)\\banalytics_events\\b",
        "timeout_seconds": 120
      },
      {
        "pattern": "(?i)\\bgenerate_series\\b",
        "timeout_seconds": 60
      }
    ]
  }
}
```

### Result Truncation

Query results are automatically truncated when they exceed `max_result_length` (default: 100,000 characters). This prevents oversized responses from overwhelming AI agents or consuming excessive tokens.

When truncation occurs, the rows are replaced with the truncated JSON and an error message:

```
...[truncated] Result is too long! Add limits in your query!
```

This prompts the AI agent to retry with a `LIMIT` clause or narrower `SELECT` columns. Configure via:

```json
{
  "query": {
    "max_result_length": 100000
  }
}
```

The `max_sql_length` setting (default: 100,000 bytes) similarly rejects queries that are too long before any processing occurs.

### Sanitization

Regex-based field-level data masking. Applied to individual cell values in query results. Recursive into JSONB objects and arrays. All rules are applied sequentially to each value.

```json
{
  "sanitization": [
    {
      "pattern": "(\\+62)\\d{6,}(\\d{3})",
      "replacement": "${1}xxx${2}",
      "description": "Mask Indonesian phone numbers: +6282123344789 → +62xxx789"
    },
    {
      "pattern": "\\d{16}",
      "replacement": "****",
      "description": "Mask 16-digit numbers (e.g., national ID, credit card)"
    }
  ]
}
```

### Error Prompts

Inject contextual guidance into error messages for AI agents. Regex patterns matched against the error message; matching prompts are appended with newline separators.

```json
{
  "error_prompts": [
    {
      "pattern": "permission denied",
      "message": "You don't have permission to access this table. Try using the read-only view instead."
    },
    {
      "pattern": "relation .* does not exist",
      "message": "Table not found. Use the list_tables tool to see available tables and their schemas."
    }
  ]
}
```

### Hooks (Server Mode)

Command-based hooks for the standalone server. Each hook specifies a regex pattern, a command path, and optional arguments. The command receives input via stdin and must return JSON on stdout.

Go hooks and command hooks are **mutually exclusive** — you cannot use both.

Hooks require `default_hook_timeout_seconds` to be set (> 0). The server panics on startup if hooks are configured without this.

#### BeforeQuery Hooks

Matched against the SQL query string. Can inspect, modify, or reject queries.

**Input:** Raw SQL query string via stdin.
**Expected output (JSON):**
```json
{
  "accept": true,
  "modified_query": "SELECT * FROM users WHERE active = true",
  "error_message": "Query rejected: reason"
}
```

- `accept`: `true` to allow, `false` to reject
- `modified_query`: optional modified SQL (passed to next hook in chain)
- `error_message`: custom error message when rejected. This message is returned directly to the AI agent — write instructions in it to steer the agent towards the correct path (e.g., `"This table is deprecated. Query the users_v2 table instead."`). This is dynamic agent steering.

#### AfterQuery Hooks

Matched against the complete result JSON string. Can inspect, modify, or reject results. Runs **before transaction commit** for write queries — rejection triggers rollback.

**Input:** Complete result JSON (columns, rows, rows_affected, error) via stdin.
**Expected output (JSON):**
```json
{
  "accept": true,
  "modified_result": "{...}",
  "error_message": "Result rejected: too many rows affected"
}
```

The `error_message` is returned directly to the AI agent — write instructions in it to guide the agent's next action (e.g., `"Too many rows affected. Use a more specific WHERE clause and try again."`). This is dynamic agent steering.

#### Hook Configuration

```json
{
  "default_hook_timeout_seconds": 5,
  "server_hooks": {
    "before_query": [
      {
        "pattern": "(?i)\\bDELETE\\b",
        "command": "/usr/local/bin/validate-delete",
        "args": ["--mode", "strict"],
        "timeout_seconds": 3
      }
    ],
    "after_query": [
      {
        "pattern": ".*",
        "command": "/usr/local/bin/audit-logger",
        "timeout_seconds": 2
      }
    ]
  }
}
```

#### Hook Behavior

- **Middleware chain**: hooks run sequentially; output from one feeds into the next.
- **Pattern matching**: regex matched against input. Modified input is re-matched for subsequent hooks.
- **Failure = rejection**: any crash, timeout, non-zero exit, or unparseable response stops the pipeline.
- **Security**: `exec.Command` with no shell context. Binary receives raw bytes on stdin. No shell injection possible at the transport level. If a hook author creates an unsafe script (e.g., `eval $(cat /dev/stdin)`), that is the hook author's responsibility — the MCP server does not create the vulnerability.
- **Logging**: hook stderr output is captured and logged (warn on failure, debug on success) but is separate from the expected JSON stdout response.
- **Concurrency**: number of concurrent hooks bounded by `pool.max_conns` via the shared semaphore.

### Hooks (Library Mode)

Go interface hooks avoid JSON serialization overhead and preserve native Go types. Unlike command hooks, library hooks have no regex pattern matching — the hook function itself decides whether to act, giving you full control inside your `Run` implementation.

```go
// BeforeQueryHook can inspect and modify queries before execution.
type BeforeQueryHook interface {
    Run(ctx context.Context, query string) (string, error)
}

// AfterQueryHook can inspect and modify results after execution.
type AfterQueryHook interface {
    Run(ctx context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error)
}
```

Register hooks via the config:

```go
config := pgmcp.Config{
    // ...
    DefaultHookTimeoutSeconds: 5,
    BeforeQueryHooks: []pgmcp.BeforeQueryHookEntry{
        {
            Name:    "audit-log",
            Timeout: 3 * time.Second, // 0 = use DefaultHookTimeoutSeconds
            Hook:    &myBeforeHook{},
        },
    },
    AfterQueryHooks: []pgmcp.AfterQueryHookEntry{
        {
            Name:    "row-limit-guard",
            Timeout: 2 * time.Second,
            Hook:    &myAfterHook{},
        },
    },
}
```

AfterQuery hooks receive native `*QueryOutput` with full Go type information (e.g., `int64` precision preserved). Return an error to reject — for write queries, this triggers a transaction rollback.

## Query Execution Pipeline

Every call to the `query` tool follows this pipeline:

```mermaid
flowchart TD
    A[Acquire semaphore] --> B[Check SQL length]
    B --> C[BeforeQuery hooks]
    C --> D[SQL protection checks]
    D --> E[Resolve timeout]
    E --> F[Acquire connection, begin transaction]
    F --> G[Execute query, collect results]
    G --> H{Read or Write?}

    H -- READ --> I[Rollback]
    H -- WRITE --> J[AfterQuery hooks]
    J -- approved --> K[Commit transaction]
    J -- rejected --> L[Rollback]

    I --> M[Sanitize results]
    K --> M
    L --> N[Return error]

    M --> O[Truncate results]
    O --> P[Log execution]
    P --> Q[Apply error prompts if error]
    Q --> R[Return QueryOutput]

    N --> S[Release semaphore]
    R --> S

    style A fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style B fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style C fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style D fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style E fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style F fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style G fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style H fill:#2d333b,stroke:#b06ce6,color:#c9d1d9
    style I fill:#2d333b,stroke:#5b8fef,color:#c9d1d9
    style J fill:#2d333b,stroke:#5b8fef,color:#c9d1d9
    style K fill:#2d333b,stroke:#5b8fef,color:#c9d1d9
    style L fill:#2d333b,stroke:#d160d4,color:#c9d1d9
    style M fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style N fill:#2d333b,stroke:#d160d4,color:#c9d1d9
    style O fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style P fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style Q fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style R fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
    style S fill:#2d333b,stroke:#56d4dd,color:#c9d1d9
```

Read-only statements (SELECT, EXPLAIN, SHOW, SET) are rolled back immediately after collecting results. Write statements (INSERT, UPDATE, DELETE, etc.) are committed only after AfterQuery hooks approve. AfterQuery hooks run for all queries — for read-only queries the transaction is already rolled back, so hooks can inspect results but cannot affect the transaction.

## SQL Protection Rules

**Layer 1: Protocol-level injection prevention.** All queries execute via pgx's extended query protocol (`QueryExecModeExec`). This mode only allows a single SQL statement per call — PostgreSQL itself rejects multi-statement strings at the protocol level. SQL injection through statement concatenation is impossible.

**Layer 2: AST-based protection.** On top of the protocol guarantee, every query is parsed using [pg_query_go](https://github.com/pganalyze/pg_query_go), which wraps PostgreSQL's actual C parser. This gives 100% parsing fidelity with real PostgreSQL — not a regex or hand-written parser.

The protection checker:
- Parses SQL into an AST
- Recursively walks the AST to detect disallowed operations
- Detects DML inside CTEs (e.g., `WITH x AS (DELETE FROM users RETURNING *) SELECT * FROM x`)
- Validates inner statements inside EXPLAIN/EXPLAIN ANALYZE
- Returns descriptive error messages explaining why a query was blocked

## Type Handling

Query results convert PostgreSQL types to JSON-friendly values. The Go type column shows the concrete type inside `QueryOutput.Rows` — relevant for library mode Go hooks.

| PostgreSQL Type | JSON Representation | Go Type |
|---|---|---|
| `NULL` | `null` | `nil` |
| `boolean` | `true` / `false` | `bool` |
| `smallint` | number | `int16` |
| `integer`, `serial` | number | `int32` |
| `bigint`, `bigserial` | number | `int64` |
| `real` | number (`NaN`, `Infinity`, `-Infinity` as strings) | `float32` or `string` |
| `double precision` | number (`NaN`, `Infinity`, `-Infinity` as strings) | `float64` or `string` |
| `numeric` / `decimal` | string (preserves arbitrary precision) | `string` |
| `money` | string (e.g., `"$1,234.56"`) | `string` |
| `text`, `varchar`, `char` | string | `string` |
| `enum` | string | `string` |
| `timestamp`, `timestamptz`, `date` | string (RFC3339Nano format) | `string` |
| `time` | string (`HH:MM:SS` or `HH:MM:SS.microseconds`) | `string` |
| `timetz` | string | `string` |
| `interval` | string (e.g., `"1 year(s) 2 mon(s) 3 day(s) 4h5m6s"`) | `string` |
| `uuid` | string (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) | `string` |
| `bytea` | string (base64 encoded) | `string` |
| `xml` | string (base64 encoded) | `string` |
| `json` / `jsonb` | native JSON (objects and arrays, recursively converted) | `map[string]interface{}`, `[]interface{}`, or primitive |

> **Note:** pgx parses JSONB numbers as `float64`, so large integers inside JSONB (e.g., `9007199254740993`) lose precision — store them as strings in your JSON if you need exact values.
| `array` (e.g., `int[]`, `text[]`) | JSON array (recursively converted) | `[]interface{}` |
| `inet` / `cidr` | string (e.g., `"192.168.1.0/24"`) | `string` |
| `macaddr` / `macaddr8` | string (e.g., `"08:00:2b:01:02:03"`) | `string` |
| `point` | string (`"(x,y)"`) | `string` |
| `line` | string (`"{A,B,C}"`) | `string` |
| `lseg` | string (`"[(x1,y1),(x2,y2)]"`) | `string` |
| `box` | string (`"(x1,y1),(x2,y2)"`) | `string` |
| `path` | string (closed: `"((x1,y1),(x2,y2))"`, open: `"[(x1,y1),(x2,y2)]"`) | `string` |
| `polygon` | string (`"((x1,y1),(x2,y2),(x3,y3))"`) | `string` |
| `circle` | string (`"<(x,y),r>"`) | `string` |
| `bit` / `varbit` | string (binary digits, e.g., `"10110"`) | `string` |
| `int4range`, `tsrange`, etc. | string (e.g., `"[1,10)"`, `"empty"`) | `string` |
| `tsvector` / `tsquery` | string | `string` |
| `composite` | string (e.g., `"(val1,val2,val3)"`) | `string` |
| `domain` | same as underlying type | same as underlying type |

## Recommended Configurations

AI agents are a fundamentally different kind of database client. They're capable but unpredictable — they can write complex queries across dozens of tables, but they can also `DELETE FROM users` without a `WHERE` clause if you let them. The configuration options in postgres-mcp exist to give agents useful access while keeping you in control.

Here are starting points for common scenarios.

### Analytics / Read-Only Exploration

The AI agent explores your data, runs reports, and answers questions. It should never modify anything.

```json
{
  "read_only": true,
  "protection": {
    "allow_copy_to": false
  },
  "query": {
    "default_timeout_seconds": 30,
    "max_result_length": 50000,
    "timeout_rules": [
      { "pattern": "(?i)\\bgenerate_series\\b", "timeout_seconds": 60 }
    ]
  },
  "error_prompts": [
    {
      "pattern": "permission denied",
      "message": "You don't have permission. Try a different table or use list_tables to see what's available."
    },
    {
      "pattern": "canceling statement due to statement timeout",
      "message": "Query timed out. Add a LIMIT clause or narrow your WHERE conditions."
    }
  ]
}
```

Start here. `read_only: true` and all protection rules at their defaults (blocked) gives you the tightest possible access. Error prompts steer the agent when it hits walls instead of leaving it to guess.

### Development Assistant

The AI agent helps with development — creating tables, writing migrations, inserting test data. You trust it more, but still want guardrails.

```json
{
  "read_only": false,
  "protection": {
    "allow_ddl": true,
    "allow_drop": false,
    "allow_truncate": false,
    "allow_delete_without_where": false,
    "allow_update_without_where": false
  },
  "query": {
    "default_timeout_seconds": 30
  }
}
```

This allows `CREATE TABLE`, `ALTER TABLE`, and `INSERT` while still blocking mass deletes, mass updates, `DROP`, and `TRUNCATE`. The agent can build and populate schemas but can't accidentally destroy data.

### Production with PII Protection

The AI agent queries production data for support or debugging, but must never see raw PII.

```json
{
  "read_only": true,
  "sanitization": [
    {
      "pattern": "(\\+62)\\d{6,}(\\d{3})",
      "replacement": "${1}xxx${2}",
      "description": "Mask phone numbers"
    },
    {
      "pattern": "([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+)",
      "replacement": "***@${2}",
      "description": "Mask email local part"
    },
    {
      "pattern": "(\\d{4})\\d{8}(\\d{4})",
      "replacement": "${1}********${2}",
      "description": "Mask 16-digit card/ID numbers"
    }
  ],
  "query": {
    "default_timeout_seconds": 15,
    "max_result_length": 30000
  }
}
```

Sanitization rules mask field values in query results before they reach the AI agent. The agent sees enough to be useful (`+62xxx789`, `***@company.com`) without accessing raw PII. Keep `max_result_length` low so the agent doesn't pull large datasets.

## Security Model

### What postgres-mcp does

- **AST-based SQL validation** using PostgreSQL's actual C parser — not regex. 100% parsing fidelity.
- **SQL injection prevention** via pgx extended query protocol (`QueryExecModeExec`). Multi-statement strings are rejected by PostgreSQL itself in this mode.
- **23 protection rules**, all blocked by default. Explicitly opt-in to each operation category.
- **Transaction management** — each query runs in a managed transaction. Users cannot issue `BEGIN`, `COMMIT`, or `ROLLBACK`.
- **Read-only mode** enforced at both connection and protection levels.
- **Hook security** — `exec.Command` with no shell context. Input via stdin, not command-line arguments. No shell injection at the transport level.
- **Semaphore-based concurrency control** — total concurrent operations bounded by pool size.

### What postgres-mcp does NOT do

- **No authentication or authorization.** The server has no auth mechanism. It is designed to run in trusted environments only (local machine, internal network, behind a reverse proxy with auth).
- **No CORS headers.** Intentionally omitted — this is not a browser-facing API. Adding CORS would create an attack surface if the server is accidentally exposed.
- **No network encryption.** Use a reverse proxy (nginx, Caddy) for TLS if needed.

**Deploy postgres-mcp only in trusted environments.** For multiple databases, run separate instances with different connection strings.

## CLI Reference

```
gopgmcp serve       Start the MCP server
gopgmcp configure   Run interactive configuration wizard
gopgmcp doctor      Validate config and show agent connection snippets
gopgmcp --version   Show version
gopgmcp --help      Show help
```

### Environment Variables

| Variable | Description |
|---|---|
| `GOPGMCP_PG_CONNSTRING` | Full PostgreSQL connection string. Skips interactive credential prompts. |
| `GOPGMCP_CONFIG_PATH` | Override config file path (default: `.gopgmcp/config.json`) |

## Library API

### Constructor

```go
func New(ctx context.Context, connString string, config Config, logger zerolog.Logger, opts ...Option) (*PostgresMcp, error)
```

Panics on invalid config. Returns error for runtime failures (pool creation, invalid regex patterns).

### Methods

All methods are goroutine-safe.

```go
// Execute SQL. All errors go to output.Error, never returns a Go error.
func (p *PostgresMcp) Query(ctx context.Context, input QueryInput) *QueryOutput

// List accessible tables. Returns Go error for infrastructure failures.
func (p *PostgresMcp) ListTables(ctx context.Context, input ListTablesInput) (*ListTablesOutput, error)

// Describe table schema. Returns Go error for infrastructure failures.
func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error)

// Close the connection pool.
func (p *PostgresMcp) Close(ctx context.Context)
```

### Options

```go
// Pass command-based hooks (mutually exclusive with Go hooks in Config).
pgmcp.WithServerHooks(pgmcp.ServerHooksConfig{...})
```

### MCP Tool Registration

```go
// Register query, list_tables, describe_table as MCP tools.
pgmcp.RegisterMCPTools(mcpServer, pgMcp)
```

### Example: OpenAI Tool Calling

Use postgres-mcp as tools with the [OpenAI Go SDK](https://github.com/openai/openai-go) (`v3`) Chat Completions API. The model decides when to call `query`, `list_tables`, or `describe_table`, and your code executes them.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/openai/openai-go/v3"
	pgmcp "github.com/rickchristie/postgres-mcp"
	"github.com/rs/zerolog"
)

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// 1. Create postgres-mcp instance
	p, err := pgmcp.New(ctx, os.Getenv("GOPGMCP_PG_CONNSTRING"), pgmcp.Config{
		Pool:     pgmcp.PoolConfig{MaxConns: 5},
		Query:    pgmcp.QueryConfig{DefaultTimeoutSeconds: 30, ListTablesTimeoutSeconds: 10, DescribeTableTimeoutSeconds: 10},
		ReadOnly: true,
	}, logger)
	if err != nil {
		panic(err)
	}
	defer p.Close(ctx)

	// 2. Define tools for OpenAI
	tools := []openai.ChatCompletionToolUnionParam{
		openai.ChatCompletionFunctionTool(openai.FunctionDefinitionParam{
			Name:        "query",
			Description: openai.String("Execute a SQL query against the PostgreSQL database. Returns results as JSON."),
			Parameters: openai.FunctionParameters{
				"type": "object",
				"properties": map[string]any{
					"sql": map[string]string{"type": "string", "description": "The SQL query to execute"},
				},
				"required": []string{"sql"},
			},
		}),
		openai.ChatCompletionFunctionTool(openai.FunctionDefinitionParam{
			Name:        "list_tables",
			Description: openai.String("List all tables, views, and materialized views accessible to the current user."),
			Parameters:  openai.FunctionParameters{"type": "object", "properties": map[string]any{}},
		}),
		openai.ChatCompletionFunctionTool(openai.FunctionDefinitionParam{
			Name:        "describe_table",
			Description: openai.String("Describe the schema of a table including columns, indexes, constraints, and foreign keys."),
			Parameters: openai.FunctionParameters{
				"type": "object",
				"properties": map[string]any{
					"table":  map[string]string{"type": "string", "description": "The table name"},
					"schema": map[string]string{"type": "string", "description": "Schema name (defaults to public)"},
				},
				"required": []string{"table"},
			},
		}),
	}

	// 3. Start conversation
	client := openai.NewClient()
	messages := []openai.ChatCompletionMessageParamUnion{
		openai.SystemMessage("You are a database analyst. Use the provided tools to answer questions about the database."),
		openai.UserMessage("What tables are available and how many rows does each have?"),
	}

	// 4. Agentic loop — keep going until the model stops calling tools
	for {
		completion, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
			Model:    openai.ChatModelGPT4o,
			Messages: messages,
			Tools:    tools,
		})
		if err != nil {
			panic(err)
		}

		choice := completion.Choices[0]
		messages = append(messages, choice.Message.ToParam())

		// No more tool calls — print final response and exit
		if len(choice.Message.ToolCalls) == 0 {
			fmt.Println(choice.Message.Content)
			break
		}

		// Execute each tool call
		for _, tc := range choice.Message.ToolCalls {
			result := executeTool(ctx, p, tc.Function.Name, []byte(tc.Function.Arguments))
			messages = append(messages, openai.ToolMessage(result, tc.ID))
		}
	}
}

func executeTool(ctx context.Context, p *pgmcp.PostgresMcp, name string, args []byte) string {
	switch name {
	case "query":
		var input pgmcp.QueryInput
		json.Unmarshal(args, &input)
		output := p.Query(ctx, input)
		b, _ := json.Marshal(output)
		return string(b)

	case "list_tables":
		output, err := p.ListTables(ctx, pgmcp.ListTablesInput{})
		if err != nil {
			return fmt.Sprintf(`{"error":"%s"}`, err.Error())
		}
		b, _ := json.Marshal(output)
		return string(b)

	case "describe_table":
		var input pgmcp.DescribeTableInput
		json.Unmarshal(args, &input)
		output, err := p.DescribeTable(ctx, input)
		if err != nil {
			return fmt.Sprintf(`{"error":"%s"}`, err.Error())
		}
		b, _ := json.Marshal(output)
		return string(b)

	default:
		return fmt.Sprintf(`{"error":"unknown tool: %s"}`, name)
	}
}
```

All protection rules, sanitization, error prompts, and timeouts are handled by postgres-mcp — your tool execution code stays simple.

### Example: Custom MCP Server with Go Hooks

Build your own `serve` command with Go hooks instead of command-based hooks. This lets you write hook logic in Go with full type safety and no serialization overhead.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	pgmcp "github.com/rickchristie/postgres-mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/rs/zerolog"
)

// AuditHook logs every query before execution.
type AuditHook struct {
	logger zerolog.Logger
}

func (h *AuditHook) Run(ctx context.Context, query string) (string, error) {
	h.logger.Info().Str("sql", query).Msg("query received")
	return query, nil // pass through unmodified
}

// RowLimitGuard rejects write queries that affect too many rows.
type RowLimitGuard struct {
	MaxRows int64
}

func (h *RowLimitGuard) Run(ctx context.Context, result *pgmcp.QueryOutput) (*pgmcp.QueryOutput, error) {
	if result.RowsAffected > h.MaxRows {
		return nil, fmt.Errorf(
			"rejected: %d rows affected exceeds limit of %d. Use a more specific WHERE clause.",
			result.RowsAffected, h.MaxRows,
		)
	}
	return result, nil
}

// BlockedTableHook prevents queries against sensitive tables.
type BlockedTableHook struct {
	Tables []string
}

func (h *BlockedTableHook) Run(ctx context.Context, query string) (string, error) {
	lower := strings.ToLower(query)
	for _, table := range h.Tables {
		if strings.Contains(lower, strings.ToLower(table)) {
			return "", fmt.Errorf(
				"access to %s is not allowed. Use the %s_summary view instead.",
				table, table,
			)
		}
	}
	return query, nil
}

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	p, err := pgmcp.New(ctx, os.Getenv("GOPGMCP_PG_CONNSTRING"), pgmcp.Config{
		Pool:     pgmcp.PoolConfig{MaxConns: 5},
		Query:    pgmcp.QueryConfig{DefaultTimeoutSeconds: 30, ListTablesTimeoutSeconds: 10, DescribeTableTimeoutSeconds: 10},
		ReadOnly: true,

		// Go hooks — no JSON serialization, full type safety
		DefaultHookTimeoutSeconds: 5,
		BeforeQueryHooks: []pgmcp.BeforeQueryHookEntry{
			{
				Name: "audit",
				Hook: &AuditHook{logger: logger},
			},
			{
				Name:    "blocked-tables",
				Timeout: 1 * time.Second,
				Hook:    &BlockedTableHook{Tables: []string{"salaries", "credentials"}},
			},
		},
		AfterQueryHooks: []pgmcp.AfterQueryHookEntry{
			{
				Name: "row-limit-guard",
				Hook: &RowLimitGuard{MaxRows: 1000},
			},
		},

		ErrorPrompts: []pgmcp.ErrorPromptRule{
			{Pattern: "not allowed", Message: "This table is restricted. Use list_tables to find accessible views."},
		},
	}, logger)
	if err != nil {
		panic(err)
	}
	defer p.Close(ctx)

	// Register as MCP server
	mcpServer := server.NewMCPServer("my-postgres-mcp", "1.0.0",
		server.WithToolCapabilities(true),
	)
	pgmcp.RegisterMCPTools(mcpServer, p)

	// Start HTTP server
	mux := http.NewServeMux()
	httpSrv := &http.Server{Addr: ":8080", Handler: mux}
	streamable := server.NewStreamableHTTPServer(mcpServer,
		server.WithEndpointPath("/mcp"),
		server.WithStateLess(true),
		server.WithStreamableHTTPServer(httpSrv),
	)
	mux.Handle("/mcp", streamable)

	logger.Info().Msg("starting custom MCP server on :8080")
	if err := streamable.Start(":8080"); !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
```

This gives you the same MCP server as `gopgmcp serve`, but with hooks written in Go. The `AuditHook` logs every query, `BlockedTableHook` prevents access to sensitive tables (with an error message that steers the agent to use a view instead), and `RowLimitGuard` rejects write operations that affect too many rows — rolling back the transaction automatically.

## Contributing

### Running Tests

Integration tests run against real PostgreSQL databases managed by [pgflock](https://github.com/rickchristie/govner/tree/main/pgflock). pgflock spins up memory-backed PostgreSQL containers in Docker and provides database locking so tests can run in parallel.

```bash
# Install pgflock
go install github.com/rickchristie/govner/pgflock@latest

# First-time setup (creates .pgflock/ config — already included in this repo)
# pgflock configure

# Build the PostgreSQL Docker image
pgflock build

# Start the database pool (runs in foreground with TUI)
pgflock up
```

With `pgflock up` running in a separate terminal:

```bash
# Run unit tests
go test ./...

# Run integration tests (requires pgflock)
go test -tags integration ./...

# Run all tests
go test -tags integration -race ./...
```

## Made with Claude

This project was built in collaboration with [Claude Code](https://claude.ai/claude-code). Architecture, implementation, tests, documentation — all of it was a conversation. The best parts came from the back-and-forth: a human who knew exactly what the tool should do, and an AI that could help make it real. It was genuinely fun to build.

## License

[MIT](LICENSE)
