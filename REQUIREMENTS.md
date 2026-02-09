Create Postgres MCP with Golang.

# Complete Requirements

- Great code architecture, with comprehensive unit tests for both correctness, race condition, and stress testing.
- Supports only HTTP transport type.
  - Does not support SSE.
  - Does not support CORS and explicitly does not set any CORS headers. CORS is only enforced by browsers —
    intended clients (AI agents, CLI tools, internal services) connect via plain HTTP where CORS doesn't apply.
    Adding CORS headers would only create an attack vector if the server is accidentally exposed (any malicious webpage could make requests to it).
    This is not designed to be run as public server (no auth!) - only in trusted environment (local or internal server service, not exposed to public).
    We need to make this very clear in the documentation.
  - Targets 2025-03-26 (Streamable HTTP) for compatibility.
- Supports only Postgres database.
- Supports multiple connections at once to a single database, production-grade, ready and scalable.
  - When needing multiple databases, just run multiple instances of the MCP server with different connection strings.
- Uses pgx library for Postgres connections and running the query:
  - The extended query protocol (used by default for parameterized queries) inherently only supports a single statement — Postgres itself 
    rejects multi-statement strings in this mode.
  - conn.Query(ctx, sql, pgx.QueryExecModeExec)
  - This prevents SQL injection attacks.
- Uses pg_query_go to parse SQL and check for disallowed statements (e.g. DROP, TRUNCATE, DELETE without WHERE, etc):
  - Uses PostgreSQL's actual parser via cgo — not a regex or hand-written parser. This means 100% parsing fidelity with real Postgres.
  - Returns proper AST - we recursively walk and traverse through it to check for disallowed statements, including DML inside CTEs (e.g. `WITH x AS (DELETE FROM users RETURNING *) SELECT * FROM x`) and inner statements inside EXPLAIN/EXPLAIN ANALYZE.
- Handles complex strings in SQLs.
  - Supports JSONB, arrays, nested queries, CTEs, etc.
  - JSONB, arrays etc are returned as proper JSON, JSON arrays, not as stringified values.
- Have these tools:
  - Query:
    - Returns only an output struct (no Go error). All errors (Postgres errors, protection rejections, hook rejections, Go errors) are converted into the output's error message field. This error message is then evaluated against error_prompts regex patterns for appending additional guidance.
    - Returns RAW results, formatted as JSON. Includes rows affected count for DML statements (INSERT/UPDATE/DELETE) even without RETURNING clause.
    - The query is run in a transaction.
  - ListTables:
    - Returns list of tables in the connected database (includes views, materialized views, foreign tables, partitioned tables).
    - Can list everything that is granted to the connected user.
    - Tables where the user has SELECT privilege but lacks schema USAGE privilege are listed separately with a flag/context indicating restricted schema access, so AI agents can make informed decisions.
    - Must acquire semaphore before executing (same semaphore as Query — ensures total concurrent operations are bounded by pool size).
    - Must have a configurable timeout (`list_tables_timeout_seconds`). Must be > 0, no default — user must explicitly set in config. Server panics on start if not set.
  - DescribeTable:
    - Returns table schema for the specified table (supports tables, views, materialized views, foreign tables, partitioned tables).
    - Returns everything including indexes, constraints, foreign keys, partition information, etc.
    - Schema parameter defaults to "public" when not specified.
    - Can describe everything that is granted to the connected user.
    - Must acquire semaphore before executing (same semaphore as Query — ensures total concurrent operations are bounded by pool size).
    - Must have a configurable timeout (`describe_table_timeout_seconds`). Must be > 0, no default — user must explicitly set in config. Server panics on start if not set.
- Reads config file and `gopgmcp configure` command to help create the config file:
  - Config file located at `<workdir>/.gopgmcp/config.json`. Will read it from working directory.
    - This can be overridden by environment variable `GOPGMCP_CONFIG_PATH`.
  - `gopgmcp configure` is interactive, command will either create or update existing (showing current config values).
    - It prints all possible config options, with current values (if any) - one by one.
    - User can just press enter to keep current value.
    - For configuration values that are arrays, the script presents current entries with their indexes, then ask if the user wants to add or remove existing entries, or continue to next config parameter.
      - For each entry to be added, it will ask for the required fields one by one.
      - For each entry to be removed, user can input index numbers to remove.
      - After entry is added/removed, it goes back to the main array config menu, showing updated entries, until user decides to continue to next config parameter.
    - At the end, it writes the config file to the location.
  - Can specify which additional string/instruction to be returned when the error message contains specific message.
    For example, if error matches regex "xxxx", then append this string to the RAW result when when returning (used for dynamic prompt injection, help steer AI agents). 
    Custom prompt injections are always evaluated and appended following array ordering, top to bottom. When multiple prompts match, they are concatenated with newline separators — each prompt is displayed as its own paragraph.
    - The error prompt matching is against ALL error messages (hook error messages are included, Golang errors too), not just specific Postgres error codes.
  - Can also specify additional pattern matching for sanitization. For example, I can create matching regex for KTP ID or phone numbers with capture groups, and then a sanitized string that can be used to turn "+62821233447" to "+62xxx447". This can be done to dynamically sanitize the response as data security policy for the AI Agent. Sanitization is always applied when match, and is applied from top to bottom following the array ordering.
    This means the same sanitization regex should not be duplicated in multiple entries, as only the first match will be applied.
    - Sanitization is run against individual cell/field value in the JSON result.
    - For JSONB, array fields, they are run against each primitive value inside the JSONB/array.
  - Can specify max timeout, similar format, using regex for matching against raw SQL query string.
    - There is list of regex pattern and the timeout in seconds. 
    - When the first match is found, the rest of the regex are not evaluated.
    - Useful for queries/tables that are known to be slow, so that AI agents can be given more time to wait for the results.
  - Default timeout if no regex match found. Must be > 0, no default value — user must explicitly set this in config. Server panics on start if not set.
  - Hooks map. Each hook is a map of regex pattern matching with bash command that will be executed with inputs that matches with the regex:
    - BeforeQuery - can reject Query, based on content. Regex matches against SQL query string.
      - Input: RAW query string, passed as stdin to the bash command.
      - Must return JSON with "accept": true/false, and optional "modified_query": "new query string" and "error_message": "custom error message when rejected".
      - Hooks are run first before protection checks. Hook output is always validated against protection rules.
      - Hooks are run in middleware fashion. The modified query from one hook is passed to the next hook as input.
      - If any hook rejects, the whole Query is rejected.
      - Hooks are matched against the query first, then executed. If the query is modified, the next hook will be matched against the modified query.
    - AfterQuery - can reject, modify RAW results, based on content. Regex matches against complete RAW results JSON string (includes columns and error fields).
      - Input: complete RAW results JSON, passed as stdin to the bash command.
      - Must return JSON with "accept": true/false, and optional "modified_result": "new RAW JSON result string" and "error_message": "custom error message when rejected".
      - Hooks are run first before sanitization.
      - Hooks are run in middleware fashion. The modified result from one hook is passed to the next hook as input.
      - If any hook rejects, the whole Query is rejected.
      - Hooks are matched against the RAW result first, then executed. If the result is modified, the next hook will be matched against the modified result.
    - Each hook entry specifies the command path and optional arguments array. The command is executed directly via Go's exec.Command (no shell), with arguments passed separately.
    - Hook timeout in seconds. Applies per hook. If not specified, falls back to default hook timeout. Default hook timeout must be > 0, no default value — user must explicitly set this in config when hooks are configured. Server panics on start if hooks exist and this is not set.
    - When 1 hook crashes, times out, returns non-zero exit code, or returns unparseable (non-JSON) content, the entire query pipeline stops and is treated as an error. Hooks are a critical part of the guardrails — a failing hook means the guardrail cannot verify the query/result, so the safe default is to reject.
      - The error message must be descriptive: include which hook failed, the command path, and the reason (crash, timeout, bad exit code, parse error).
    - The number of hooks being run is equal to the amount of connection in the pgxpool:
      - The system reads pgxpool config of max connections - it then forces a lock that for that amount that encompasses the transaction, Before and After hooks.
      - This ensures predictable resource usage when deployed.
    - Hook security:
      - Go's exec.Command passes no shell context. The hook binary receives raw bytes on stdin. No injection possible at the transport level.
      - If a hook author does something reckless like eval `cat /dev/stdin`, that's on them. But the MCP server itself isn't creating the vulnerability. We need to properly document this for users.
  - Default hook timeout in seconds.
  - Library mode hooks (Go interfaces):
    - In library mode, hooks are Go interfaces instead of command-line scripts. This avoids JSON serialization overhead, preserves Go type information, and is the natural choice for Go library consumers.
    - `BeforeQueryHook` interface: receives SQL query string, returns (possibly modified) query string or error to reject.
    - `AfterQueryHook` interface: receives `*QueryOutput` directly (native Go types), returns (possibly modified) `*QueryOutput` or error to reject.
    - No JSON round-trip for library hooks — AfterQueryHook works with Go structs directly, preserving int64 precision and all type information.
    - No regex pattern matching for library hooks — the hook function itself decides whether to act (user has full control inside their Run implementation).
    - Same timeout and pipeline-stopping behavior as command hooks — any hook failure stops the pipeline.
    - Library hooks and command hooks are mutually exclusive — if Go hooks are set in Config, command-based HooksConfig is ignored.
- MCP reads connection string through environment variable `GOPGMCP_PG_CONNSTRING`.
  - It's postgresql connection string  - so whether it's sslmode, etc. - can be specified here. It has highest priority.
  - If connection string from environment is not found, server will try to read host, port, dbname, and sslmode from configuration file.
    - Username and password is not read from config file.
    - Username and password is then asked to the user interactively on server start.
    - This provides flexibility for users to not store username/password in config file and environment variable, providing it interactively on server start - recommended when running it locally.
- Other configuration:
  - HTTP port to listen on. No default port - must be specified in config file, server panics if not found.
  - Read-only mode. If true, only allow SELECT queries and other queries that do not modify data - starts connections in read-only mode.
    - When Read-only mode is on. Even when SET is allowed (`allow_set: true`), we detect and reject any attempt to change transaction mode to write.
  - Connection pool config - max connections, min connections, idle timeout, etc - this should mirror pgxpool config options.
  - Logging config - log level, output format (json, text), output file (stdout, file path).
  - Health check endpoint - for load balancers/k8s probes. Health check confirms the MCP server process is running and responsive — it does NOT check database connectivity.
  - Protection (each rule can be individually toggled — Go zero-value `false` = blocked, which is the safe default):
    - SET - blocked by default (`allow_set: false`).
    - DROP - blocked by default (`allow_drop: false`).
    - TRUNCATE - blocked by default (`allow_truncate: false`).
    - DO $$ blocks - blocked by default (`allow_do: false`). DO blocks can execute arbitrary SQL inside PL/pgSQL, bypassing all other protection checks.
    - COPY FROM - blocked by default (`allow_copy_from: false`). COPY FROM can bulk-import data into tables. COPY TO (export) is not blocked.
    - CREATE FUNCTION / CREATE PROCEDURE - blocked by default (`allow_create_function: false`). These can create server-side functions containing arbitrary SQL that bypasses protection checks when called, similar to DO blocks.
    - PREPARE - blocked by default (`allow_prepare: false`). Prepared statements persist at the session level and can be executed later via EXECUTE, bypassing protection checks on the prepared content.
    - ALTER SYSTEM - blocked by default (`allow_alter_system: false`). ALTER SYSTEM modifies `postgresql.auto.conf` and can change any server-level parameter. Dangerous examples: `shared_preload_libraries` (load arbitrary libraries), `archive_command` (execute arbitrary shell commands), `ssl = off` (disable encryption), `listen_addresses = '*'` (expose to network). Requires superuser, but dev environments often connect as superuser.
    - DELETE without WHERE - blocked by default (`allow_delete_without_where: false`).
    - UPDATE without WHERE - blocked by default (`allow_update_without_where: false`).
    - Multi-statement queries (e.g. `SELECT 1; DROP TABLE users`) - always blocked, cannot be toggled.
    - EXPLAIN / EXPLAIN ANALYZE - always recurses into the inner statement and applies all protection rules. `EXPLAIN ANALYZE` actually executes the query, so `EXPLAIN ANALYZE DELETE FROM users` must be blocked when DELETE without WHERE is blocked. This is not togglable — EXPLAIN always checks its inner statement.
    - When Read-only mode is on, additionally block:
      - `RESET ALL` and `RESET default_transaction_read_only` (could disable read-only mode).
      - `BEGIN READ WRITE` / `START TRANSACTION READ WRITE` (explicit write transaction).
  - Max result length (in character length). Applied to the JSON result, if exceeded, truncate and append "...[truncated] Result is too long! Add limits in your query!". Defaults to 100000 if not set (0). Cannot be disabled — there is no "no limit" option.
  - Health check path. No default — must be explicitly set when health check is enabled. Server panics on start if health check is enabled and path is empty.
- Authentication:
  - This MCP server is designed to be run in local or trusted environment, so no authentication for clients.
- Server CLI command:
  - All logs are printed out.
  - People can install as CLI tool and run as long as they have Golang.
- This Golang MCP library is not only startable as CLI MCP server, but also a library that can be initialized and then registered as internal Agent Loop code as tool call.
  - In library mode, PostgresMcp is instantiated with a full PostgreSQL connection string (must include credentials) and a Config object. Unlike CLI mode, library mode does not read connection details from Config.Connection fields — the connection string is the sole source of connection information.
  - PostgresMcp hooks, sanitization, etc can also be passed from the Config object.
  - The API for library mode is the tool calls. For each tool (e.g. Query, ListTables, DescribeTable) - there's a function that can be called directly.
    - Each function takes context, and input struct, and returns output struct and error.
  - All tool functions (Query, ListTables, DescribeTable) are safe for concurrent use from multiple goroutines. All internal state is either immutable after construction or goroutine-safe (pgxpool, channel semaphore, zerolog).
- Use zerolog as logger.
- No graceful shutdown needed. If server is killed, close all connections immediately.
- Config validation panics on startup (not errors). This is intentional — both CLI and library mode are expected to initialize at application startup. Missing/invalid config values should crash immediately rather than produce subtle runtime failures. Library users call `New()` during initialization, so panics are caught at startup.

# Sample config JSON

```json
  {
    "connection": {
      "host": "localhost",
      "port": 5432,
      "dbname": "myapp",
      "sslmode": "prefer"
    },
    "pool": {
      "max_conns": 10,
      "min_conns": 2,
      "max_conn_lifetime": "1h",
      "max_conn_idle_time": "30m",
      "health_check_period": "1m"
    },
    "server": {
      "port": 8080,
      "health_check_enabled": true,
      "health_check_path": "/health-check",
      "read_only": false
    },
    "logging": {
      "level": "info",
      "format": "json",
      "output": "stdout"
    },
    "protection": {
      "allow_set": false,
      "allow_drop": false,
      "allow_truncate": false,
      "allow_do": false,
      "allow_copy_from": false,
      "allow_create_function": false,
      "allow_prepare": false,
      "allow_delete_without_where": false,
      "allow_update_without_where": false,
      "allow_alter_system": false
    },
    "query": {
      "default_timeout_seconds": 30,
      "list_tables_timeout_seconds": 10,
      "describe_table_timeout_seconds": 10,
      "max_result_length": 100000,
      "timeout_rules": [
        {
          "pattern": "(?i)SELECT.*pg_stat",
          "timeout_seconds": 5
        },
        {
          "pattern": "(?i)SELECT.*JOIN.*JOIN.*JOIN",
          "timeout_seconds": 60
        }
      ]
    },
    "error_prompts": [
      {
        "pattern": "(?i)permission denied",
        "message": "This table requires elevated privileges. Try querying the _readonly view instead."
      },
      {
        "pattern": "(?i)relation.*does not exist",
        "message": "Use the ListTables tool first to discover available table names."
      }
    ],
    "sanitization": [
      {
        "pattern": "(\\+62)(\\d{3})(\\d+)(\\d{3})",
        "replacement": "${1}xxx${4}",
        "description": "Mask Indonesian phone numbers"
      },
      {
        "pattern": "(\\d{2})(\\d{10})(\\d{4})",
        "replacement": "${1}xxxxxxxxxx${3}",
        "description": "Mask KTP numbers"
      }
    ],
    "hooks": {
      "default_timeout_seconds": 10,
      "before_query": [
        {
          "pattern": "(?i)SELECT.*FROM.*users",
          "command": "/usr/local/bin/audit-query.sh",
          "args": ["--audit", "--log-level=info"],
          "timeout_seconds": 5
        }
      ],
      "after_query": [
        {
          "pattern": ".*",
          "command": "/usr/local/bin/redact-pii.sh",
          "args": [],
          "timeout_seconds": 10
        }
      ]
    }
  }
```
