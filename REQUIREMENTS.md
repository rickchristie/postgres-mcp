Create Postgres MCP with Golang.

# Complete Requirements

- Great code architecture, with comprehensive unit tests for both correctness, race condition, and stress testing.
- Supports only HTTP transport type.
  - Does not support SSE.
  - Does not support CORS, as it is not designed to be run as public server (no auth!) - 
    only in trusted environment (local or internal server service, not exposed to public). We need to make this very clear in the documentation.
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
  - Returns proper AST - we walk and traverse through it to check for disallowed statements.
- Handles complex strings in SQLs.
  - Supports JSONB, arrays, nested queries, CTEs, etc.
  - JSONB, arrays etc are returned as proper JSON, JSON arrays, not as stringified values.
- Have these tools:
  - Query:
    - Returns error messages directly whenever there are query errors.
    - Returns RAW results, formatted as JSON.
    - The query is run in a transaction.
  - ListTables:
    - Returns list of tables in the connected database (includes views, materialized views, foreign tables).
    - Can list everything that is granted to the connected user.
  - DescribeTable:
    - Returns table schema for the specified table.
    - Returns everything including indexes, constraints, foreign keys, etc.
    - Can describe everything that is granted to the connected user.
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
    Custom prompt injections are always evaluated and appended following array ordering, top to bottom.
    - The error prompt matching is against ALL error messages (hook error messages are included, Golang errors too), not just specific Postgres error codes.
  - Can also specify additional pattern matching for sanitization. For example, I can create matching regex for KTP ID or phone numbers with capture groups, and then a sanitized string that can be used to turn "+62821233447" to "+62xxx447". This can be done to dynamically sanitize the response as data security policy for the AI Agent. Sanitization is always applied when match, and is applied from top to bottom following the array ordering.
    This means the same sanitization regex should not be duplicated in multiple entries, as only the first match will be applied.
    - Sanitization is run against individual cell/field value in the JSON result.
    - For JSONB, array fields, they are run against each primitive value inside the JSONB/array.
  - Can specify max timeout, similar format, using regex for matching against raw SQL query string.
    - There is list of regex pattern and the timeout in seconds. 
    - When the first match is found, the rest of the regex are not evaluated.
    - Useful for queries/tables that are known to be slow, so that AI agents can be given more time to wait for the results.
  - Default timeout if no regex match found.
  - Hooks map. Each hook is a map of regex pattern matching with bash command that will be executed with inputs that matches with the regex:
    - BeforeQuery - can reject Query, based on content. Regex matches against SQL query string.
      - Input: RAW query string, passed as stdin to the bash command.
      - Must return JSON with "accept": true/false, and optional "modified_query": "new query string" and "error_message": "custom error message when rejected".
      - Hooks are run first before protection checks. Hook output is always validated against protection rules.
      - Hooks are run in middleware fashion. The modified query from one hook is passed to the next hook as input.
      - If any hook rejects, the whole Query is rejected.
      - Hooks are matched against the query first, then executed. If the query is modified, the next hook will be matched against the modified query.
    - AfterQuery - can reject, modify RAW results, based on content. Regex matches against RAW results JSON string.
      - Input: RAW results JSON, passed as stdin to the bash command.
      - Must return JSON with "accept": true/false, and optional "modified_result": "new RAW JSON result string" and "error_message": "custom error message when rejected".
      - Hooks are run first before sanitization.
      - Hooks are run in middleware fashion. The modified result from one hook is passed to the next hook as input.
      - If any hook rejects, the whole Query is rejected.
      - Hooks are matched against the RAW result first, then executed. If the result is modified, the next hook will be matched against the modified result.
    - Hook timeout in seconds. Applies per hook.
    - When 1 hook crashes/times out, it does not stop the whole process, just log the error and continue.
    - The number of hooks being run is equal to the amount of connection in the pgxpool:
      - The system reads pgxpool config of max connections - it then forces a lock that for that amount that encompasses the transaction, Before and After hooks.
      - This ensures predictable resource usage when deployed.
    - Hook security:
      - Go's exec.Command passes no shell context. The hook binary receives raw bytes on stdin. No injection possible at the transport level.
      - If a hook author does something reckless like eval `cat /dev/stdin`, that's on them. But the MCP server itself isn't creating the vulnerability. We need to properly document this for users.
  - Default hook timeout in seconds.
- MCP reads connection string through environment variable `GOPGMCP_PG_CONNSTRING`.
  - It's postgresql connection string  - so whether it's sslmode, etc. - can be specified here. It has highest priority.
  - If connection string from environment is not found, server will try to read host, port, dbname from configuration file.
    - Username and password is not read from config file.
    - Username and password is then asked to the user interactively on server start.
    - This provides flexibility for users to not store username/password in config file and environment variable, providing it interactively on server start - recommended when running it locally.
- Other configuration:
  - HTTP port to listen on. No default port - must be specified in config file, server panics if not found.
  - Read-only mode. If true, only allow SELECT queries and other queries that do not modify data - starts connections in read-only mode.
    - When Read-only mode is on. Even when SET protection is off, we detect and reject any attempt to change transaction mode to write.
  - Connection pool config - max connections, min connections, idle timeout, etc - this should mirror pgxpool config options.
  - Logging config - log level, output format (json, text), output file (stdout, file path).
  - Health check endpoint - for load balancers/k8s probes.
  - Protection (each rule can be individually toggled on/off, all default on):
    - SET - disallowed.
    - DROP - disallowed.
    - TRUNCATE - disallowed.
    - DELETE - disallowed unless with WHERE clause.
    - UPDATE - disallowed unless with WHERE clause.
  - Max result length (in character length). Applied to the JSON result, if exceeded, truncate and append "...[truncated] Result is too long! Add limits in your query!".
  - Health check path. Defaults to `/health-check`.
- Authentication:
  - This MCP server is designed to be run in local or trusted environment, so no authentication for clients.
- Server CLI command:
  - All logs are printed out.
  - People can install as CLI tool and run as long as they have Golang.
- This Golang MCP library is not only startable as CLI MCP server, but also a library that can be initialized and then registered as internal Agent Loop code as tool call.
  - In toolcall mode, the PostgresMcp requires config object to be built and passed on construction.
  - PostgresMcp hooks, sanitization, etc can also be passed from the Config file.
  - The API for library mode is the tool calls. For each tool (e.g. Query, ListTables, DescribeTable) - there's a function that can be called directly.
    - Each function takes context, and input struct, and returns output struct and error.
- Use zerolog as logger.
- No graceful shutdown needed. If server is killed, close all connections immediately.

# Sample config JSON

```json
  {
    "connection": {
      "host": "localhost",
      "port": 5432,
      "dbname": "myapp"
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
      "block_set": true,
      "block_drop": true,
      "block_truncate": true,
      "require_where_on_delete": true,
      "require_where_on_update": true
    },
    "query": {
      "default_timeout_seconds": 30,
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
          "timeout_seconds": 5
        }
      ],
      "after_query": [
        {
          "pattern": ".*",
          "command": "/usr/local/bin/redact-pii.sh",
          "timeout_seconds": 10
        }
      ]
    },
  }
```
