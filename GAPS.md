# Gap Analysis: IMPLEMENTATION.md vs Actual Code

Comprehensive comparison of the specification against the implemented code. Organized by category with references to IMPLEMENTATION.md sections.

---

## 1. Code / Feature Gaps

### 1.1 `internal/configure` package does not exist
- **Spec**: Phase 2.6 (line 1047-1069) defines `internal/configure/configure.go` with `Run(configPath string) error` for an interactive config wizard.
- **Actual**: `cmd/gopgmcp/configure.go` is a stub that prints "not yet implemented". The `internal/configure/` package does not exist.
- **Severity**: Medium — entire feature missing.

### 1.2 MCP initialize lifecycle logging not implemented
- **Spec**: Phase 5.2 (lines 2289-2296) specifies `mcpServer.OnInitialize()` to log AI agent connections with client name/version.
- **Actual**: `cmd/gopgmcp/serve.go` does not call `OnInitialize` or log MCP initialize requests.
- **Severity**: Low — logging/observability feature.

### 1.3 Server.Port validation missing in serve.go
- **Spec**: Section 6.6 (line 2963) specifies `TestLoadConfigValidation_NoPort` — server.port must be > 0, server should panic if missing.
- **Actual**: `serve.go` uses `fmt.Sprintf(":%d", serverConfig.Server.Port)` without validating port > 0. Zero port would bind to a random port instead of panicking.
- **Severity**: Low — config validation gap.

### 1.4 Spec bug: `TestSQLInjection_Stacked` expects wrong statement count
- **Spec**: Section 6.1 (line 2858) says `SELECT 1; DELETE FROM users; --` should produce "found 3 statements".
- **Actual**: pg_query parses this as 2 statements (trailing `; --` is a comment). Actual test correctly asserts "found 2 statements".
- **Action**: Fix the spec, not the code.

---

## 2. Missing Config Tests (Section 6.6, lines 2953-2984)

**None of the `TestLoadConfig*` tests exist.** This is the largest gap — 27 tests covering config loading, validation, defaults, and edge cases.

| # | Missing Test | Description |
|---|---|---|
| 1 | `TestLoadConfigValid` | Valid JSON config file parsed correctly |
| 2 | `TestLoadConfigFromEnvPath` | `GOPGMCP_CONFIG_PATH` env var path used |
| 3 | `TestLoadConfigMissing` | Missing config file returns error |
| 4 | `TestLoadConfigInvalidJSON` | Malformed JSON returns error |
| 5 | `TestLoadConfigInvalidRegex` | Invalid regex in rules panics |
| 6 | `TestLoadConfigDefaults_MaxResultLength` | max_result_length defaults to 100000 |
| 7 | `TestLoadConfigValidation_NoPort` | Missing server.port panics |
| 8 | `TestLoadConfigValidation_ZeroMaxConns` | pool.max_conns=0 panics |
| 9 | `TestLoadConfigValidation_ZeroDefaultTimeout` | default_timeout_seconds=0 panics |
| 10 | `TestLoadConfigValidation_MissingDefaultTimeout` | Omitted default_timeout_seconds panics |
| 11 | `TestLoadConfigValidation_ZeroListTablesTimeout` | list_tables_timeout_seconds=0 panics |
| 12 | `TestLoadConfigValidation_ZeroDescribeTableTimeout` | describe_table_timeout_seconds=0 panics |
| 13 | `TestLoadConfigValidation_NegativeTimeout` | Negative timeout panics |
| 14 | `TestLoadConfigValidation_ZeroHookDefaultTimeout` | Hooks with default_hook_timeout=0 panics |
| 15 | `TestLoadConfigValidation_MissingHookDefaultTimeout` | Hooks with omitted default_hook_timeout panics |
| 16 | `TestLoadConfigValidation_HookDefaultTimeoutNotRequiredWithoutHooks` | No hooks, omitted timeout — no panic |
| 17 | `TestLoadConfigValidation_HookTimeoutFallback` | Per-hook timeout=0 falls back to default |
| 18 | `TestLoadConfigValidation_HealthCheckPathEmpty` | health_check_enabled=true with empty path panics |
| 19 | `TestLoadConfigValidation_HealthCheckPathNotRequiredWhenDisabled` | health_check_enabled=false, empty path — no panic |
| 20 | `TestLoadConfigDefaults_MaxSQLLength` | max_sql_length defaults to 100000 |
| 21 | `TestLoadConfigProtectionDefaults` | All Allow* fields default to false |
| 22 | `TestLoadConfigProtectionExplicitAllow` | allow_drop=true sets AllowDrop=true, others false |
| 23 | `TestLoadConfigProtectionNewFields` | All new protection fields settable |
| 24 | `TestLoadConfigSSLMode` | sslmode field parsed correctly |
| 25 | `TestLoadConfigValidation_GoHooksAndCmdHooksMutuallyExclusive` | Both Go and cmd hooks panics |
| 26 | `TestLoadConfigValidation_GoHooksRequireDefaultTimeout` | Go hooks with timeout=0 panics |
| 27 | `TestLoadConfigValidation_GoHooksOnlyNoCmd` | Only Go hooks, no cmd — no panic |

---

## 3. Missing Hook Tests

### 3.1 Command Hook Tests (Section 6.3, lines 2880-2909)

| # | Missing Test | Description | File |
|---|---|---|---|
| 1 | `TestHookStdinInput` | Verify raw SQL passed as stdin to BeforeQuery hook | `internal/hooks/hooks_test.go` |

### 3.2 Go Hook Unit Tests (Section 6.3.1, lines 2911-2931)

The spec defines these as **unit tests** (no database, calling `runGoBeforeHooks`/`runGoAfterHooks` directly). The actual `query_gohooks_test.go` implements them as **integration tests** through the full `Query()` pipeline. The following spec'd unit tests have no equivalent:

| # | Missing Test | Description | File |
|---|---|---|---|
| 1 | `TestGoBeforeHooks_Chaining` | Two hooks: first modifies, second receives modified | `query_gohooks_test.go` |
| 2 | `TestGoBeforeHooks_ChainStopsOnReject` | First hook rejects, second never called | `query_gohooks_test.go` |
| 3 | `TestGoBeforeHooks_PerHookTimeoutOverridesDefault` | entry.Timeout=3s overrides default=1s, hook sleeps 2s | `query_gohooks_test.go` |
| 4 | `TestGoBeforeHooks_Empty` | No hooks configured, query passes through | `query_gohooks_test.go` |
| 5 | `TestGoAfterHooks_Chaining` | Two hooks: first modifies, second receives modified | `query_gohooks_test.go` |
| 6 | `TestGoAfterHooks_Empty` | No hooks configured, result passes through | `query_gohooks_test.go` |
| 7 | `TestGoAfterHooks_PreservesTypes` | Hook type-asserts int64/string, confirms no serialization | `query_gohooks_test.go` |

---

## 4. Missing Integration Tests — Query Tool (Section 6.7, lines 2987-3058)

| # | Missing Test | Description |
|---|---|---|
| 1 | `TestQuery_JSONBReturnType` | JSONB returned as parsed Go map/slice, not string |
| 2 | `TestQuery_JSONBNumericPrecision` | Large integers in JSONB lose precision (known limitation) |
| 3 | `TestQuery_SelectNestedSubquery` | Query with nested subqueries |
| 4 | `TestQuery_Transaction` | Verify query runs in transaction (data consistency) |
| 5 | `TestQuery_HooksEndToEnd` | Config with real hook scripts, hook executed |
| 6 | `TestQuery_HookTimeoutStopsPipeline` | slow.sh with timeout=1s stops pipeline |
| 7 | `TestQuery_TimezoneEmpty` | timezone="" returns server default |
| 8 | `TestQuery_TimezoneWithReadOnly` | Both timezone and read_only applied via AfterConnect |
| 9 | `TestQuery_NumericPrecisionWithHooks` | Bigint survives JSON round-trip through cmd hooks |
| 10 | `TestQuery_NumericPrecisionWithoutHooks` | Bigint preserved without hooks (no round-trip) |
| 11 | `TestQuery_RowsAffected_Select` | SELECT returns row count in RowsAffected |
| 12 | `TestQuery_RowsAffected_InsertReturning` | INSERT RETURNING has RowsAffected=1 and rows |
| 13 | `TestQuery_CidrColumn` | cidr value returned as string |
| 14 | `TestQuery_AfterHookRejectSelectNoSideEffect` | Hook rejects SELECT, no side effect |
| 15 | `TestQuery_ReadOnlyStatementRollbacksBeforeHooks` | SELECT triggers rollback before hooks |
| 16 | `TestQuery_MaxSQLLength_ExactLimit` | Query under max_sql_length limit succeeds |
| 17 | `TestQuery_CreateExtensionBlocked` | CREATE EXTENSION blocked by default |
| 18 | `TestQuery_MaintenanceBlocked` | ANALYZE blocked by AllowMaintenance |
| 19 | `TestQuery_CreateTriggerBlocked` | CREATE TRIGGER blocked by default |
| 20 | `TestQuery_CreateRuleBlocked` | CREATE RULE blocked by default |
| 21 | `TestQuery_CommitBlocked` | COMMIT blocked (transaction control) |
| 22 | `TestQuery_AlterExtensionBlocked` | ALTER EXTENSION blocked by default |

---

## 5. Missing Integration Tests — Go Hook Pipeline (Section 6.7, lines 3059-3084)

| # | Missing Test | Description |
|---|---|---|
| 1 | `TestQuery_GoBeforeHook_Chaining` | Two BeforeQueryHooks applied in sequence |
| 2 | `TestQuery_GoBeforeHook_PerHookTimeout` | entry.Timeout=2s overrides default=1s, hook sleeps 1.5s |
| 3 | `TestQuery_GoAfterHook_Chaining` | Two AfterQueryHooks applied in sequence |
| 4 | `TestQuery_GoAfterHook_ReceivesNativeTypes` | Hook receives int64/string, confirms no serialization |
| 5 | `TestQuery_GoAfterHook_SelectRollbacksBeforeHooks` | SELECT rollback before hooks, hooks still run |

---

## 6. Missing Integration Tests — ListTables (Section 6.7, lines 3270-3284)

| # | Missing Test | Description |
|---|---|---|
| 1 | `TestListTables_SchemaAccessLimited` | Table with SELECT but no USAGE on schema, SchemaAccessLimited=true |
| 2 | `TestListTables_SchemaAccessNormal` | Table in public schema, SchemaAccessLimited=false |
| 3 | `TestListTables_Timeout` | Config with list_tables_timeout=1s, error contains deadline exceeded |
| 4 | `TestListTables_AcquiresSemaphore` | max_conns=1, ListTables blocks on held semaphore |
| 5 | `TestListTables_SemaphoreContention` | max_conns=1, short context timeout, error contains "failed to acquire query slot" |

---

## 7. Missing Integration Tests — DescribeTable (Section 6.7, lines 3286-3310)

| # | Missing Test | Description |
|---|---|---|
| 1 | `TestDescribeTable_ForeignTable` | Foreign table support (type="foreign_table", columns listed) |
| 2 | `TestDescribeTable_PartitionedTableList` | LIST partitioning, Partition.Strategy="list" |
| 3 | `TestDescribeTable_PartitionedTableHash` | HASH partitioning, Partition.Strategy="hash" |
| 4 | `TestDescribeTable_Timeout` | Config with describe_table_timeout=1s, error contains deadline exceeded |
| 5 | `TestDescribeTable_AcquiresSemaphore` | max_conns=1, DescribeTable blocks on held semaphore |
| 6 | `TestDescribeTable_SemaphoreContention` | max_conns=1, short context timeout, error contains "failed to acquire query slot" |

---

## 8. Missing Full Pipeline Test (Section 6.7, lines 3312-3316)

| # | Missing Test | Description |
|---|---|---|
| 1 | `TestFullPipeline` | End-to-end: BeforeQuery hook (modify) + protection + query + AfterQuery hook + sanitization + error prompts, all applied in correct order |

---

## 9. No-Gap Areas (Fully Implemented)

The following areas have **zero gaps** between spec and implementation:

| Area | Spec Reference |
|---|---|
| Protection checker (`internal/protection/`) | Phase 2.1, Section 6.1 |
| Protection tests (all 200+ cases) | Section 6.1 |
| Sanitize package (`internal/sanitize/`) | Phase 2.3, Section 6.2 |
| Error prompt package (`internal/errprompt/`) | Phase 2.4, Section 6.4 |
| Timeout package (`internal/timeout/`) | Phase 2.5, Section 6.5 |
| Command hook runner (`internal/hooks/`) | Phase 2.2 |
| Command hook test scripts (`testdata/hooks/`) | Phase 6 test data |
| Query pipeline code (`query.go`) | Phase 3.2 |
| `convertValue` — all 24 type cases | Phase 3.2 |
| Config structs (`config.go`, `types.go`) | Phase 1.2 |
| PostgresMcp struct, New(), Close() (`pgmcp.go`) | Phase 3.1 |
| MCP tool registration (`mcp.go`) | Phase 4.1 |
| MCP server tests (`mcpserver_test.go`) | Section 6.7 MCP tests |
| Stress tests (`stress_test.go`) | Section 6.8 |
| Race tests (`race_test.go`) | Section 6.9 |
| ListTables code (`listtables.go`) | Phase 3.3 |
| DescribeTable code (`describetable.go`) | Phase 3.4 |
| CLI main.go/serve.go (core logic) | Phase 5.1-5.2 |

---

## Summary

| Category | Missing Items |
|---|---|
| Code/feature gaps | 3 (configure package, initialize logging, port validation) |
| Config tests | 27 |
| Hook tests (command) | 1 |
| Hook tests (Go unit) | 7 |
| Integration tests — Query | 22 |
| Integration tests — Go hooks | 5 |
| Integration tests — ListTables | 5 |
| Integration tests — DescribeTable | 6 |
| Full pipeline test | 1 |
| **Total** | **77 items** |

All production code (protection, hooks, sanitize, errprompt, timeout, query pipeline, convertValue, ListTables, DescribeTable, MCP bridge, config structs) matches the spec with zero functional gaps. The gaps are almost entirely in **test coverage** and the **unimplemented configure feature**.
