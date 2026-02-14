# Test Coverage Gaps

Features/behaviors advertised in README.md that lack test coverage.
Audited against all test files in the repository on 2026-02-14.

## Sections with ZERO gaps

- **Protection Rules** (23 togglable rules + always-blocked) -- fully covered
- **Type Handling** (35+ PostgreSQL types) -- fully covered
- **Query Settings** (timeouts, max_sql_length, max_result_length, defaults, panics) -- fully covered
- **Result Truncation** (truncation marker, UTF-8 safety, max_sql_length pre-check) -- fully covered
- **Concurrency** (semaphore, race safety, stress tests) -- fully covered
- **MCP Tool Registration** (RegisterMCPTools, tools/list, all 3 tools via JSON-RPC) -- fully covered
- **Error Prompts** (regex matching, multiple concat, newline separators) -- fully covered
- **Timezone** (IANA name, SET on every connection, empty = server default) -- fully covered
- **describe_table definition field** -- fully covered (added `TestDescribeTable_DefinitionEmptyForNonViews`)
- **Command hooks: AfterQuery timeout rollback** -- fully covered (added `TestQuery_CmdAfterHookTimeoutRollbacksInsert`)
- **Command hooks: AfterQuery reject rollback UPDATE/DELETE** -- fully covered (added `TestQuery_CmdAfterHookRejectRollbacksUpdate`, `TestQuery_CmdAfterHookRejectRollbacksDelete`)
- **Command hooks: BeforeQuery multi-hook chaining** -- fully covered (added `TestQuery_CmdBeforeHookChaining`)
- **Command hooks: AfterQuery multi-hook chaining** -- fully covered (added `TestQuery_CmdAfterHookChaining`)
- **Read-only mode: SET transaction_read_only** -- fully covered (added `TestQuery_ReadOnlyBlocksSetTransactionReadOnly`)
- **JSONB sanitization with real database** -- fully covered (added `TestQuery_SanitizationJSONBIntegration`)
- **pool.min_conns applied to connection pool** -- fully covered (added `TestLoadConfig_MinConnsApplied`, `TestLoadConfig_MinConnsZeroDefault`)
- **server.port panic on <= 0** -- fully covered (added `TestRunServe_PanicsOnZeroPort`, `TestRunServe_PanicsOnNegativePort`)
- **Query logging (SQL, duration, row_count, optional pipeline fields)** -- fully covered (added `TestQuery_LogsExecutionDetails`, `TestQuery_LogsOptionalPipelineFields`)
- **ListTables logging (duration, table_count)** -- fully covered (added `TestListTables_LogsExecution`)
- **DescribeTable logging (schema, table, duration, type, column_count)** -- fully covered (added `TestDescribeTable_LogsExecution`)
- **list_tables bypasses hook/protection/sanitization pipeline** -- fully covered (added `TestListTables_BypassesHookProtectionSanitizationPipeline`)
- **describe_table bypasses hook/protection/sanitization pipeline** -- fully covered (added `TestDescribeTable_BypassesHookProtectionSanitizationPipeline`)

---

## Remaining gaps (not fixed)

None — all README-advertised behaviors now have test coverage.
