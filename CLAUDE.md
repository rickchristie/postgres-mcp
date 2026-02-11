<testing>
- When running tests, if database does not exist, ask the user to run `pgflock up`.
- Integration tests (need a real database) use `//go:build integration` tag, run with `go test -tags integration ./...`
- pgflock helpers are in `testhelpers_test.go` — use them, don't reinvent:
  - `acquireTestDB(t)` → connStr. Locks a database via pgflock, auto-unlocks via `t.Cleanup`.
  - `newTestInstance(t, config)` → `(*pgmcp.PostgresMcp, connStr)`. Acquires DB + creates instance, auto-cleans up both.
  - `newTestInstanceWithHooks(t, config, hooks)` → `*pgmcp.PostgresMcp`. Same but with server hooks.
  - `defaultConfig()` → `pgmcp.Config` with safe defaults (5 pool conns, 30s query timeout, 10s list/describe, 100000 max SQL/result).
  - `setupTable(t, p, sql)` — runs DDL/DML, fails test on error.
  - `testLogger()` → disabled zerolog logger.
- Always add `t.Parallel()` at the start of every test function.
- Standard integration test pattern:
  ```go
  func TestFoo(t *testing.T) {
      config := defaultConfig()
      config.Protection.AllowDDL = true // enable what you need
      p, _ := newTestInstance(t, config)
      setupTable(t, p, "CREATE TABLE ...")
      output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT ..."})
      // assertions on output
  }
  ```
- For raw pgxpool access (e.g., type verification): use `acquireTestDB(t)` directly, create your own pool.
- Unit tests (no DB) omit the build tag entirely.
</testing>

<critical_rules>
- For every feature/behavior created, always create tests that assert that behavior.
- Tests must assert entire fields, not just parts of them.
- Always re-run all tests after making any change to the codebase, fix any regressions.
</critical_rules>