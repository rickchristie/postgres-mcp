// Package pgmcp provides safe, controlled PostgreSQL access for AI agents
// through the Model Context Protocol (MCP).
//
// It exposes three tools — Query, ListTables, and DescribeTable — with
// a full execution pipeline: SQL protection, query hooks, data sanitization,
// result truncation, and dynamic agent steering via error prompts.
//
// SQL injection is prevented at the protocol level using pgx extended query
// protocol (QueryExecModeExec). On top of that, 23 AST-based protection rules
// (all blocked by default) validate queries using PostgreSQL's actual C parser
// via pg_query.
//
// # Library Usage
//
//	p, err := pgmcp.New(ctx, connString, pgmcp.Config{
//		Pool:     pgmcp.PoolConfig{MaxConns: 10},
//		ReadOnly: true,
//		Query: pgmcp.QueryConfig{
//			DefaultTimeoutSeconds:       30,
//			ListTablesTimeoutSeconds:    10,
//			DescribeTableTimeoutSeconds: 10,
//		},
//	}, logger)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer p.Close(ctx)
//
//	// Use directly
//	output := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT * FROM users LIMIT 10"})
//
//	// Or register as MCP tools
//	pgmcp.RegisterMCPTools(mcpServer, p)
//
// # Hooks
//
// BeforeQuery and AfterQuery hooks run as a middleware chain around query
// execution. Implement [BeforeQueryHook] and [AfterQueryHook] for native Go
// hooks with full type safety:
//
//	type AuditHook struct{}
//
//	func (h *AuditHook) Run(ctx context.Context, query string) (string, error) {
//		log.Printf("query: %s", query)
//		return query, nil // return modified query or original
//	}
//
// Unlike command-based hooks (server mode), Go hooks have no regex pattern
// matching — the hook function itself decides whether to act.
//
// AfterQuery hooks run before transaction commit for write queries, enabling
// guardrails like rolling back if too many rows are affected. AfterQuery hooks
// do not run for read-only queries (SELECT, EXPLAIN) — those are rolled back
// immediately after collecting results.
//
// For full documentation, configuration reference, and examples, see:
// https://github.com/rickchristie/postgres-mcp
package pgmcp
