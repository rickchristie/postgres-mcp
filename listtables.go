package pgmcp

import (
	"context"
	"fmt"
	"time"
)

const listTablesSQL = `
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
`

// ListTables returns all tables, views, materialized views, and foreign tables
// accessible to the current user. Does NOT go through the hook/protection/sanitization pipeline.
func (p *PostgresMcp) ListTables(ctx context.Context, input ListTablesInput) (*ListTablesOutput, error) {
	startTime := time.Now()

	// 1. Acquire semaphore
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
	if err != nil {
		return nil, fmt.Errorf("ListTables query failed: %w", err)
	}
	defer rows.Close()

	var tables []TableEntry
	for rows.Next() {
		var entry TableEntry
		if err := rows.Scan(&entry.Schema, &entry.Name, &entry.Type, &entry.Owner, &entry.SchemaAccessLimited); err != nil {
			return nil, fmt.Errorf("ListTables scan failed: %w", err)
		}
		tables = append(tables, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListTables rows error: %w", err)
	}

	if tables == nil {
		tables = []TableEntry{}
	}

	p.logger.Info().
		Dur("duration", time.Since(startTime)).
		Int("table_count", len(tables)).
		Msg("ListTables executed")

	return &ListTablesOutput{Tables: tables}, nil
}
