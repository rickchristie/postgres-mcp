package pgmcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// SQL queries for DescribeTable

const detectTypeSQL = `
SELECT c.relkind
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1::regclass;
`

const columnsSQL = `
SELECT
    c.column_name AS name,
    c.data_type AS type,
    CASE c.is_nullable WHEN 'YES' THEN true ELSE false END AS nullable,
    COALESCE(c.column_default, '') AS default_val,
    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
FROM information_schema.columns c
LEFT JOIN (
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2
) pk ON pk.column_name = c.column_name
WHERE c.table_schema = $1
    AND c.table_name = $2
ORDER BY c.ordinal_position;
`

// For materialized views (not in information_schema.columns)
const matviewColumnsSQL = `
SELECT a.attname AS name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
       NOT a.attnotnull AS nullable,
       COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '') AS default_val
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
WHERE a.attrelid = $1::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
`

const viewDefSQL = `
SELECT pg_catalog.pg_get_viewdef($1::regclass, true) AS definition;
`

const indexesSQL = `
SELECT
    indexname AS name,
    indexdef AS definition,
    i.indisunique AS is_unique,
    i.indisprimary AS is_primary
FROM pg_catalog.pg_indexes pi
JOIN pg_catalog.pg_class c ON c.relname = pi.indexname AND c.relnamespace = (
    SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = pi.schemaname
)
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE pi.schemaname = $1
  AND pi.tablename = $2
ORDER BY pi.indexname;
`

const constraintsSQL = `
SELECT
    con.conname AS name,
    CASE con.contype
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'f' THEN 'FOREIGN KEY'
        WHEN 'u' THEN 'UNIQUE'
        WHEN 'c' THEN 'CHECK'
        WHEN 'x' THEN 'EXCLUSION'
    END AS type,
    pg_catalog.pg_get_constraintdef(con.oid, true) AS definition
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relname = $2
ORDER BY con.conname;
`

const foreignKeysSQL = `
SELECT
    con.conname AS name,
    (
        SELECT string_agg(a.attname, ', ' ORDER BY array_position(con.conkey, a.attnum))
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
    ) AS columns,
    fc.relname AS referenced_table,
    (
        SELECT string_agg(a.attname, ', ' ORDER BY array_position(con.confkey, a.attnum))
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = con.confrelid AND a.attnum = ANY(con.confkey)
    ) AS referenced_columns,
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_update,
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_delete
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class fc ON fc.oid = con.confrelid
WHERE con.contype = 'f'
  AND n.nspname = $1
  AND c.relname = $2
ORDER BY con.conname;
`

const partitionInfoSQL = `
SELECT pg_catalog.pg_get_partkeydef(c.oid) AS partition_key,
       pt.partstrat AS strategy
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = c.oid
WHERE c.oid = $1::regclass;
`

const childPartitionsSQL = `
SELECT c.relname AS partition_name
FROM pg_catalog.pg_inherits i
JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = $1::regclass
ORDER BY c.relname;
`

const parentTableSQL = `
SELECT pc.relname AS parent_table,
       pn.nspname AS parent_schema
FROM pg_catalog.pg_inherits i
JOIN pg_catalog.pg_class pc ON pc.oid = i.inhparent
JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
WHERE i.inhrelid = $1::regclass;
`

// DescribeTable returns detailed schema information about a table, view, or materialized view.
// Does NOT go through the hook/protection/sanitization pipeline.
func (p *PostgresMcp) DescribeTable(ctx context.Context, input DescribeTableInput) (*DescribeTableOutput, error) {
	startTime := time.Now()

	schema := input.Schema
	if schema == "" {
		schema = "public"
	}

	// 1. Acquire semaphore
	select {
	case p.semaphore <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("DescribeTable: failed to acquire query slot: all %d connection slots are in use, context cancelled while waiting: %w", cap(p.semaphore), ctx.Err())
	}
	defer func() { <-p.semaphore }()

	// 2. Apply configurable timeout
	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(p.config.Query.DescribeTableTimeoutSeconds)*time.Second)
	defer cancel()

	// 3. Acquire connection and execute in read-only transaction
	conn, err := p.pool.Acquire(queryCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(queryCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // always rollback — read-only metadata queries

	// Construct properly-quoted identifier for $1::regclass parameters
	qualName := quoteIdent(schema) + "." + quoteIdent(input.Table)

	output := &DescribeTableOutput{
		Schema: schema,
		Name:   input.Table,
	}

	// 4. Detect object type
	var relkind string
	err = tx.QueryRow(queryCtx, detectTypeSQL, qualName).Scan(&relkind)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s.%s: %w", schema, input.Table, err)
	}

	switch relkind {
	case "r":
		output.Type = "table"
	case "v":
		output.Type = "view"
	case "m":
		output.Type = "materialized_view"
	case "f":
		output.Type = "foreign_table"
	case "p":
		output.Type = "partitioned_table"
	default:
		output.Type = "unknown"
	}

	// 5. Fetch columns
	if relkind == "m" {
		// Materialized views use pg_attribute (not in information_schema.columns)
		if err := p.fetchMatviewColumns(queryCtx, tx, qualName, output); err != nil {
			return nil, err
		}
	} else {
		if err := p.fetchColumns(queryCtx, tx, schema, input.Table, output); err != nil {
			return nil, err
		}
	}

	// 6. Fetch view definition (views and materialized views)
	if relkind == "v" || relkind == "m" {
		var def string
		err = tx.QueryRow(queryCtx, viewDefSQL, qualName).Scan(&def)
		if err != nil && err != pgx.ErrNoRows {
			return nil, fmt.Errorf("failed to fetch view definition: %w", err)
		}
		output.Definition = def
	}

	// 7. Fetch indexes (tables, partitioned tables, materialized views — views don't have indexes)
	if relkind == "r" || relkind == "p" || relkind == "m" {
		if err := p.fetchIndexes(queryCtx, tx, schema, input.Table, output); err != nil {
			return nil, err
		}
	}

	// 8. Fetch constraints (tables and partitioned tables)
	if relkind == "r" || relkind == "p" {
		if err := p.fetchConstraints(queryCtx, tx, schema, input.Table, output); err != nil {
			return nil, err
		}
	}

	// 9. Fetch foreign keys (tables and partitioned tables)
	if relkind == "r" || relkind == "p" {
		if err := p.fetchForeignKeys(queryCtx, tx, schema, input.Table, output); err != nil {
			return nil, err
		}
	}

	// 10. Fetch partition info (partitioned tables)
	if relkind == "p" {
		if err := p.fetchPartitionInfo(queryCtx, tx, qualName, output); err != nil {
			return nil, err
		}
	}

	// 11. Check if this is a child partition (regular table inheriting from a partitioned table)
	if relkind == "r" {
		if err := p.fetchParentTable(queryCtx, tx, qualName, output); err != nil {
			return nil, err
		}
	}

	// Ensure non-nil slices for JSON serialization
	if output.Columns == nil {
		output.Columns = []ColumnInfo{}
	}
	if output.Indexes == nil {
		output.Indexes = []IndexInfo{}
	}
	if output.Constraints == nil {
		output.Constraints = []ConstraintInfo{}
	}
	if output.ForeignKeys == nil {
		output.ForeignKeys = []ForeignKeyInfo{}
	}

	p.logger.Info().
		Str("schema", schema).
		Str("table", input.Table).
		Dur("duration", time.Since(startTime)).
		Str("type", output.Type).
		Int("column_count", len(output.Columns)).
		Msg("DescribeTable executed")

	return output, nil
}

func (p *PostgresMcp) fetchColumns(ctx context.Context, tx pgx.Tx, schema, table string, output *DescribeTableOutput) error {
	rows, err := tx.Query(ctx, columnsSQL, schema, table)
	if err != nil {
		return fmt.Errorf("failed to fetch columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.Type, &col.Nullable, &col.Default, &col.IsPrimaryKey); err != nil {
			return fmt.Errorf("failed to scan column: %w", err)
		}
		output.Columns = append(output.Columns, col)
	}
	return rows.Err()
}

func (p *PostgresMcp) fetchMatviewColumns(ctx context.Context, tx pgx.Tx, qualName string, output *DescribeTableOutput) error {
	rows, err := tx.Query(ctx, matviewColumnsSQL, qualName)
	if err != nil {
		return fmt.Errorf("failed to fetch materialized view columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.Type, &col.Nullable, &col.Default); err != nil {
			return fmt.Errorf("failed to scan materialized view column: %w", err)
		}
		// Materialized views don't have primary keys
		output.Columns = append(output.Columns, col)
	}
	return rows.Err()
}

func (p *PostgresMcp) fetchIndexes(ctx context.Context, tx pgx.Tx, schema, table string, output *DescribeTableOutput) error {
	rows, err := tx.Query(ctx, indexesSQL, schema, table)
	if err != nil {
		return fmt.Errorf("failed to fetch indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx IndexInfo
		if err := rows.Scan(&idx.Name, &idx.Definition, &idx.IsUnique, &idx.IsPrimary); err != nil {
			return fmt.Errorf("failed to scan index: %w", err)
		}
		output.Indexes = append(output.Indexes, idx)
	}
	return rows.Err()
}

func (p *PostgresMcp) fetchConstraints(ctx context.Context, tx pgx.Tx, schema, table string, output *DescribeTableOutput) error {
	rows, err := tx.Query(ctx, constraintsSQL, schema, table)
	if err != nil {
		return fmt.Errorf("failed to fetch constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var con ConstraintInfo
		if err := rows.Scan(&con.Name, &con.Type, &con.Definition); err != nil {
			return fmt.Errorf("failed to scan constraint: %w", err)
		}
		output.Constraints = append(output.Constraints, con)
	}
	return rows.Err()
}

func (p *PostgresMcp) fetchForeignKeys(ctx context.Context, tx pgx.Tx, schema, table string, output *DescribeTableOutput) error {
	rows, err := tx.Query(ctx, foreignKeysSQL, schema, table)
	if err != nil {
		return fmt.Errorf("failed to fetch foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fk ForeignKeyInfo
		if err := rows.Scan(&fk.Name, &fk.Columns, &fk.ReferencedTable, &fk.ReferencedColumns, &fk.OnUpdate, &fk.OnDelete); err != nil {
			return fmt.Errorf("failed to scan foreign key: %w", err)
		}
		output.ForeignKeys = append(output.ForeignKeys, fk)
	}
	return rows.Err()
}

func (p *PostgresMcp) fetchPartitionInfo(ctx context.Context, tx pgx.Tx, qualName string, output *DescribeTableOutput) error {
	var partKey, strategy string
	err := tx.QueryRow(ctx, partitionInfoSQL, qualName).Scan(&partKey, &strategy)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return fmt.Errorf("failed to fetch partition info: %w", err)
	}

	// Map strategy code to human-readable string
	switch strategy {
	case "h":
		strategy = "hash"
	case "l":
		strategy = "list"
	case "r":
		strategy = "range"
	}

	output.Partition = &PartitionInfo{
		Strategy:     strategy,
		PartitionKey: partKey,
	}

	// Fetch child partitions
	rows, err := tx.Query(ctx, childPartitionsSQL, qualName)
	if err != nil {
		return fmt.Errorf("failed to fetch child partitions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan child partition: %w", err)
		}
		output.Partition.Partitions = append(output.Partition.Partitions, name)
	}
	return rows.Err()
}

func (p *PostgresMcp) fetchParentTable(ctx context.Context, tx pgx.Tx, qualName string, output *DescribeTableOutput) error {
	var parentTable, parentSchema string
	err := tx.QueryRow(ctx, parentTableSQL, qualName).Scan(&parentTable, &parentSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil // not a child partition
		}
		return fmt.Errorf("failed to fetch parent table: %w", err)
	}

	if output.Partition == nil {
		output.Partition = &PartitionInfo{}
	}
	if parentSchema != "" && parentSchema != "public" {
		output.Partition.ParentTable = parentSchema + "." + parentTable
	} else {
		output.Partition.ParentTable = parentTable
	}
	return nil
}

// quoteIdent escapes a SQL identifier for safe use in $1::regclass.
// Doubles embedded double-quotes and wraps in double-quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
