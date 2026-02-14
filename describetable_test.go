package pgmcp_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	pgmcp "github.com/rickchristie/postgres-mcp"
)

func TestDescribeTable_Columns(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name varchar(100) NOT NULL, email text, age integer DEFAULT 0)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "table" {
		t.Fatalf("expected type 'table', got %q", output.Type)
	}
	if len(output.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(output.Columns))
	}

	// Verify column details
	for _, col := range output.Columns {
		switch col.Name {
		case "id":
			if !col.IsPrimaryKey {
				t.Error("expected id to be primary key")
			}
			if col.Type != "integer" {
				t.Errorf("expected id type 'integer', got %q", col.Type)
			}
		case "name":
			if col.Nullable {
				t.Error("expected name to be NOT NULL")
			}
			if !strings.Contains(col.Type, "character varying") {
				t.Errorf("expected name type to contain 'character varying', got %q", col.Type)
			}
		case "email":
			if col.Type != "text" {
				t.Errorf("expected email type 'text', got %q", col.Type)
			}
		case "age":
			if col.Default == "" {
				t.Error("expected age to have a default")
			}
			if col.Type != "integer" {
				t.Errorf("expected age type 'integer', got %q", col.Type)
			}
		}
	}
}

func TestDescribeTable_PrimaryKey(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE pk_table (id serial PRIMARY KEY, name text)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "pk_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// PK column should be marked
	for _, col := range output.Columns {
		if col.Name == "id" && !col.IsPrimaryKey {
			t.Error("expected id to be primary key")
		}
	}

	// PK constraint should be listed
	foundPK := false
	for _, con := range output.Constraints {
		if con.Type == "PRIMARY KEY" {
			foundPK = true
			if con.Name == "" {
				t.Error("expected PRIMARY KEY constraint name to be non-empty")
			}
			if con.Definition == "" {
				t.Error("expected PRIMARY KEY constraint definition to be non-empty")
			}
			break
		}
	}
	if !foundPK {
		t.Error("expected PRIMARY KEY constraint in list")
	}
}

func TestDescribeTable_Indexes(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE idx_table (id serial PRIMARY KEY, email text)")
	setupTable(t, p, "CREATE INDEX idx_email ON idx_table (email)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "idx_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	foundPKIndex := false
	for _, idx := range output.Indexes {
		if idx.Name == "idx_email" {
			found = true
			if idx.IsUnique {
				t.Error("expected non-unique index")
			}
			if idx.Definition == "" {
				t.Error("expected idx_email definition to be non-empty")
			}
			if idx.IsPrimary {
				t.Error("expected idx_email to not be primary")
			}
		}
		if idx.IsPrimary {
			foundPKIndex = true
		}
	}
	if !found {
		t.Error("expected idx_email in indexes")
	}
	if !foundPKIndex {
		t.Error("expected a primary key index in indexes")
	}
}

func TestDescribeTable_ForeignKeys(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE authors (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE TABLE books (id serial PRIMARY KEY, author_id integer REFERENCES authors(id) ON DELETE CASCADE)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "books"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(output.ForeignKeys) != 1 {
		t.Fatalf("expected 1 foreign key, got %d", len(output.ForeignKeys))
	}
	fk := output.ForeignKeys[0]
	if fk.ReferencedTable != "authors" {
		t.Fatalf("expected referenced table 'authors', got %q", fk.ReferencedTable)
	}
	if fk.OnDelete != "CASCADE" {
		t.Fatalf("expected ON DELETE CASCADE, got %q", fk.OnDelete)
	}
	if fk.Columns != "author_id" {
		t.Fatalf("expected fk columns 'author_id', got %q", fk.Columns)
	}
	if fk.ReferencedColumns != "id" {
		t.Fatalf("expected fk referenced columns 'id', got %q", fk.ReferencedColumns)
	}
	if fk.OnUpdate != "NO ACTION" {
		t.Fatalf("expected ON UPDATE 'NO ACTION', got %q", fk.OnUpdate)
	}
}

func TestDescribeTable_UniqueConstraint(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE uniq_table (id serial PRIMARY KEY, email text UNIQUE)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "uniq_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundUnique := false
	for _, con := range output.Constraints {
		if con.Type == "UNIQUE" {
			foundUnique = true
			if con.Name == "" {
				t.Error("expected UNIQUE constraint name to be non-empty")
			}
			if con.Definition == "" {
				t.Error("expected UNIQUE constraint definition to be non-empty")
			}
			break
		}
	}
	if !foundUnique {
		t.Error("expected UNIQUE constraint in list")
	}
}

func TestDescribeTable_CheckConstraint(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE check_table (id serial PRIMARY KEY, age integer CHECK (age >= 0))")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "check_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundCheck := false
	for _, con := range output.Constraints {
		if con.Type == "CHECK" {
			foundCheck = true
			if con.Name == "" {
				t.Error("expected CHECK constraint name to be non-empty")
			}
			if con.Definition == "" {
				t.Error("expected CHECK constraint definition to be non-empty")
			}
			break
		}
	}
	if !foundCheck {
		t.Error("expected CHECK constraint in list")
	}
}

func TestDescribeTable_DefaultValues(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE def_table (id serial PRIMARY KEY, status text DEFAULT 'active', count integer DEFAULT 0)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "def_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, col := range output.Columns {
		if col.Name == "status" && col.Default == "" {
			t.Error("expected status to have default 'active'")
		}
		if col.Name == "count" && col.Default == "" {
			t.Error("expected count to have default 0")
		}
	}
}

func TestDescribeTable_NotFound(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	_, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "nonexistent_table"})
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
	if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected not found error, got %q", err.Error())
	}
}

func TestDescribeTable_View(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE VIEW users_view AS SELECT id, name FROM users")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "users_view"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "view" {
		t.Fatalf("expected type 'view', got %q", output.Type)
	}
	if output.Definition == "" {
		t.Error("expected view definition to be set")
	}
	if len(output.Columns) < 2 {
		t.Fatalf("expected at least 2 columns, got %d", len(output.Columns))
	}
	// Views don't have indexes or constraints
	if len(output.Indexes) != 0 {
		t.Fatalf("expected no indexes for view, got %d", len(output.Indexes))
	}
}

func TestDescribeTable_MaterializedView(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE MATERIALIZED VIEW users_matview AS SELECT id, name FROM users")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "users_matview"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "materialized_view" {
		t.Fatalf("expected type 'materialized_view', got %q", output.Type)
	}
	if output.Definition == "" {
		t.Error("expected matview definition to be set")
	}
	if len(output.Columns) < 2 {
		t.Fatalf("expected at least 2 columns, got %d", len(output.Columns))
	}
}

func TestDescribeTable_MaterializedViewWithIndex(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE MATERIALIZED VIEW users_matview AS SELECT id, name FROM users")
	setupTable(t, p, "CREATE UNIQUE INDEX idx_matview_id ON users_matview (id)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "users_matview"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, idx := range output.Indexes {
		if idx.Name == "idx_matview_id" {
			found = true
			if !idx.IsUnique {
				t.Error("expected unique index")
			}
			break
		}
	}
	if !found {
		t.Error("expected idx_matview_id in indexes")
	}
}

func TestDescribeTable_PartitionedTable(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE events (id serial, created_at timestamp NOT NULL) PARTITION BY RANGE (created_at)")
	setupTable(t, p, "CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")
	setupTable(t, p, "CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "events"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "partitioned_table" {
		t.Fatalf("expected type 'partitioned_table', got %q", output.Type)
	}
	if output.Partition == nil {
		t.Fatal("expected partition info")
	}
	if output.Partition.Strategy != "range" {
		t.Fatalf("expected strategy 'range', got %q", output.Partition.Strategy)
	}
	if !strings.Contains(output.Partition.PartitionKey, "created_at") {
		t.Fatalf("expected partition key to contain 'created_at', got %q", output.Partition.PartitionKey)
	}
	if len(output.Partition.Partitions) != 2 {
		t.Fatalf("expected 2 child partitions, got %d", len(output.Partition.Partitions))
	}
}

func TestDescribeTable_ChildPartition(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE events (id serial, created_at timestamp NOT NULL) PARTITION BY RANGE (created_at)")
	setupTable(t, p, "CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "events_2024"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "table" {
		t.Fatalf("expected type 'table', got %q", output.Type)
	}
	if output.Partition == nil {
		t.Fatal("expected partition info with parent")
	}
	if output.Partition.ParentTable != "events" {
		t.Fatalf("expected parent table 'events', got %q", output.Partition.ParentTable)
	}
}

func TestDescribeTable_DefaultSchemaPublic(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE my_table (id serial PRIMARY KEY)")

	// No schema specified — should default to "public"
	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "my_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Schema != "public" {
		t.Fatalf("expected schema 'public', got %q", output.Schema)
	}
}

func TestDescribeTable_SchemaQualified(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE SCHEMA custom_schema")
	setupTable(t, p, "CREATE TABLE custom_schema.my_table (id serial PRIMARY KEY, name text)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "my_table", Schema: "custom_schema"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Schema != "custom_schema" {
		t.Fatalf("expected schema 'custom_schema', got %q", output.Schema)
	}
	if output.Name != "my_table" {
		t.Fatalf("expected table 'my_table', got %q", output.Name)
	}
}

func TestDescribeTable_ForeignTable(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowCreateExtension = true
	p, _ := newTestInstance(t, config)

	// Try to create file_fdw extension — skip if not available
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE EXTENSION IF NOT EXISTS file_fdw"})
	if output.Error != "" {
		t.Skipf("file_fdw extension not available: %s", output.Error)
	}

	setupTable(t, p, "CREATE SERVER ft_test_server FOREIGN DATA WRAPPER file_fdw")
	setupTable(t, p, "CREATE FOREIGN TABLE ft_test_table (id integer, name text) SERVER ft_test_server OPTIONS (filename '/dev/null', format 'csv')")

	descOutput, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "ft_test_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if descOutput.Type != "foreign_table" {
		t.Fatalf("expected type 'foreign_table', got %q", descOutput.Type)
	}
	if len(descOutput.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(descOutput.Columns))
	}

	// Verify column names
	colNames := map[string]bool{}
	for _, col := range descOutput.Columns {
		colNames[col.Name] = true
	}
	if !colNames["id"] || !colNames["name"] {
		t.Fatalf("expected columns 'id' and 'name', got %v", descOutput.Columns)
	}
}

func TestDescribeTable_PartitionedTableList(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE regions (id serial, region text NOT NULL) PARTITION BY LIST (region)")
	setupTable(t, p, "CREATE TABLE regions_us PARTITION OF regions FOR VALUES IN ('us-east', 'us-west')")
	setupTable(t, p, "CREATE TABLE regions_eu PARTITION OF regions FOR VALUES IN ('eu-west', 'eu-central')")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "regions"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "partitioned_table" {
		t.Fatalf("expected type 'partitioned_table', got %q", output.Type)
	}
	if output.Partition == nil {
		t.Fatal("expected partition info")
	}
	if output.Partition.Strategy != "list" {
		t.Fatalf("expected strategy 'list', got %q", output.Partition.Strategy)
	}
	if !strings.Contains(output.Partition.PartitionKey, "region") {
		t.Fatalf("expected partition key to contain 'region', got %q", output.Partition.PartitionKey)
	}
	if len(output.Partition.Partitions) != 2 {
		t.Fatalf("expected 2 child partitions, got %d", len(output.Partition.Partitions))
	}
}

func TestDescribeTable_PartitionedTableHash(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE hash_data (id integer NOT NULL, val text) PARTITION BY HASH (id)")
	setupTable(t, p, "CREATE TABLE hash_data_0 PARTITION OF hash_data FOR VALUES WITH (MODULUS 2, REMAINDER 0)")
	setupTable(t, p, "CREATE TABLE hash_data_1 PARTITION OF hash_data FOR VALUES WITH (MODULUS 2, REMAINDER 1)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "hash_data"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Type != "partitioned_table" {
		t.Fatalf("expected type 'partitioned_table', got %q", output.Type)
	}
	if output.Partition == nil {
		t.Fatal("expected partition info")
	}
	if output.Partition.Strategy != "hash" {
		t.Fatalf("expected strategy 'hash', got %q", output.Partition.Strategy)
	}
	if !strings.Contains(output.Partition.PartitionKey, "id") {
		t.Fatalf("expected partition key to contain 'id', got %q", output.Partition.PartitionKey)
	}
	if len(output.Partition.Partitions) != 2 {
		t.Fatalf("expected 2 child partitions, got %d", len(output.Partition.Partitions))
	}
}

func TestDescribeTable_Timeout(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Query.DescribeTableTimeoutSeconds = 1
	p, connStr := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE dt_timeout_table (id serial PRIMARY KEY)")

	// Hold an ACCESS EXCLUSIVE lock on the table from a separate connection.
	// This blocks ::regclass resolution (which acquires AccessShareLock) in DescribeTable.
	ctx := context.Background()
	lockConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect for lock: %v", err)
	}
	t.Cleanup(func() {
		lockConn.Exec(ctx, "ROLLBACK")
		lockConn.Close(ctx)
	})

	_, err = lockConn.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("failed to begin lock transaction: %v", err)
	}
	_, err = lockConn.Exec(ctx, "LOCK TABLE dt_timeout_table IN ACCESS EXCLUSIVE MODE")
	if err != nil {
		t.Fatalf("failed to lock table: %v", err)
	}

	// DescribeTable should block on the lock and timeout after 1s
	_, descErr := p.DescribeTable(ctx, pgmcp.DescribeTableInput{Table: "dt_timeout_table"})
	if descErr == nil {
		t.Fatal("expected timeout error")
	}
	errMsg := descErr.Error()
	if !strings.Contains(errMsg, "context deadline exceeded") && !strings.Contains(errMsg, "canceling statement") {
		t.Fatalf("expected deadline exceeded or canceling statement error, got %q", errMsg)
	}
}

func TestDescribeTable_AcquiresSemaphore(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Pool.MaxConns = 1
	config.Query.DefaultTimeoutSeconds = 30
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE dt_sem_table (id serial PRIMARY KEY, name text)")

	// Hold the semaphore with a slow query (2 seconds)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(2)"})
	}()

	// Give the goroutine time to acquire the semaphore
	time.Sleep(100 * time.Millisecond)

	// DescribeTable should block until the slow query finishes, then succeed
	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "dt_sem_table"})
	if err != nil {
		t.Fatalf("expected DescribeTable to succeed after semaphore released, got error: %v", err)
	}
	if output.Type != "table" {
		t.Fatalf("expected type 'table', got %q", output.Type)
	}

	<-done
}

func TestDescribeTable_SemaphoreContention(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Pool.MaxConns = 1
	config.Query.DefaultTimeoutSeconds = 30
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE dt_contention_table (id serial PRIMARY KEY)")

	// Hold the single semaphore slot with a slow query
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(5)"})
	}()

	// Give the goroutine time to acquire the semaphore
	time.Sleep(100 * time.Millisecond)

	// DescribeTable with a short context timeout — should fail with semaphore contention
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := p.DescribeTable(ctx, pgmcp.DescribeTableInput{Table: "dt_contention_table"})
	if err == nil {
		t.Fatal("expected semaphore contention error")
	}
	if !strings.Contains(err.Error(), "failed to acquire query slot") {
		t.Fatalf("expected 'failed to acquire query slot' in error, got %q", err.Error())
	}

	<-done
}

func TestDescribeTable_ExclusionConstraint(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowCreateExtension = true
	p, _ := newTestInstance(t, config)

	// Try to create btree_gist extension — skip if not available
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE EXTENSION IF NOT EXISTS btree_gist"})
	if output.Error != "" {
		t.Skipf("btree_gist extension not available: %s", output.Error)
	}

	setupTable(t, p, "CREATE TABLE reservations (id serial PRIMARY KEY, room integer NOT NULL, during tsrange NOT NULL, EXCLUDE USING gist (room WITH =, during WITH &&))")

	descOutput, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "reservations"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundExclusion := false
	for _, con := range descOutput.Constraints {
		if con.Type == "EXCLUSION" {
			foundExclusion = true
			if con.Name == "" {
				t.Error("expected exclusion constraint name to be non-empty")
			}
			if con.Definition == "" {
				t.Error("expected exclusion constraint definition to be non-empty")
			}
			break
		}
	}
	if !foundExclusion {
		t.Error("expected EXCLUSION constraint in list")
	}
}

func TestDescribeTable_DefinitionEmptyForNonViews(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowCreateExtension = true
	p, _ := newTestInstance(t, config)

	// Regular table
	setupTable(t, p, "CREATE TABLE def_empty_table (id serial PRIMARY KEY, name text)")
	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "def_empty_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Definition != "" {
		t.Fatalf("expected empty definition for regular table, got %q", output.Definition)
	}

	// Partitioned table
	setupTable(t, p, "CREATE TABLE def_empty_part (id serial, created_at timestamp NOT NULL) PARTITION BY RANGE (created_at)")
	output, err = p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "def_empty_part"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Definition != "" {
		t.Fatalf("expected empty definition for partitioned table, got %q", output.Definition)
	}

	// Foreign table (if extension available)
	extOutput := p.Query(context.Background(), pgmcp.QueryInput{SQL: "CREATE EXTENSION IF NOT EXISTS file_fdw"})
	if extOutput.Error == "" {
		setupTable(t, p, "CREATE SERVER def_empty_ft_server FOREIGN DATA WRAPPER file_fdw")
		setupTable(t, p, "CREATE FOREIGN TABLE def_empty_ft (id integer, name text) SERVER def_empty_ft_server OPTIONS (filename '/dev/null', format 'csv')")
		output, err = p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "def_empty_ft"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if output.Definition != "" {
			t.Fatalf("expected empty definition for foreign table, got %q", output.Definition)
		}
	}
}

func TestDescribeTable_ErrorField(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE error_field_table (id serial PRIMARY KEY, name text)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "error_field_table"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output.Error != "" {
		t.Fatalf("expected Error field to be empty on successful describe, got %q", output.Error)
	}
}

func TestDescribeTable_ForeignKeyConstraintInConstraintsList(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE fk_con_authors (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE TABLE fk_con_books (id serial PRIMARY KEY, author_id integer REFERENCES fk_con_authors(id) ON DELETE CASCADE)")

	output, err := p.DescribeTable(context.Background(), pgmcp.DescribeTableInput{Table: "fk_con_books"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	foundFK := false
	for _, con := range output.Constraints {
		if con.Type == "FOREIGN KEY" {
			foundFK = true
			if con.Name == "" {
				t.Error("expected FOREIGN KEY constraint name to be non-empty")
			}
			if con.Definition == "" {
				t.Error("expected FOREIGN KEY constraint definition to be non-empty")
			}
			break
		}
	}
	if !foundFK {
		t.Error("expected FOREIGN KEY constraint in constraints list")
	}
}

// rejectAllBeforeHookDT is a BeforeQueryHook that rejects every query.
type rejectAllBeforeHookDT struct{}

func (h *rejectAllBeforeHookDT) Run(ctx context.Context, query string) (string, error) {
	return "", fmt.Errorf("hook rejected: %s", query)
}

func TestDescribeTable_BypassesHookProtectionSanitizationPipeline(t *testing.T) {
	t.Parallel()

	// Configure hooks that reject all queries, strict protection (all blocked),
	// and sanitization rules — DescribeTable must bypass all of them.
	config := defaultConfig()
	config.DefaultHookTimeoutSeconds = 5
	config.BeforeQueryHooks = []pgmcp.BeforeQueryHookEntry{
		{
			Name: "reject_all",
			Hook: &rejectAllBeforeHookDT{},
		},
	}
	config.Sanitization = []pgmcp.SanitizationRule{
		{
			Pattern:     `.*`,
			Replacement: "REDACTED",
			Description: "redact everything",
		},
	}

	// We need a setup instance first (without hooks) to create a table with data.
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupP, connStr := newTestInstance(t, setupConfig)
	setupTable(t, setupP, `CREATE TABLE dt_bypass_test (
		id serial PRIMARY KEY,
		name text NOT NULL DEFAULT 'unnamed',
		email text UNIQUE
	)`)
	setupTable(t, setupP, "CREATE INDEX idx_dt_bypass_name ON dt_bypass_test (name)")
	setupP.Close(context.Background())

	// Now create instance with hooks/sanitization — DescribeTable must still work.
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}
	t.Cleanup(func() { p.Close(ctx) })

	output, err := p.DescribeTable(ctx, pgmcp.DescribeTableInput{Table: "dt_bypass_test"})
	if err != nil {
		t.Fatalf("DescribeTable should bypass hooks/protection/sanitization pipeline, but got error: %v", err)
	}

	// Verify basic metadata is correct and NOT sanitized.
	if output.Schema != "public" {
		t.Fatalf("expected schema 'public', got %q", output.Schema)
	}
	if output.Name != "dt_bypass_test" {
		t.Fatalf("expected name 'dt_bypass_test', got %q", output.Name)
	}
	if output.Type != "table" {
		t.Fatalf("expected type 'table', got %q", output.Type)
	}

	// Verify columns are returned correctly.
	if len(output.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(output.Columns))
	}
	colNames := make(map[string]bool)
	for _, col := range output.Columns {
		colNames[col.Name] = true
		if col.Name == "REDACTED" {
			t.Fatal("column name should NOT be sanitized — DescribeTable bypasses sanitization")
		}
	}
	if !colNames["id"] || !colNames["name"] || !colNames["email"] {
		t.Fatalf("expected columns id, name, email — got %v", output.Columns)
	}

	// Verify indexes include the one we created.
	foundIdx := false
	for _, idx := range output.Indexes {
		if idx.Name == "idx_dt_bypass_name" {
			foundIdx = true
			break
		}
	}
	if !foundIdx {
		t.Fatalf("expected to find index idx_dt_bypass_name, got %v", output.Indexes)
	}

	// Verify that Query IS blocked by the hook (proving the hook is active).
	queryOutput := p.Query(ctx, pgmcp.QueryInput{SQL: "SELECT 1"})
	if queryOutput.Error == "" {
		t.Fatal("expected Query to be rejected by hook, but it succeeded")
	}
	if !strings.Contains(queryOutput.Error, "hook rejected") {
		t.Fatalf("expected 'hook rejected' in error, got %q", queryOutput.Error)
	}
}
