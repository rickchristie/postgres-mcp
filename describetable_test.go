//go:build integration

package pgmcp_test

import (
	"context"
	"strings"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

func TestDescribeTable_Columns(t *testing.T) {
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
		case "name":
			if col.Nullable {
				t.Error("expected name to be NOT NULL")
			}
		case "age":
			if col.Default == "" {
				t.Error("expected age to have a default")
			}
		}
	}
}

func TestDescribeTable_PrimaryKey(t *testing.T) {
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
			break
		}
	}
	if !foundPK {
		t.Error("expected PRIMARY KEY constraint in list")
	}
}

func TestDescribeTable_Indexes(t *testing.T) {
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
	for _, idx := range output.Indexes {
		if idx.Name == "idx_email" {
			found = true
			if idx.IsUnique {
				t.Error("expected non-unique index")
			}
			break
		}
	}
	if !found {
		t.Error("expected idx_email in indexes")
	}
}

func TestDescribeTable_ForeignKeys(t *testing.T) {
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
}

func TestDescribeTable_UniqueConstraint(t *testing.T) {
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
			break
		}
	}
	if !foundUnique {
		t.Error("expected UNIQUE constraint in list")
	}
}

func TestDescribeTable_CheckConstraint(t *testing.T) {
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
			break
		}
	}
	if !foundCheck {
		t.Error("expected CHECK constraint in list")
	}
}

func TestDescribeTable_DefaultValues(t *testing.T) {
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
