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

func TestListTables_Basic(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE TABLE posts (id serial PRIMARY KEY, title text)")
	setupTable(t, p, "CREATE TABLE comments (id serial PRIMARY KEY, body text)")

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(output.Tables) < 3 {
		t.Fatalf("expected at least 3 tables, got %d", len(output.Tables))
	}

	names := map[string]bool{}
	for _, tbl := range output.Tables {
		names[tbl.Name] = true
	}
	for _, expected := range []string{"users", "posts", "comments"} {
		if !names[expected] {
			t.Fatalf("expected table %q in list", expected)
		}
	}
}

func TestListTables_IncludesViews(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE VIEW users_view AS SELECT id, name FROM users")

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, tbl := range output.Tables {
		if tbl.Name == "users_view" {
			if tbl.Type != "view" {
				t.Fatalf("expected type 'view', got %q", tbl.Type)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("view 'users_view' not found in list")
	}
}

func TestListTables_IncludesMaterializedViews(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE users (id serial PRIMARY KEY, name text)")
	setupTable(t, p, "CREATE MATERIALIZED VIEW users_matview AS SELECT id, name FROM users")

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, tbl := range output.Tables {
		if tbl.Name == "users_matview" {
			if tbl.Type != "materialized_view" {
				t.Fatalf("expected type 'materialized_view', got %q", tbl.Type)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("materialized view not found in list")
	}
}

func TestListTables_OwnerField(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, connStr := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE owner_test (id serial PRIMARY KEY)")

	// Get the current user from the connection string
	pgxConfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse connStr: %v", err)
	}
	expectedOwner := pgxConfig.User

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, tbl := range output.Tables {
		if tbl.Name == "owner_test" {
			found = true
			if tbl.Owner != expectedOwner {
				t.Fatalf("expected owner %q, got %q", expectedOwner, tbl.Owner)
			}
			break
		}
	}
	if !found {
		t.Fatal("expected table 'owner_test' in list")
	}
}

func TestListTables_ForeignTable(t *testing.T) {
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

	setupTable(t, p, "CREATE SERVER lt_ft_server FOREIGN DATA WRAPPER file_fdw")
	setupTable(t, p, "CREATE FOREIGN TABLE lt_ft_table (id integer, name text) SERVER lt_ft_server OPTIONS (filename '/dev/null', format 'csv')")

	listOutput, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, tbl := range listOutput.Tables {
		if tbl.Name == "lt_ft_table" {
			if tbl.Type != "foreign_table" {
				t.Fatalf("expected type 'foreign_table', got %q", tbl.Type)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("foreign table 'lt_ft_table' not found in list")
	}
}

func TestListTables_ExcludesSystemTables(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, tbl := range output.Tables {
		if tbl.Schema == "pg_catalog" || tbl.Schema == "information_schema" || tbl.Schema == "pg_toast" {
			t.Fatalf("system table should be excluded: %s.%s", tbl.Schema, tbl.Name)
		}
	}
}

func TestListTables_IncludesPartitionedTables(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE events (id serial, created_at timestamp NOT NULL) PARTITION BY RANGE (created_at)")

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, tbl := range output.Tables {
		if tbl.Name == "events" {
			if tbl.Type != "partitioned_table" {
				t.Fatalf("expected type 'partitioned_table', got %q", tbl.Type)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("partitioned table 'events' not found in list")
	}
}

func TestListTables_Empty(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	p, _ := newTestInstance(t, config)

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(output.Tables) != 0 {
		t.Fatalf("expected 0 tables in fresh DB, got %d", len(output.Tables))
	}
}

func TestListTables_SchemaAccessLimited(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	config.Protection.AllowManageRoles = true
	config.Protection.AllowGrantRevoke = true
	p, connStr := newTestInstance(t, config)

	// Create a restricted role (unique per test to avoid parallel conflicts)
	roleName := fmt.Sprintf("lt_restricted_%d", time.Now().UnixNano())
	setupTable(t, p, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'test123'", roleName))
	t.Cleanup(func() {
		// Drop the role after the test (roles are cluster-level, not dropped with DB)
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			conn.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", roleName))
			conn.Close(ctx)
		}
	})

	// Create schema with a table
	setupTable(t, p, "CREATE SCHEMA restricted_schema")
	setupTable(t, p, "CREATE TABLE restricted_schema.secret_table (id integer)")

	// Grant SELECT on the table but revoke USAGE on the schema
	setupTable(t, p, fmt.Sprintf("GRANT SELECT ON restricted_schema.secret_table TO %s", roleName))
	setupTable(t, p, fmt.Sprintf("REVOKE USAGE ON SCHEMA restricted_schema FROM %s", roleName))
	setupTable(t, p, "REVOKE USAGE ON SCHEMA restricted_schema FROM PUBLIC")

	// Also grant CONNECT on the database
	pgxConfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse connStr: %v", err)
	}
	setupTable(t, p, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pgxConfig.Database, roleName))

	// Build a new connection string for the restricted role
	restrictedConnStr := fmt.Sprintf("postgresql://%s:test123@%s:%d/%s?sslmode=disable",
		roleName, pgxConfig.Host, pgxConfig.Port, pgxConfig.Database)

	// Create a new pgmcp instance as the restricted user
	restrictedConfig := defaultConfig()
	ctx := context.Background()
	restrictedP, err := pgmcp.New(ctx, restrictedConnStr, restrictedConfig, testLogger())
	if err != nil {
		t.Fatalf("failed to create restricted instance: %v", err)
	}
	defer restrictedP.Close(ctx)

	// ListTables as the restricted user
	output, err := restrictedP.ListTables(ctx, pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the table in the restricted schema
	found := false
	for _, tbl := range output.Tables {
		if tbl.Name == "secret_table" && tbl.Schema == "restricted_schema" {
			found = true
			if !tbl.SchemaAccessLimited {
				t.Fatal("expected SchemaAccessLimited=true for table in schema without USAGE")
			}
			break
		}
	}
	if !found {
		t.Fatal("expected secret_table to be listed (user has SELECT grant)")
	}
}

func TestListTables_SchemaAccessNormal(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	p, _ := newTestInstance(t, config)

	setupTable(t, p, "CREATE TABLE normal_table (id serial PRIMARY KEY, name text)")

	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	found := false
	for _, tbl := range output.Tables {
		if tbl.Name == "normal_table" && tbl.Schema == "public" {
			found = true
			if tbl.SchemaAccessLimited {
				t.Fatal("expected SchemaAccessLimited=false for table in public schema")
			}
			break
		}
	}
	if !found {
		t.Fatal("expected normal_table to be listed")
	}
}

func TestListTables_Timeout(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Pool.MaxConns = 1
	config.Query.DefaultTimeoutSeconds = 30
	config.Query.ListTablesTimeoutSeconds = 10
	p, _ := newTestInstance(t, config)

	// Hold the single semaphore slot with a slow query
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(5)"})
	}()

	// Give the goroutine time to acquire the semaphore
	time.Sleep(100 * time.Millisecond)

	// ListTables with a short context timeout — blocks at semaphore, context expires
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := p.ListTables(ctx, pgmcp.ListTablesInput{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected 'context deadline exceeded' in error, got %q", err.Error())
	}

	<-done
}

func TestListTables_AcquiresSemaphore(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Pool.MaxConns = 1
	config.Query.DefaultTimeoutSeconds = 30
	p, _ := newTestInstance(t, config)

	// Hold the semaphore with a slow query (2 seconds)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(2)"})
	}()

	// Give the goroutine time to acquire the semaphore
	time.Sleep(100 * time.Millisecond)

	// ListTables should block until the slow query finishes, then succeed
	output, err := p.ListTables(context.Background(), pgmcp.ListTablesInput{})
	if err != nil {
		t.Fatalf("expected ListTables to succeed after semaphore released, got error: %v", err)
	}
	// Just verify it returned a valid output (no tables in clean DB is fine)
	_ = output

	<-done
}

func TestListTables_SemaphoreContention(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Pool.MaxConns = 1
	config.Query.DefaultTimeoutSeconds = 30
	p, _ := newTestInstance(t, config)

	// Hold the single semaphore slot with a slow query
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Query(context.Background(), pgmcp.QueryInput{SQL: "SELECT pg_sleep(5)"})
	}()

	// Give the goroutine time to acquire the semaphore
	time.Sleep(100 * time.Millisecond)

	// ListTables with a short context timeout — should fail with semaphore contention
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := p.ListTables(ctx, pgmcp.ListTablesInput{})
	if err == nil {
		t.Fatal("expected semaphore contention error")
	}
	if !strings.Contains(err.Error(), "failed to acquire query slot") {
		t.Fatalf("expected 'failed to acquire query slot' in error, got %q", err.Error())
	}

	<-done
}
