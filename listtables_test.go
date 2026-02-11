//go:build integration

package pgmcp_test

import (
	"context"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

func TestListTables_Basic(t *testing.T) {
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

func TestListTables_ExcludesSystemTables(t *testing.T) {
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
