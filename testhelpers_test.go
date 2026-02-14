package pgmcp_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rickchristie/govner/pgflock/client"
	pgmcp "github.com/rickchristie/postgres-mcp"
	"github.com/rs/zerolog"
)

const (
	pgflockLockerPort = 9776
	pgflockPassword   = "pgflock"
)

func acquireTestDB(t *testing.T) string {
	t.Helper()
	connStr, err := client.Lock(pgflockLockerPort, t.Name(), pgflockPassword)
	if err != nil {
		t.Fatalf("Failed to acquire test database: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Unlock(pgflockLockerPort, pgflockPassword, connStr)
	})
	return connStr
}

func testLogger() zerolog.Logger {
	return zerolog.New(os.Stderr).Level(zerolog.Disabled)
}

func defaultConfig() pgmcp.Config {
	return pgmcp.Config{
		Pool: pgmcp.PoolConfig{MaxConns: 5},
		Query: pgmcp.QueryConfig{
			DefaultTimeoutSeconds:       30,
			ListTablesTimeoutSeconds:    10,
			DescribeTableTimeoutSeconds: 10,
			MaxSQLLength:                100000,
			MaxResultLength:             100000,
		},
	}
}

func newTestInstance(t *testing.T, config pgmcp.Config) (*pgmcp.PostgresMcp, string) {
	t.Helper()
	connStr := acquireTestDB(t)
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	t.Cleanup(func() { p.Close(ctx) })
	return p, connStr
}

func newTestInstanceWithHooks(t *testing.T, config pgmcp.Config, hooks pgmcp.ServerHooksConfig) *pgmcp.PostgresMcp {
	t.Helper()
	connStr := acquireTestDB(t)
	ctx := context.Background()
	p, err := pgmcp.New(ctx, connStr, config, testLogger(), pgmcp.WithServerHooks(hooks))
	if err != nil {
		t.Fatalf("Failed to create PostgresMcp: %v", err)
	}
	t.Cleanup(func() { p.Close(ctx) })
	return p
}

func hookScript(name string) string {
	return filepath.Join("testdata", "hooks", name)
}

func setupTable(t *testing.T, p *pgmcp.PostgresMcp, sql string) {
	t.Helper()
	output := p.Query(context.Background(), pgmcp.QueryInput{SQL: sql})
	if output.Error != "" {
		t.Fatalf("setup failed: %s", output.Error)
	}
}

// newReadOnlyTestInstance creates a read-only PostgresMcp instance with tables
// pre-populated by setupFn. It first creates a write instance to run DDL/DML,
// then closes it and creates a read-only instance with the given config.
func newReadOnlyTestInstance(t *testing.T, config pgmcp.Config, setupFn func(t *testing.T, p *pgmcp.PostgresMcp)) *pgmcp.PostgresMcp {
	t.Helper()
	connStr := acquireTestDB(t)
	ctx := context.Background()

	// Set up tables with a write instance
	setupConfig := defaultConfig()
	setupConfig.Protection.AllowDDL = true
	setupConfig.Protection.AllowMerge = true
	setupP, err := pgmcp.New(ctx, connStr, setupConfig, testLogger())
	if err != nil {
		t.Fatalf("failed to create setup instance: %v", err)
	}
	setupFn(t, setupP)
	setupP.Close(ctx)

	// Create read-only instance
	config.ReadOnly = true
	p, err := pgmcp.New(ctx, connStr, config, testLogger())
	if err != nil {
		t.Fatalf("failed to create read-only instance: %v", err)
	}
	t.Cleanup(func() { p.Close(ctx) })
	return p
}
