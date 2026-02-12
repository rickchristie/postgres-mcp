package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

// validServerConfig returns a minimal valid ServerConfig for testing.
func validServerConfig() pgmcp.ServerConfig {
	return pgmcp.ServerConfig{
		Config: pgmcp.Config{
			Pool: pgmcp.PoolConfig{MaxConns: 5},
			Query: pgmcp.QueryConfig{
				DefaultTimeoutSeconds:       30,
				ListTablesTimeoutSeconds:    10,
				DescribeTableTimeoutSeconds: 10,
			},
		},
		Server: pgmcp.ServerSettings{
			Port: 8080,
		},
		Connection: pgmcp.ConnectionConfig{
			Host:   "localhost",
			Port:   5432,
			DBName: "testdb",
		},
	}
}

func writeConfigFile(t *testing.T, dir string, config pgmcp.ServerConfig) string {
	t.Helper()
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	return path
}

// Note: Tests using t.Setenv() cannot use t.Parallel() in Go.

func TestLoadConfigValid(t *testing.T) {
	dir := t.TempDir()
	cfg := validServerConfig()
	path := writeConfigFile(t, dir, cfg)

	t.Setenv("GOPGMCP_CONFIG_PATH", path)

	loaded, err := loadServerConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded.Server.Port != 8080 {
		t.Fatalf("expected port 8080, got %d", loaded.Server.Port)
	}
	if loaded.Pool.MaxConns != 5 {
		t.Fatalf("expected max_conns 5, got %d", loaded.Pool.MaxConns)
	}
	if loaded.Query.DefaultTimeoutSeconds != 30 {
		t.Fatalf("expected default_timeout_seconds 30, got %d", loaded.Query.DefaultTimeoutSeconds)
	}
	if loaded.Connection.Host != "localhost" {
		t.Fatalf("expected host 'localhost', got %q", loaded.Connection.Host)
	}
	if loaded.Connection.Port != 5432 {
		t.Fatalf("expected connection port 5432, got %d", loaded.Connection.Port)
	}
	if loaded.Connection.DBName != "testdb" {
		t.Fatalf("expected dbname 'testdb', got %q", loaded.Connection.DBName)
	}
}

func TestLoadConfigFromEnvPath(t *testing.T) {
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.Server.Port = 9999
	path := writeConfigFile(t, dir, cfg)

	t.Setenv("GOPGMCP_CONFIG_PATH", path)

	loaded, err := loadServerConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded.Server.Port != 9999 {
		t.Fatalf("expected port 9999 from env path, got %d", loaded.Server.Port)
	}
}

func TestLoadConfigMissing(t *testing.T) {
	t.Setenv("GOPGMCP_CONFIG_PATH", "/nonexistent/path/config.json")

	_, err := loadServerConfig()
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
	if !strings.Contains(err.Error(), "/nonexistent/path/config.json") {
		t.Fatalf("expected error to contain config path, got %q", err.Error())
	}
}

func TestLoadConfigInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte("{invalid json}"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	t.Setenv("GOPGMCP_CONFIG_PATH", path)

	_, err := loadServerConfig()
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "parse") && !strings.Contains(errMsg, "unmarshal") && !strings.Contains(errMsg, "invalid") {
		t.Fatalf("expected parse/unmarshal/invalid error, got %q", errMsg)
	}
}

func TestLoadConfigValidation_NoPort(t *testing.T) {
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.Server.Port = 0
	path := writeConfigFile(t, dir, cfg)

	t.Setenv("GOPGMCP_CONFIG_PATH", path)

	loaded, err := loadServerConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	// The validation happens in runServe() which checks Server.Port <= 0.
	// We verify the loaded config has port 0, which would trigger the panic.
	if loaded.Server.Port != 0 {
		t.Fatalf("expected port 0, got %d", loaded.Server.Port)
	}
}

func TestLoadConfigValidation_HealthCheckPathEmpty(t *testing.T) {
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.Server.HealthCheckEnabled = true
	cfg.Server.HealthCheckPath = ""
	path := writeConfigFile(t, dir, cfg)

	t.Setenv("GOPGMCP_CONFIG_PATH", path)

	loaded, err := loadServerConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	// Verify the loaded config would trigger the health check validation error
	// in runServe(): "health_check_path must be set when health_check_enabled is true"
	if !loaded.Server.HealthCheckEnabled {
		t.Fatal("expected health_check_enabled to be true")
	}
	if loaded.Server.HealthCheckPath != "" {
		t.Fatalf("expected empty health_check_path, got %q", loaded.Server.HealthCheckPath)
	}
}

func TestLoadConfigValidation_HealthCheckPathNotRequiredWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.Server.HealthCheckEnabled = false
	cfg.Server.HealthCheckPath = ""
	path := writeConfigFile(t, dir, cfg)

	t.Setenv("GOPGMCP_CONFIG_PATH", path)

	loaded, err := loadServerConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	// When health check is disabled, empty path should be fine
	if loaded.Server.HealthCheckEnabled {
		t.Fatal("expected health_check_enabled to be false")
	}
}
