package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

func TestDoctorValidConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := validServerConfig()
	path := writeConfigFile(t, dir, cfg)

	var buf bytes.Buffer
	err := doctor(&buf, false, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	// All checks should pass
	if strings.Contains(output, "✗") {
		t.Fatalf("expected all checks to pass, but found failures in output:\n%s", output)
	}

	// Should contain pass marks
	if !strings.Contains(output, "✓") {
		t.Fatalf("expected pass marks (✓) in output:\n%s", output)
	}

	// Should contain config checks
	if !strings.Contains(output, "Config file readable") {
		t.Fatalf("expected 'Config file readable' check in output:\n%s", output)
	}
	if !strings.Contains(output, "Config file is valid JSON") {
		t.Fatalf("expected 'Config file is valid JSON' check in output:\n%s", output)
	}
	if !strings.Contains(output, "connection.dbname is set") {
		t.Fatalf("expected 'connection.dbname is set' check in output:\n%s", output)
	}
	if !strings.Contains(output, "server.port is > 0") {
		t.Fatalf("expected 'server.port is > 0' check in output:\n%s", output)
	}
	if !strings.Contains(output, "All regex patterns compile") {
		t.Fatalf("expected 'All regex patterns compile' check in output:\n%s", output)
	}

	// Should contain agent snippets
	if !strings.Contains(output, "Claude Code") {
		t.Fatalf("expected Claude Code snippet in output:\n%s", output)
	}
	if !strings.Contains(output, "claude mcp add --transport http postgres") {
		t.Fatalf("expected 'claude mcp add --transport http postgres' command in output:\n%s", output)
	}
	// Server name in snippets should be "postgres" for AI agent discoverability
	if !strings.Contains(output, `"postgres"`) {
		t.Fatalf("expected server name 'postgres' in agent snippets:\n%s", output)
	}
	if !strings.Contains(output, "Gemini CLI") {
		t.Fatalf("expected Gemini CLI snippet in output:\n%s", output)
	}
	if !strings.Contains(output, "OpenCode") {
		t.Fatalf("expected OpenCode snippet in output:\n%s", output)
	}
	if !strings.Contains(output, "Cursor") {
		t.Fatalf("expected Cursor snippet in output:\n%s", output)
	}
	if !strings.Contains(output, "Windsurf") {
		t.Fatalf("expected Windsurf snippet in output:\n%s", output)
	}
	if !strings.Contains(output, "Copilot CLI") {
		t.Fatalf("expected Copilot CLI snippet in output:\n%s", output)
	}
}

func TestDoctorMissingConfig(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := doctor(&buf, false, "/nonexistent/path/config.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	if !strings.Contains(output, "✗") {
		t.Fatalf("expected failure mark (✗) for missing config:\n%s", output)
	}
	if !strings.Contains(output, "Config file readable") {
		t.Fatalf("expected 'Config file readable' check in output:\n%s", output)
	}

	// Should not contain agent snippets when config is missing
	if strings.Contains(output, "Agent Connection Snippets") {
		t.Fatalf("expected no agent snippets when config is missing:\n%s", output)
	}
}

func TestDoctorInvalidJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte("{invalid json}"), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	var buf bytes.Buffer
	err := doctor(&buf, false, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	if !strings.Contains(output, "✗") {
		t.Fatalf("expected failure mark (✗) for invalid JSON:\n%s", output)
	}
	if !strings.Contains(output, "Config file is valid JSON") {
		t.Fatalf("expected 'Config file is valid JSON' check in output:\n%s", output)
	}

	// Should not contain agent snippets when JSON is invalid
	if strings.Contains(output, "Agent Connection Snippets") {
		t.Fatalf("expected no agent snippets when JSON is invalid:\n%s", output)
	}
}

func TestDoctorMissingDBName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.Connection.DBName = ""
	path := writeConfigFile(t, dir, cfg)

	var buf bytes.Buffer
	err := doctor(&buf, false, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	// Should show failure for dbname
	if !strings.Contains(output, "✗") {
		t.Fatalf("expected failure mark (✗) for missing dbname:\n%s", output)
	}
	if !strings.Contains(output, "connection.dbname is set") {
		t.Fatalf("expected 'connection.dbname is set' check in output:\n%s", output)
	}

	// Should still show "fix issues" message
	if !strings.Contains(output, "Fix the issues above") {
		t.Fatalf("expected 'Fix the issues above' message in output:\n%s", output)
	}
}

func TestDoctorInvalidRegex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.ErrorPrompts = []pgmcp.ErrorPromptRule{
		{Pattern: "[invalid(regex", Message: "test"},
	}
	path := writeConfigFile(t, dir, cfg)

	var buf bytes.Buffer
	err := doctor(&buf, false, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	if !strings.Contains(output, "✗") {
		t.Fatalf("expected failure mark (✗) for invalid regex:\n%s", output)
	}
	if !strings.Contains(output, "error_prompts[0] regex compiles") {
		t.Fatalf("expected 'error_prompts[0] regex compiles' check in output:\n%s", output)
	}
}

func TestDoctorPortInSnippets(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := validServerConfig()
	cfg.Server.Port = 9999
	path := writeConfigFile(t, dir, cfg)

	var buf bytes.Buffer
	err := doctor(&buf, false, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	// All agent snippets should use port 9999
	expectedURL := "http://localhost:9999/mcp"
	count := strings.Count(output, expectedURL)
	// 7 occurrences: Claude Code command (1) + Claude Code .mcp.json (1) +
	// Copilot CLI (1) + Gemini CLI (1) + OpenCode (1) + Cursor (1) + Windsurf (1)
	if count != 7 {
		t.Fatalf("expected %s to appear 7 times in agent snippets, found %d times:\n%s", expectedURL, count, output)
	}
}
