package configure

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

// allEnterInputs returns enough empty lines to accept defaults for every prompt
// in the wizard. Each empty line means "accept current/default value".
// Count: 4 connection + 3 server + 3 logging + 5 pool + 5 query + 3 general + 23 protection + 5 array editors (c for each) = 51
func allEnterInputs(overrides map[int]string) string {
	lines := make([]string, 51)
	for i := range lines {
		lines[i] = ""
	}
	// Array editors need "c" to continue
	// Timeout Rules (index 40), Error Prompts (41), Sanitization Rules (42), Before Query Hooks (43), After Query Hooks (44)
	lines[40] = "c"
	lines[41] = "c"
	lines[42] = "c"
	lines[43] = "c"
	lines[44] = "c"
	for k, v := range overrides {
		lines[k] = v
	}
	return strings.Join(lines, "\n") + "\n"
}

func TestRun_NewConfig_ShowsDefaultLabel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	input := allEnterInputs(nil)
	var output bytes.Buffer

	err := run(configPath, strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	out := output.String()

	// New config should show "default" labels, not "current"
	if strings.Contains(out, "(current:") {
		t.Errorf("new config should use 'default' label, but found 'current' in output:\n%s", out)
	}
	if !strings.Contains(out, "(default:") {
		t.Errorf("new config should contain 'default' label, output:\n%s", out)
	}

	// Verify specific default values are shown
	if !strings.Contains(out, `(default: "localhost")`) {
		t.Errorf("expected default host 'localhost' in output")
	}
	if !strings.Contains(out, "(default: 5432)") {
		t.Errorf("expected default port 5432 in output")
	}
	if !strings.Contains(out, `(default: "prefer"`) {
		t.Errorf("expected default sslmode 'prefer' in output")
	}
	if !strings.Contains(out, "(default: 8080)") {
		t.Errorf("expected default server port 8080 in output")
	}
	if !strings.Contains(out, `(default: "info"`) {
		t.Errorf("expected default log level 'info' in output")
	}
	if !strings.Contains(out, `(default: "json"`) {
		t.Errorf("expected default log format 'json' in output")
	}
	if !strings.Contains(out, `(default: "stderr"`) {
		t.Errorf("expected default log output 'stderr' in output")
	}
	if !strings.Contains(out, "(default: 5)") {
		t.Errorf("expected default max_conns 5 in output")
	}
	if !strings.Contains(out, "(default: 30)") {
		t.Errorf("expected default timeout 30 in output")
	}
}

func TestRun_NewConfig_DefaultsWrittenToFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	input := allEnterInputs(nil)
	var output bytes.Buffer

	err := run(configPath, strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read config: %v", err)
	}

	var cfg pgmcp.ServerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}

	if cfg.Connection.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", cfg.Connection.Host)
	}
	if cfg.Connection.Port != 5432 {
		t.Errorf("expected port 5432, got %d", cfg.Connection.Port)
	}
	if cfg.Connection.SSLMode != "prefer" {
		t.Errorf("expected sslmode 'prefer', got %q", cfg.Connection.SSLMode)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("expected server port 8080, got %d", cfg.Server.Port)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("expected log level 'info', got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected log format 'json', got %q", cfg.Logging.Format)
	}
	if cfg.Logging.Output != "stderr" {
		t.Errorf("expected log output 'stderr', got %q", cfg.Logging.Output)
	}
	if cfg.Pool.MaxConns != 5 {
		t.Errorf("expected max_conns 5, got %d", cfg.Pool.MaxConns)
	}
	if cfg.Query.DefaultTimeoutSeconds != 30 {
		t.Errorf("expected default_timeout_seconds 30, got %d", cfg.Query.DefaultTimeoutSeconds)
	}
	if cfg.Query.ListTablesTimeoutSeconds != 10 {
		t.Errorf("expected list_tables_timeout_seconds 10, got %d", cfg.Query.ListTablesTimeoutSeconds)
	}
	if cfg.Query.DescribeTableTimeoutSeconds != 10 {
		t.Errorf("expected describe_table_timeout_seconds 10, got %d", cfg.Query.DescribeTableTimeoutSeconds)
	}
	if cfg.Query.MaxSQLLength != 100000 {
		t.Errorf("expected max_sql_length 100000, got %d", cfg.Query.MaxSQLLength)
	}
	if cfg.Query.MaxResultLength != 100000 {
		t.Errorf("expected max_result_length 100000, got %d", cfg.Query.MaxResultLength)
	}
}

func TestRun_ExistingConfig_ShowsCurrentLabel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	// Write an existing config file
	existing := &pgmcp.ServerConfig{}
	existing.Connection.Host = "myhost"
	existing.Connection.Port = 5433
	existing.Connection.SSLMode = "require"
	existing.Logging.Level = "warn"
	existing.Logging.Format = "text"
	data, _ := json.Marshal(existing)
	os.WriteFile(configPath, data, 0644)

	input := allEnterInputs(nil)
	var output bytes.Buffer

	err := run(configPath, strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	out := output.String()

	// Existing config should show "current" labels, not "default"
	if strings.Contains(out, "(default:") {
		t.Errorf("existing config should use 'current' label, but found 'default' in output:\n%s", out)
	}
	if !strings.Contains(out, "(current:") {
		t.Errorf("existing config should contain 'current' label, output:\n%s", out)
	}

	// Verify existing values are shown
	if !strings.Contains(out, `(current: "myhost")`) {
		t.Errorf("expected current host 'myhost' in output")
	}
	if !strings.Contains(out, "(current: 5433)") {
		t.Errorf("expected current port 5433 in output")
	}
}

func TestRun_ExistingConfig_PreservesValues(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	// Write an existing config
	existing := &pgmcp.ServerConfig{}
	existing.Connection.Host = "prodhost"
	existing.Connection.Port = 5433
	existing.Connection.DBName = "proddb"
	existing.Connection.SSLMode = "require"
	existing.Server.Port = 9090
	existing.Logging.Level = "error"
	existing.Logging.Format = "text"
	data, _ := json.Marshal(existing)
	os.WriteFile(configPath, data, 0644)

	// Accept all defaults (press enter for everything)
	input := allEnterInputs(nil)
	var output bytes.Buffer

	err := run(configPath, strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	// Read back
	data, _ = os.ReadFile(configPath)
	var cfg pgmcp.ServerConfig
	json.Unmarshal(data, &cfg)

	if cfg.Connection.Host != "prodhost" {
		t.Errorf("expected preserved host 'prodhost', got %q", cfg.Connection.Host)
	}
	if cfg.Connection.Port != 5433 {
		t.Errorf("expected preserved port 5433, got %d", cfg.Connection.Port)
	}
	if cfg.Connection.SSLMode != "require" {
		t.Errorf("expected preserved sslmode 'require', got %q", cfg.Connection.SSLMode)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("expected preserved server port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Logging.Level != "error" {
		t.Errorf("expected preserved level 'error', got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "text" {
		t.Errorf("expected preserved format 'text', got %q", cfg.Logging.Format)
	}
}

func TestPromptEnum_ShowsOptionsInPrompt(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("require\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptEnum("connection.sslmode", "prefer", sslModes)

	if result != "require" {
		t.Errorf("expected 'require', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, "options: disable, allow, prefer, require, verify-ca, verify-full") {
		t.Errorf("expected options list in output, got: %s", out)
	}
	if !strings.Contains(out, `(default: "prefer"`) {
		t.Errorf("expected default label with 'prefer', got: %s", out)
	}
}

func TestPromptEnum_RejectsInvalidValue(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	// First input invalid, then valid
	p := &prompter{
		scanner: newScanner("invalid\nrequire\n"),
		output:  &output,
		isNew:   false,
	}

	result := p.promptEnum("connection.sslmode", "prefer", sslModes)

	if result != "require" {
		t.Errorf("expected 'require', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, `Invalid value "invalid", must be one of: disable, allow, prefer, require, verify-ca, verify-full`) {
		t.Errorf("expected invalid value error message, got: %s", out)
	}
}

func TestPromptEnum_AcceptsEmptyForDefault(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptEnum("logging.level", "info", logLevels)

	if result != "info" {
		t.Errorf("expected default 'info', got %q", result)
	}
}

func TestPromptEnum_MultipleInvalidThenValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("bad1\nbad2\nerror\n"),
		output:  &output,
		isNew:   false,
	}

	result := p.promptEnum("logging.level", "info", logLevels)

	if result != "error" {
		t.Errorf("expected 'error', got %q", result)
	}

	out := output.String()
	// Should show the error message twice (for bad1 and bad2)
	count := strings.Count(out, "Invalid value")
	if count != 2 {
		t.Errorf("expected 2 invalid value messages, got %d", count)
	}
}

func TestPromptEnum_SSLModeAllValues(t *testing.T) {
	t.Parallel()

	for _, mode := range sslModes {
		var output bytes.Buffer
		p := &prompter{
			scanner: newScanner(mode + "\n"),
			output:  &output,
			isNew:   true,
		}

		result := p.promptEnum("connection.sslmode", "prefer", sslModes)
		if result != mode {
			t.Errorf("expected %q, got %q", mode, result)
		}
	}
}

func TestPromptEnum_LogLevelAllValues(t *testing.T) {
	t.Parallel()

	for _, level := range logLevels {
		var output bytes.Buffer
		p := &prompter{
			scanner: newScanner(level + "\n"),
			output:  &output,
			isNew:   true,
		}

		result := p.promptEnum("logging.level", "info", logLevels)
		if result != level {
			t.Errorf("expected %q, got %q", level, result)
		}
	}
}

func TestPromptEnum_LogFormatAllValues(t *testing.T) {
	t.Parallel()

	for _, format := range logFormats {
		var output bytes.Buffer
		p := &prompter{
			scanner: newScanner(format + "\n"),
			output:  &output,
			isNew:   true,
		}

		result := p.promptEnum("logging.format", "json", logFormats)
		if result != format {
			t.Errorf("expected %q, got %q", format, result)
		}
	}
}

func TestPromptEnum_CurrentLabelForExisting(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("\n"),
		output:  &output,
		isNew:   false,
	}

	p.promptEnum("logging.format", "text", logFormats)

	out := output.String()
	if !strings.Contains(out, `(current: "text"`) {
		t.Errorf("expected current label, got: %s", out)
	}
	if strings.Contains(out, "(default:") {
		t.Errorf("should not contain default label for existing config, got: %s", out)
	}
}

func TestApplyDefaults(t *testing.T) {
	t.Parallel()

	cfg := &pgmcp.ServerConfig{}
	applyDefaults(cfg)

	if cfg.Connection.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", cfg.Connection.Host)
	}
	if cfg.Connection.Port != 5432 {
		t.Errorf("expected port 5432, got %d", cfg.Connection.Port)
	}
	if cfg.Connection.SSLMode != "prefer" {
		t.Errorf("expected sslmode 'prefer', got %q", cfg.Connection.SSLMode)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("expected server port 8080, got %d", cfg.Server.Port)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("expected level 'info', got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected format 'json', got %q", cfg.Logging.Format)
	}
	if cfg.Logging.Output != "stderr" {
		t.Errorf("expected output 'stderr', got %q", cfg.Logging.Output)
	}
	if cfg.Pool.MaxConns != 5 {
		t.Errorf("expected max_conns 5, got %d", cfg.Pool.MaxConns)
	}
	if cfg.Query.DefaultTimeoutSeconds != 30 {
		t.Errorf("expected default_timeout_seconds 30, got %d", cfg.Query.DefaultTimeoutSeconds)
	}
	if cfg.Query.ListTablesTimeoutSeconds != 10 {
		t.Errorf("expected list_tables_timeout_seconds 10, got %d", cfg.Query.ListTablesTimeoutSeconds)
	}
	if cfg.Query.DescribeTableTimeoutSeconds != 10 {
		t.Errorf("expected describe_table_timeout_seconds 10, got %d", cfg.Query.DescribeTableTimeoutSeconds)
	}
	if cfg.Query.MaxSQLLength != 100000 {
		t.Errorf("expected max_sql_length 100000, got %d", cfg.Query.MaxSQLLength)
	}
	if cfg.Query.MaxResultLength != 100000 {
		t.Errorf("expected max_result_length 100000, got %d", cfg.Query.MaxResultLength)
	}

	// Fields that should NOT have defaults
	if cfg.Connection.DBName != "" {
		t.Errorf("expected empty dbname, got %q", cfg.Connection.DBName)
	}
	if cfg.Timezone != "" {
		t.Errorf("expected empty timezone, got %q", cfg.Timezone)
	}
	if cfg.ReadOnly != false {
		t.Errorf("expected read_only false, got %v", cfg.ReadOnly)
	}
}

func TestLoadExisting_NewFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "nonexistent.json")

	cfg, isNew := loadExisting(configPath)
	if !isNew {
		t.Error("expected isNew=true for nonexistent file")
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
}

func TestLoadExisting_ExistingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	existing := &pgmcp.ServerConfig{}
	existing.Connection.Host = "testhost"
	data, _ := json.Marshal(existing)
	os.WriteFile(configPath, data, 0644)

	cfg, isNew := loadExisting(configPath)
	if isNew {
		t.Error("expected isNew=false for existing file")
	}
	if cfg.Connection.Host != "testhost" {
		t.Errorf("expected host 'testhost', got %q", cfg.Connection.Host)
	}
}

func TestRun_NewConfig_EnumFieldsShowOptions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	input := allEnterInputs(nil)
	var output bytes.Buffer

	err := run(configPath, strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	out := output.String()

	// SSLMode should show options
	if !strings.Contains(out, "options: disable, allow, prefer, require, verify-ca, verify-full") {
		t.Errorf("expected sslmode options in output")
	}

	// Log level should show options
	if !strings.Contains(out, "options: debug, info, warn, error") {
		t.Errorf("expected log level options in output")
	}

	// Log format should show options
	if !strings.Contains(out, "options: json, text") {
		t.Errorf("expected log format options in output")
	}
}

func TestRun_NewConfig_OverrideEnumValues(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	// Override sslmode (index 3), logging.level (index 7), logging.format (index 8)
	input := allEnterInputs(map[int]string{
		3: "require",
		7: "debug",
		8: "text",
	})
	var output bytes.Buffer

	err := run(configPath, strings.NewReader(input), &output)
	if err != nil {
		t.Fatalf("run() returned error: %v", err)
	}

	data, _ := os.ReadFile(configPath)
	var cfg pgmcp.ServerConfig
	json.Unmarshal(data, &cfg)

	if cfg.Connection.SSLMode != "require" {
		t.Errorf("expected sslmode 'require', got %q", cfg.Connection.SSLMode)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("expected level 'debug', got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "text" {
		t.Errorf("expected format 'text', got %q", cfg.Logging.Format)
	}
}

func newScanner(input string) *bufio.Scanner {
	return bufio.NewScanner(strings.NewReader(input))
}
