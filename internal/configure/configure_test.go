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

// validExistingConfig returns a ServerConfig with all promptPositiveInt fields
// set to valid values, so pressing Enter preserves them without validation errors.
func validExistingConfig() *pgmcp.ServerConfig {
	cfg := &pgmcp.ServerConfig{}
	cfg.Connection.Host = "localhost"
	cfg.Connection.Port = 5432
	cfg.Connection.DBName = "testdb"
	cfg.Connection.SSLMode = "prefer"
	cfg.Server.Port = 8080
	cfg.Logging.Level = "info"
	cfg.Logging.Format = "json"
	cfg.Logging.Output = "stderr"
	cfg.Pool.MaxConns = 5
	cfg.Query.DefaultTimeoutSeconds = 30
	cfg.Query.ListTablesTimeoutSeconds = 10
	cfg.Query.DescribeTableTimeoutSeconds = 10
	cfg.Query.MaxSQLLength = 100000
	cfg.Query.MaxResultLength = 100000
	return cfg
}

// allEnterInputs returns enough empty lines to accept defaults for every prompt
// in the wizard. Each empty line means "accept current/default value".
// Count: 4 connection + 3 server + 3 logging + 5 pool + 5 query + 3 general + 23 protection + 5 array editors (c for each) = 51
//
// Prompt index map:
//
//	0-3:   connection (host, port, dbname, sslmode)
//	4-6:   server (port, health_check_enabled, health_check_path)
//	7-9:   logging (level, format, output)
//	10-14: pool (max_conns, min_conns, max_conn_lifetime, max_conn_idle_time, health_check_period)
//	15-19: query (default_timeout, list_tables_timeout, describe_table_timeout, max_sql_length, max_result_length)
//	20-22: general (read_only, timezone, default_hook_timeout)
//	23-45: protection (23 bool fields)
//	46-50: array editors (timeout_rules, error_prompts, sanitization, before_query hooks, after_query hooks)
func allEnterInputs(overrides map[int]string) string {
	lines := make([]string, 51)
	for i := range lines {
		lines[i] = ""
	}
	// Array editors need "c" to continue (indices 46-50)
	lines[46] = "c"
	lines[47] = "c"
	lines[48] = "c"
	lines[49] = "c"
	lines[50] = "c"
	for k, v := range overrides {
		lines[k] = v
	}
	return strings.Join(lines, "\n") + "\n"
}

func TestRun_NewConfig_ShowsDefaultLabel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	// connection.dbname (index 2) is required and has no default for new configs.
	input := allEnterInputs(map[int]string{2: "testdb"})
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

	// Verify hint text for fields with constraints
	hints := []struct {
		hint string
		desc string
	}{
		{"[required]", "connection.dbname required hint"},
		{"[must be > 0]", "server.port/connection.port/pool.max_conns must be > 0 hint"},
		{"[must be >= 0]", "pool.min_conns must be >= 0 hint"},
		{"[e.g. /healthz, required when health_check_enabled is true]", "health_check_path hint"},
		{"[stdout, stderr, or file path]", "logging.output hint"},
		{"[Go duration: e.g. 1h, 30m, 1h30m]", "pool duration hint"},
		{"[Go duration: e.g. 1m, 30s, 1m30s]", "health_check_period hint"},
		{"[seconds, must be > 0]", "timeout seconds hint"},
		{"[bytes, must be > 0]", "max_sql_length hint"},
		{"[characters, must be > 0]", "max_result_length hint"},
		{"[e.g. UTC, America/New_York, empty = server default]", "timezone hint"},
		{"[seconds, must be > 0 when hooks are configured]", "default_hook_timeout_seconds hint"},
	}
	for _, h := range hints {
		if !strings.Contains(out, h.hint) {
			t.Errorf("expected %s %q in output", h.desc, h.hint)
		}
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

	input := allEnterInputs(map[int]string{2: "testdb"})
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
	if cfg.Connection.DBName != "testdb" {
		t.Errorf("expected dbname 'testdb', got %q", cfg.Connection.DBName)
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
	if cfg.Pool.MaxConnLifetime != "1h" {
		t.Errorf("expected max_conn_lifetime '1h', got %q", cfg.Pool.MaxConnLifetime)
	}
	if cfg.Pool.MaxConnIdleTime != "30m" {
		t.Errorf("expected max_conn_idle_time '30m', got %q", cfg.Pool.MaxConnIdleTime)
	}
	if cfg.Pool.HealthCheckPeriod != "1m" {
		t.Errorf("expected health_check_period '1m', got %q", cfg.Pool.HealthCheckPeriod)
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

	// Write an existing config file with all required fields set to valid values
	existing := validExistingConfig()
	existing.Connection.Host = "myhost"
	existing.Connection.Port = 5433
	existing.Connection.DBName = "mydb"
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

	// Write an existing config with all required fields set to valid values
	existing := validExistingConfig()
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
	if cfg.Pool.MaxConnLifetime != "1h" {
		t.Errorf("expected max_conn_lifetime '1h', got %q", cfg.Pool.MaxConnLifetime)
	}
	if cfg.Pool.MaxConnIdleTime != "30m" {
		t.Errorf("expected max_conn_idle_time '30m', got %q", cfg.Pool.MaxConnIdleTime)
	}
	if cfg.Pool.HealthCheckPeriod != "1m" {
		t.Errorf("expected health_check_period '1m', got %q", cfg.Pool.HealthCheckPeriod)
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

	input := allEnterInputs(map[int]string{2: "testdb"})
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

	// Override dbname (index 2), sslmode (index 3), logging.level (index 7), logging.format (index 8)
	input := allEnterInputs(map[int]string{
		2: "testdb",
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

func TestPromptTimezone_AcceptsValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("Asia/Jakarta\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptTimezone("")

	if result != "Asia/Jakarta" {
		t.Errorf("expected 'Asia/Jakarta', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, "[e.g. UTC, America/New_York, empty = server default]") {
		t.Errorf("expected timezone hint in output, got: %s", out)
	}
}

func TestPromptTimezone_AcceptsUTC(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("UTC\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptTimezone("")

	if result != "UTC" {
		t.Errorf("expected 'UTC', got %q", result)
	}
}

func TestPromptTimezone_RejectsInvalidThenAcceptsValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("NotATimezone\nAmerica/New_York\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptTimezone("")

	if result != "America/New_York" {
		t.Errorf("expected 'America/New_York', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, `Invalid timezone "NotATimezone"`) {
		t.Errorf("expected invalid timezone error, got: %s", out)
	}
}

func TestPromptTimezone_EmptyKeepsCurrent(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("\n"),
		output:  &output,
		isNew:   false,
	}

	result := p.promptTimezone("Europe/London")

	if result != "Europe/London" {
		t.Errorf("expected 'Europe/London', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, `(current: "Europe/London")`) {
		t.Errorf("expected current label, got: %s", out)
	}
}

func TestPromptTimezone_EmptyKeepsEmpty(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptTimezone("")

	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestPromptTimezone_MultipleInvalidThenValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("bad1\nbad2\nAsia/Tokyo\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptTimezone("")

	if result != "Asia/Tokyo" {
		t.Errorf("expected 'Asia/Tokyo', got %q", result)
	}

	out := output.String()
	count := strings.Count(out, "Invalid timezone")
	if count != 2 {
		t.Errorf("expected 2 invalid timezone messages, got %d", count)
	}
}

// --- promptPositiveInt tests ---

func TestPromptPositiveInt_ShowsHintAndDefault(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: true}

	result := p.promptPositiveInt("query.max_sql_length", 100000, "bytes, must be > 0")

	if result != 100000 {
		t.Errorf("expected 100000, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "[bytes, must be > 0]") {
		t.Errorf("expected hint in output, got: %s", out)
	}
	if !strings.Contains(out, "(default: 100000)") {
		t.Errorf("expected default label, got: %s", out)
	}
}

func TestPromptPositiveInt_AcceptsValidValue(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("50000\n"), output: &output, isNew: true}

	result := p.promptPositiveInt("query.max_result_length", 100000, "characters, must be > 0")

	if result != 50000 {
		t.Errorf("expected 50000, got %d", result)
	}
}

func TestPromptPositiveInt_RejectsZeroThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("0\n5\n"), output: &output, isNew: true}

	result := p.promptPositiveInt("pool.max_conns", 5, "must be > 0")

	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be > 0") {
		t.Errorf("expected > 0 error message, got: %s", out)
	}
}

func TestPromptPositiveInt_RejectsNegativeThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("-1\n10\n"), output: &output, isNew: true}

	result := p.promptPositiveInt("server.port", 8080, "must be > 0")

	if result != 10 {
		t.Errorf("expected 10, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be > 0") {
		t.Errorf("expected > 0 error message, got: %s", out)
	}
}

func TestPromptPositiveInt_RejectsNonIntegerThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("abc\n42\n"), output: &output, isNew: true}

	result := p.promptPositiveInt("server.port", 8080, "must be > 0")

	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, `Invalid integer "abc"`) {
		t.Errorf("expected invalid integer error, got: %s", out)
	}
}

func TestPromptPositiveInt_CurrentLabelForExisting(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: false}

	result := p.promptPositiveInt("query.max_sql_length", 200000, "bytes, must be > 0")

	if result != 200000 {
		t.Errorf("expected 200000, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "(current: 200000)") {
		t.Errorf("expected current label, got: %s", out)
	}
	if strings.Contains(out, "(default:") {
		t.Errorf("should not contain default label, got: %s", out)
	}
}

// --- promptNonNegativeInt tests ---

func TestPromptNonNegativeInt_AcceptsZero(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("0\n"), output: &output, isNew: true}

	result := p.promptNonNegativeInt("pool.min_conns", 0, "must be >= 0")

	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestPromptNonNegativeInt_AcceptsPositive(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("3\n"), output: &output, isNew: true}

	result := p.promptNonNegativeInt("pool.min_conns", 0, "must be >= 0")

	if result != 3 {
		t.Errorf("expected 3, got %d", result)
	}
}

func TestPromptNonNegativeInt_RejectsNegativeThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("-1\n2\n"), output: &output, isNew: true}

	result := p.promptNonNegativeInt("pool.min_conns", 0, "must be >= 0")

	if result != 2 {
		t.Errorf("expected 2, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be >= 0") {
		t.Errorf("expected >= 0 error message, got: %s", out)
	}
}

func TestPromptNonNegativeInt_RejectsNonIntegerThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("xyz\n5\n"), output: &output, isNew: true}

	result := p.promptNonNegativeInt("pool.min_conns", 0, "must be >= 0")

	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, `Invalid integer "xyz"`) {
		t.Errorf("expected invalid integer error, got: %s", out)
	}
}

func TestPromptNonNegativeInt_EmptyKeepsCurrent(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: false}

	result := p.promptNonNegativeInt("default_hook_timeout_seconds", 10, "seconds, must be > 0 when hooks are configured")

	if result != 10 {
		t.Errorf("expected 10, got %d", result)
	}
}

// --- promptDuration tests ---

func TestPromptDuration_AcceptsValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("2h\n"), output: &output, isNew: true}

	result := p.promptDuration("pool.max_conn_lifetime", "1h", "Go duration: e.g. 1h, 30m, 1h30m")

	if result != "2h" {
		t.Errorf("expected '2h', got %q", result)
	}
}

func TestPromptDuration_EmptyKeepsCurrent(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: true}

	result := p.promptDuration("pool.max_conn_lifetime", "1h", "Go duration: e.g. 1h, 30m, 1h30m")

	if result != "1h" {
		t.Errorf("expected '1h', got %q", result)
	}
}

func TestPromptDuration_RejectsInvalidThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("notaduration\n30m\n"), output: &output, isNew: true}

	result := p.promptDuration("pool.max_conn_idle_time", "30m", "Go duration: e.g. 1h, 30m, 1h30m")

	if result != "30m" {
		t.Errorf("expected '30m', got %q", result)
	}
	out := output.String()
	if !strings.Contains(out, `Invalid Go duration "notaduration"`) {
		t.Errorf("expected invalid duration error, got: %s", out)
	}
}

func TestPromptDuration_ShowsHint(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: true}

	p.promptDuration("pool.health_check_period", "1m", "Go duration: e.g. 1m, 30s, 1m30s")

	out := output.String()
	if !strings.Contains(out, "[Go duration: e.g. 1m, 30s, 1m30s]") {
		t.Errorf("expected duration hint, got: %s", out)
	}
}

// --- promptInt re-ask loop tests ---

func TestPromptInt_RejectsNonIntegerThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("abc\n42\n"), output: &output, isNew: true}

	result := p.promptInt("some_field", 10)

	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, `Invalid integer "abc"`) {
		t.Errorf("expected invalid integer error, got: %s", out)
	}
}

func TestPromptInt_MultipleInvalidThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("bad\nworse\n7\n"), output: &output, isNew: true}

	result := p.promptInt("some_field", 10)

	if result != 7 {
		t.Errorf("expected 7, got %d", result)
	}
	out := output.String()
	count := strings.Count(out, "Invalid integer")
	if count != 2 {
		t.Errorf("expected 2 invalid integer messages, got %d", count)
	}
}

// --- promptBool re-ask loop tests ---

func TestPromptBool_RejectsInvalidThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("maybe\ntrue\n"), output: &output, isNew: true}

	result := p.promptBool("read_only", false)

	if result != true {
		t.Errorf("expected true, got %v", result)
	}
	out := output.String()
	if !strings.Contains(out, `Invalid value "maybe"`) {
		t.Errorf("expected invalid boolean error, got: %s", out)
	}
	if !strings.Contains(out, "use true/false/yes/no") {
		t.Errorf("expected guidance on valid values, got: %s", out)
	}
}

func TestPromptBool_MultipleInvalidThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("bad\nworse\nno\n"), output: &output, isNew: true}

	result := p.promptBool("read_only", true)

	if result != false {
		t.Errorf("expected false, got %v", result)
	}
	out := output.String()
	count := strings.Count(out, "Invalid value")
	if count != 2 {
		t.Errorf("expected 2 invalid value messages, got %d", count)
	}
}

// --- promptNewRegexField tests ---

func TestPromptNewRegexField_AcceptsValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("^SELECT.*\n"), output: &output, isNew: true}

	result := p.promptNewRegexField("pattern")

	if result != "^SELECT.*" {
		t.Errorf("expected '^SELECT.*', got %q", result)
	}
}

func TestPromptNewRegexField_AcceptsEmpty(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: true}

	result := p.promptNewRegexField("pattern")

	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestPromptNewRegexField_RejectsInvalidThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("[invalid\n.*valid.*\n"), output: &output, isNew: true}

	result := p.promptNewRegexField("pattern")

	if result != ".*valid.*" {
		t.Errorf("expected '.*valid.*', got %q", result)
	}
	out := output.String()
	if !strings.Contains(out, `Invalid regex "[invalid"`) {
		t.Errorf("expected invalid regex error, got: %s", out)
	}
}

// --- promptNewPositiveIntField tests ---

func TestPromptNewPositiveIntField_AcceptsValid(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("30\n"), output: &output, isNew: true}

	result := p.promptNewPositiveIntField("timeout_seconds")

	if result != 30 {
		t.Errorf("expected 30, got %d", result)
	}
}

func TestPromptNewPositiveIntField_RejectsZeroThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("0\n5\n"), output: &output, isNew: true}

	result := p.promptNewPositiveIntField("timeout_seconds")

	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be > 0") {
		t.Errorf("expected > 0 error message, got: %s", out)
	}
}

func TestPromptNewPositiveIntField_RejectsEmptyThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n10\n"), output: &output, isNew: true}

	result := p.promptNewPositiveIntField("timeout_seconds")

	if result != 10 {
		t.Errorf("expected 10, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value is required and must be > 0") {
		t.Errorf("expected required error message, got: %s", out)
	}
}

// --- promptNewNonNegativeIntField tests ---

func TestPromptNewNonNegativeIntField_AcceptsZero(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("0\n"), output: &output, isNew: true}

	result := p.promptNewNonNegativeIntField("timeout_seconds")

	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestPromptNewNonNegativeIntField_AcceptsEmpty(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: true}

	result := p.promptNewNonNegativeIntField("timeout_seconds")

	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestPromptNewNonNegativeIntField_RejectsNegativeThenAccepts(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("-5\n3\n"), output: &output, isNew: true}

	result := p.promptNewNonNegativeIntField("timeout_seconds")

	if result != 3 {
		t.Errorf("expected 3, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be >= 0") {
		t.Errorf("expected >= 0 error message, got: %s", out)
	}
}

func TestPromptStringWithHint_ShowsHintAndDefault(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptStringWithHint("logging.output", "stderr", "stdout, stderr, or file path")

	if result != "stderr" {
		t.Errorf("expected 'stderr', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, "[stdout, stderr, or file path]") {
		t.Errorf("expected hint in output, got: %s", out)
	}
	if !strings.Contains(out, `(default: "stderr")`) {
		t.Errorf("expected default label, got: %s", out)
	}
}

func TestPromptStringWithHint_AcceptsOverride(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("/var/log/pgmcp.log\n"),
		output:  &output,
		isNew:   true,
	}

	result := p.promptStringWithHint("logging.output", "stderr", "stdout, stderr, or file path")

	if result != "/var/log/pgmcp.log" {
		t.Errorf("expected '/var/log/pgmcp.log', got %q", result)
	}
}

func TestPromptStringWithHint_CurrentLabelForExisting(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{
		scanner: newScanner("\n"),
		output:  &output,
		isNew:   false,
	}

	result := p.promptStringWithHint("pool.max_conn_lifetime", "1h", "Go duration: e.g. 1h, 30m, 1h30m")

	if result != "1h" {
		t.Errorf("expected '1h', got %q", result)
	}

	out := output.String()
	if !strings.Contains(out, "[Go duration: e.g. 1h, 30m, 1h30m]") {
		t.Errorf("expected hint in output, got: %s", out)
	}
	if !strings.Contains(out, `(current: "1h")`) {
		t.Errorf("expected current label, got: %s", out)
	}
	if strings.Contains(out, "(default:") {
		t.Errorf("should not contain default label for existing config, got: %s", out)
	}
}

// --- promptRequiredStringWithHint tests ---

func TestPromptRequiredStringWithHint_AcceptsNonEmpty(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("mydb\n"), output: &output, isNew: true}

	result := p.promptRequiredStringWithHint("connection.dbname", "", "required")

	if result != "mydb" {
		t.Errorf("expected 'mydb', got %q", result)
	}
}

func TestPromptRequiredStringWithHint_RejectsEmptyWhenCurrentEmpty(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\nmydb\n"), output: &output, isNew: true}

	result := p.promptRequiredStringWithHint("connection.dbname", "", "required")

	if result != "mydb" {
		t.Errorf("expected 'mydb', got %q", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value is required") {
		t.Errorf("expected required error message, got: %s", out)
	}
}

func TestPromptRequiredStringWithHint_AcceptsEnterWhenCurrentNonEmpty(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n"), output: &output, isNew: false}

	result := p.promptRequiredStringWithHint("connection.dbname", "existingdb", "required")

	if result != "existingdb" {
		t.Errorf("expected 'existingdb', got %q", result)
	}
}

// --- promptPositiveInt: reject Enter on invalid current ---

func TestPromptPositiveInt_RejectsEnterWhenCurrentZero(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n5\n"), output: &output, isNew: false}

	result := p.promptPositiveInt("pool.max_conns", 0, "must be > 0")

	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be > 0") {
		t.Errorf("expected > 0 error message, got: %s", out)
	}
}

func TestPromptPositiveInt_RejectsEnterWhenCurrentNegative(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	p := &prompter{scanner: newScanner("\n10\n"), output: &output, isNew: false}

	result := p.promptPositiveInt("server.port", -1, "must be > 0")

	if result != 10 {
		t.Errorf("expected 10, got %d", result)
	}
	out := output.String()
	if !strings.Contains(out, "Value must be > 0") {
		t.Errorf("expected > 0 error message, got: %s", out)
	}
}

func newScanner(input string) *bufio.Scanner {
	return bufio.NewScanner(strings.NewReader(input))
}
