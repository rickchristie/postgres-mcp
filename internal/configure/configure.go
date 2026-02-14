package configure

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	pgmcp "github.com/rickchristie/postgres-mcp"
)

// Run runs the interactive configuration wizard.
// Reads existing config (if any), prompts for each field,
// writes updated config to the given path.
func Run(configPath string) error {
	return run(configPath, os.Stdin, os.Stderr)
}

func run(configPath string, input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	cfg, isNew := loadExisting(configPath)
	if isNew {
		applyDefaults(cfg)
	}

	p := &prompter{
		scanner: scanner,
		output:  output,
		isNew:   isNew,
	}

	fmt.Fprintf(output, "gopgmcp configuration wizard\n")
	fmt.Fprintf(output, "Config file: %s\n\n", configPath)

	// Connection
	fmt.Fprintf(output, "=== Connection ===\n")
	cfg.Connection.Host = p.promptString("connection.host", cfg.Connection.Host)
	cfg.Connection.Port = p.promptPositiveInt("connection.port", cfg.Connection.Port, "must be > 0")
	cfg.Connection.DBName = p.promptStringWithHint("connection.dbname", cfg.Connection.DBName, "required")
	cfg.Connection.SSLMode = p.promptEnum("connection.sslmode", cfg.Connection.SSLMode, sslModes)

	// Server
	fmt.Fprintf(output, "\n=== Server ===\n")
	cfg.Server.Port = p.promptPositiveInt("server.port", cfg.Server.Port, "must be > 0")
	cfg.Server.HealthCheckEnabled = p.promptBool("server.health_check_enabled", cfg.Server.HealthCheckEnabled)
	cfg.Server.HealthCheckPath = p.promptStringWithHint("server.health_check_path", cfg.Server.HealthCheckPath, "e.g. /healthz, required when health_check_enabled is true")

	// Logging
	fmt.Fprintf(output, "\n=== Logging ===\n")
	cfg.Logging.Level = p.promptEnum("logging.level", cfg.Logging.Level, logLevels)
	cfg.Logging.Format = p.promptEnum("logging.format", cfg.Logging.Format, logFormats)
	cfg.Logging.Output = p.promptStringWithHint("logging.output", cfg.Logging.Output, "stdout, stderr, or file path")

	// Pool
	fmt.Fprintf(output, "\n=== Pool ===\n")
	cfg.Pool.MaxConns = p.promptPositiveInt("pool.max_conns", cfg.Pool.MaxConns, "must be > 0")
	cfg.Pool.MinConns = p.promptNonNegativeInt("pool.min_conns", cfg.Pool.MinConns, "must be >= 0")
	cfg.Pool.MaxConnLifetime = p.promptDuration("pool.max_conn_lifetime", cfg.Pool.MaxConnLifetime, "Go duration: e.g. 1h, 30m, 1h30m")
	cfg.Pool.MaxConnIdleTime = p.promptDuration("pool.max_conn_idle_time", cfg.Pool.MaxConnIdleTime, "Go duration: e.g. 1h, 30m, 1h30m")
	cfg.Pool.HealthCheckPeriod = p.promptDuration("pool.health_check_period", cfg.Pool.HealthCheckPeriod, "Go duration: e.g. 1m, 30s, 1m30s")

	// Query
	fmt.Fprintf(output, "\n=== Query ===\n")
	cfg.Query.DefaultTimeoutSeconds = p.promptPositiveInt("query.default_timeout_seconds", cfg.Query.DefaultTimeoutSeconds, "seconds, must be > 0")
	cfg.Query.ListTablesTimeoutSeconds = p.promptPositiveInt("query.list_tables_timeout_seconds", cfg.Query.ListTablesTimeoutSeconds, "seconds, must be > 0")
	cfg.Query.DescribeTableTimeoutSeconds = p.promptPositiveInt("query.describe_table_timeout_seconds", cfg.Query.DescribeTableTimeoutSeconds, "seconds, must be > 0")
	cfg.Query.MaxSQLLength = p.promptPositiveInt("query.max_sql_length", cfg.Query.MaxSQLLength, "bytes, must be > 0")
	cfg.Query.MaxResultLength = p.promptPositiveInt("query.max_result_length", cfg.Query.MaxResultLength, "characters, must be > 0")

	// Read-only and misc
	fmt.Fprintf(output, "\n=== General ===\n")
	cfg.ReadOnly = p.promptBool("read_only", cfg.ReadOnly)
	cfg.Timezone = p.promptTimezone(cfg.Timezone)
	cfg.DefaultHookTimeoutSeconds = p.promptNonNegativeInt("default_hook_timeout_seconds", cfg.DefaultHookTimeoutSeconds, "seconds, must be > 0 when hooks are configured")

	// Protection
	fmt.Fprintf(output, "\n=== Protection ===\n")
	cfg.Protection.AllowSet = p.promptBool("protection.allow_set", cfg.Protection.AllowSet)
	cfg.Protection.AllowDrop = p.promptBool("protection.allow_drop", cfg.Protection.AllowDrop)
	cfg.Protection.AllowTruncate = p.promptBool("protection.allow_truncate", cfg.Protection.AllowTruncate)
	cfg.Protection.AllowDo = p.promptBool("protection.allow_do", cfg.Protection.AllowDo)
	cfg.Protection.AllowCopyFrom = p.promptBool("protection.allow_copy_from", cfg.Protection.AllowCopyFrom)
	cfg.Protection.AllowCopyTo = p.promptBool("protection.allow_copy_to", cfg.Protection.AllowCopyTo)
	cfg.Protection.AllowCreateFunction = p.promptBool("protection.allow_create_function", cfg.Protection.AllowCreateFunction)
	cfg.Protection.AllowPrepare = p.promptBool("protection.allow_prepare", cfg.Protection.AllowPrepare)
	cfg.Protection.AllowDeleteWithoutWhere = p.promptBool("protection.allow_delete_without_where", cfg.Protection.AllowDeleteWithoutWhere)
	cfg.Protection.AllowUpdateWithoutWhere = p.promptBool("protection.allow_update_without_where", cfg.Protection.AllowUpdateWithoutWhere)
	cfg.Protection.AllowAlterSystem = p.promptBool("protection.allow_alter_system", cfg.Protection.AllowAlterSystem)
	cfg.Protection.AllowMerge = p.promptBool("protection.allow_merge", cfg.Protection.AllowMerge)
	cfg.Protection.AllowGrantRevoke = p.promptBool("protection.allow_grant_revoke", cfg.Protection.AllowGrantRevoke)
	cfg.Protection.AllowManageRoles = p.promptBool("protection.allow_manage_roles", cfg.Protection.AllowManageRoles)
	cfg.Protection.AllowCreateExtension = p.promptBool("protection.allow_create_extension", cfg.Protection.AllowCreateExtension)
	cfg.Protection.AllowLockTable = p.promptBool("protection.allow_lock_table", cfg.Protection.AllowLockTable)
	cfg.Protection.AllowListenNotify = p.promptBool("protection.allow_listen_notify", cfg.Protection.AllowListenNotify)
	cfg.Protection.AllowMaintenance = p.promptBool("protection.allow_maintenance", cfg.Protection.AllowMaintenance)
	cfg.Protection.AllowDDL = p.promptBool("protection.allow_ddl", cfg.Protection.AllowDDL)
	cfg.Protection.AllowDiscard = p.promptBool("protection.allow_discard", cfg.Protection.AllowDiscard)
	cfg.Protection.AllowComment = p.promptBool("protection.allow_comment", cfg.Protection.AllowComment)
	cfg.Protection.AllowCreateTrigger = p.promptBool("protection.allow_create_trigger", cfg.Protection.AllowCreateTrigger)
	cfg.Protection.AllowCreateRule = p.promptBool("protection.allow_create_rule", cfg.Protection.AllowCreateRule)

	// Array fields
	fmt.Fprintf(output, "\n=== Timeout Rules ===\n")
	cfg.Query.TimeoutRules = p.promptTimeoutRules(cfg.Query.TimeoutRules)

	fmt.Fprintf(output, "\n=== Error Prompts ===\n")
	cfg.ErrorPrompts = p.promptErrorPrompts(cfg.ErrorPrompts)

	fmt.Fprintf(output, "\n=== Sanitization Rules ===\n")
	cfg.Sanitization = p.promptSanitizationRules(cfg.Sanitization)

	fmt.Fprintf(output, "\n=== Server Hooks: Before Query ===\n")
	cfg.ServerHooks.BeforeQuery = p.promptHookEntries("server_hooks.before_query", cfg.ServerHooks.BeforeQuery)

	fmt.Fprintf(output, "\n=== Server Hooks: After Query ===\n")
	cfg.ServerHooks.AfterQuery = p.promptHookEntries("server_hooks.after_query", cfg.ServerHooks.AfterQuery)

	// Write config
	if err := writeConfig(configPath, cfg); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	fmt.Fprintf(output, "\nConfiguration saved to %s\n", configPath)
	return nil
}

func loadExisting(configPath string) (*pgmcp.ServerConfig, bool) {
	cfg := &pgmcp.ServerConfig{}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, true
	}
	// Ignore unmarshal errors — start with whatever was parseable.
	_ = json.Unmarshal(data, cfg)
	return cfg, false
}

// applyDefaults sets sensible default values for a new configuration.
func applyDefaults(cfg *pgmcp.ServerConfig) {
	cfg.Connection.Host = "localhost"
	cfg.Connection.Port = 5432
	cfg.Connection.SSLMode = "prefer"
	cfg.Server.Port = 8080
	cfg.Logging.Level = "info"
	cfg.Logging.Format = "json"
	cfg.Logging.Output = "stderr"
	cfg.Pool.MaxConns = 5
	cfg.Pool.MaxConnLifetime = "1h"
	cfg.Pool.MaxConnIdleTime = "30m"
	cfg.Pool.HealthCheckPeriod = "1m"
	cfg.Query.DefaultTimeoutSeconds = 30
	cfg.Query.ListTablesTimeoutSeconds = 10
	cfg.Query.DescribeTableTimeoutSeconds = 10
	cfg.Query.MaxSQLLength = 100000
	cfg.Query.MaxResultLength = 100000
}

var (
	sslModes  = []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
	logLevels = []string{"debug", "info", "warn", "error"}
	logFormats = []string{"json", "text"}
)

func writeConfig(configPath string, cfg *pgmcp.ServerConfig) error {
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Append trailing newline.
	data = append(data, '\n')

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", configPath, err)
	}

	return nil
}

// prompter handles reading user input and displaying prompts.
type prompter struct {
	scanner *bufio.Scanner
	output  io.Writer
	isNew   bool
}

func (p *prompter) readLine() string {
	if p.scanner.Scan() {
		return strings.TrimSpace(p.scanner.Text())
	}
	return ""
}

func (p *prompter) valueLabel() string {
	if p.isNew {
		return "default"
	}
	return "current"
}

func (p *prompter) promptString(field string, current string) string {
	fmt.Fprintf(p.output, "%s (%s: %q): ", field, p.valueLabel(), current)
	input := p.readLine()
	if input == "" {
		return current
	}
	return input
}

func (p *prompter) promptStringWithHint(field string, current string, hint string) string {
	fmt.Fprintf(p.output, "%s [%s] (%s: %q): ", field, hint, p.valueLabel(), current)
	input := p.readLine()
	if input == "" {
		return current
	}
	return input
}

func (p *prompter) promptInt(field string, current int) int {
	for {
		fmt.Fprintf(p.output, "%s (%s: %d): ", field, p.valueLabel(), current)
		input := p.readLine()
		if input == "" {
			return current
		}
		val, err := strconv.Atoi(input)
		if err != nil {
			fmt.Fprintf(p.output, "  Invalid integer %q, try again.\n", input)
			continue
		}
		return val
	}
}

func (p *prompter) promptPositiveInt(field string, current int, hint string) int {
	for {
		fmt.Fprintf(p.output, "%s [%s] (%s: %d): ", field, hint, p.valueLabel(), current)
		input := p.readLine()
		if input == "" {
			return current
		}
		val, err := strconv.Atoi(input)
		if err != nil {
			fmt.Fprintf(p.output, "  Invalid integer %q, try again.\n", input)
			continue
		}
		if val <= 0 {
			fmt.Fprintf(p.output, "  Value must be > 0, try again.\n")
			continue
		}
		return val
	}
}

func (p *prompter) promptNonNegativeInt(field string, current int, hint string) int {
	for {
		fmt.Fprintf(p.output, "%s [%s] (%s: %d): ", field, hint, p.valueLabel(), current)
		input := p.readLine()
		if input == "" {
			return current
		}
		val, err := strconv.Atoi(input)
		if err != nil {
			fmt.Fprintf(p.output, "  Invalid integer %q, try again.\n", input)
			continue
		}
		if val < 0 {
			fmt.Fprintf(p.output, "  Value must be >= 0, try again.\n")
			continue
		}
		return val
	}
}

func (p *prompter) promptBool(field string, current bool) bool {
	for {
		fmt.Fprintf(p.output, "%s (%s: %v): ", field, p.valueLabel(), current)
		input := p.readLine()
		if input == "" {
			return current
		}
		switch strings.ToLower(input) {
		case "true", "t", "yes", "y", "1":
			return true
		case "false", "f", "no", "n", "0":
			return false
		default:
			fmt.Fprintf(p.output, "  Invalid value %q, use true/false/yes/no, try again.\n", input)
		}
	}
}

func (p *prompter) promptDuration(field string, current string, hint string) string {
	for {
		fmt.Fprintf(p.output, "%s [%s] (%s: %q): ", field, hint, p.valueLabel(), current)
		input := p.readLine()
		if input == "" {
			return current
		}
		if _, err := time.ParseDuration(input); err != nil {
			fmt.Fprintf(p.output, "  Invalid Go duration %q, try again.\n", input)
			continue
		}
		return input
	}
}

func (p *prompter) promptTimezone(current string) string {
	for {
		fmt.Fprintf(p.output, "timezone [e.g. UTC, America/New_York, empty = server default] (%s: %q): ", p.valueLabel(), current)
		input := p.readLine()
		if input == "" {
			return current
		}
		if _, err := time.LoadLocation(input); err != nil {
			fmt.Fprintf(p.output, "  Invalid timezone %q, please enter a valid IANA timezone.\n", input)
			continue
		}
		return input
	}
}

func (p *prompter) promptEnum(field string, current string, allowed []string) string {
	for {
		fmt.Fprintf(p.output, "%s (%s: %q, options: %s): ", field, p.valueLabel(), current, strings.Join(allowed, ", "))
		input := p.readLine()
		if input == "" {
			return current
		}
		for _, v := range allowed {
			if input == v {
				return input
			}
		}
		fmt.Fprintf(p.output, "  Invalid value %q, must be one of: %s\n", input, strings.Join(allowed, ", "))
	}
}

// Array field editors

func (p *prompter) promptTimeoutRules(current []pgmcp.TimeoutRule) []pgmcp.TimeoutRule {
	rules := current
	for {
		p.displayTimeoutRules(rules)
		fmt.Fprintf(p.output, "[a]dd, [r]emove, [c]ontinue? ")
		choice := strings.ToLower(p.readLine())
		switch choice {
		case "a":
			pattern := p.promptNewRegexField("pattern")
			timeout := p.promptNewPositiveIntField("timeout_seconds")
			rules = append(rules, pgmcp.TimeoutRule{
				Pattern:        pattern,
				TimeoutSeconds: timeout,
			})
		case "r":
			rules = removeByIndex(p, "timeout rule", rules)
		case "c", "":
			return rules
		default:
			fmt.Fprintf(p.output, "  Unknown choice, try again.\n")
		}
	}
}

func (p *prompter) displayTimeoutRules(rules []pgmcp.TimeoutRule) {
	if len(rules) == 0 {
		fmt.Fprintf(p.output, "  (no entries)\n")
		return
	}
	for i, r := range rules {
		fmt.Fprintf(p.output, "  [%d] pattern=%q timeout_seconds=%d\n", i, r.Pattern, r.TimeoutSeconds)
	}
}

func (p *prompter) promptErrorPrompts(current []pgmcp.ErrorPromptRule) []pgmcp.ErrorPromptRule {
	rules := current
	for {
		p.displayErrorPrompts(rules)
		fmt.Fprintf(p.output, "[a]dd, [r]emove, [c]ontinue? ")
		choice := strings.ToLower(p.readLine())
		switch choice {
		case "a":
			pattern := p.promptNewRegexField("pattern")
			message := p.promptNewField("message")
			rules = append(rules, pgmcp.ErrorPromptRule{
				Pattern: pattern,
				Message: message,
			})
		case "r":
			rules = removeByIndex(p, "error prompt", rules)
		case "c", "":
			return rules
		default:
			fmt.Fprintf(p.output, "  Unknown choice, try again.\n")
		}
	}
}

func (p *prompter) displayErrorPrompts(rules []pgmcp.ErrorPromptRule) {
	if len(rules) == 0 {
		fmt.Fprintf(p.output, "  (no entries)\n")
		return
	}
	for i, r := range rules {
		fmt.Fprintf(p.output, "  [%d] pattern=%q message=%q\n", i, r.Pattern, r.Message)
	}
}

func (p *prompter) promptSanitizationRules(current []pgmcp.SanitizationRule) []pgmcp.SanitizationRule {
	rules := current
	for {
		p.displaySanitizationRules(rules)
		fmt.Fprintf(p.output, "[a]dd, [r]emove, [c]ontinue? ")
		choice := strings.ToLower(p.readLine())
		switch choice {
		case "a":
			pattern := p.promptNewRegexField("pattern")
			replacement := p.promptNewField("replacement")
			description := p.promptNewField("description")
			rules = append(rules, pgmcp.SanitizationRule{
				Pattern:     pattern,
				Replacement: replacement,
				Description: description,
			})
		case "r":
			rules = removeByIndex(p, "sanitization rule", rules)
		case "c", "":
			return rules
		default:
			fmt.Fprintf(p.output, "  Unknown choice, try again.\n")
		}
	}
}

func (p *prompter) displaySanitizationRules(rules []pgmcp.SanitizationRule) {
	if len(rules) == 0 {
		fmt.Fprintf(p.output, "  (no entries)\n")
		return
	}
	for i, r := range rules {
		fmt.Fprintf(p.output, "  [%d] pattern=%q replacement=%q description=%q\n", i, r.Pattern, r.Replacement, r.Description)
	}
}

func (p *prompter) promptHookEntries(label string, current []pgmcp.HookEntry) []pgmcp.HookEntry {
	entries := current
	for {
		p.displayHookEntries(entries)
		fmt.Fprintf(p.output, "[a]dd, [r]emove, [c]ontinue? ")
		choice := strings.ToLower(p.readLine())
		switch choice {
		case "a":
			pattern := p.promptNewRegexField("pattern")
			command := p.promptNewField("command")
			argsStr := p.promptNewField("args (comma-separated)")
			var args []string
			if argsStr != "" {
				for _, a := range strings.Split(argsStr, ",") {
					args = append(args, strings.TrimSpace(a))
				}
			}
			timeout := p.promptNewNonNegativeIntField("timeout_seconds")
			entries = append(entries, pgmcp.HookEntry{
				Pattern:        pattern,
				Command:        command,
				Args:           args,
				TimeoutSeconds: timeout,
			})
		case "r":
			entries = removeByIndex(p, label, entries)
		case "c", "":
			return entries
		default:
			fmt.Fprintf(p.output, "  Unknown choice, try again.\n")
		}
	}
}

func (p *prompter) displayHookEntries(entries []pgmcp.HookEntry) {
	if len(entries) == 0 {
		fmt.Fprintf(p.output, "  (no entries)\n")
		return
	}
	for i, e := range entries {
		fmt.Fprintf(p.output, "  [%d] pattern=%q command=%q args=%v timeout_seconds=%d\n",
			i, e.Pattern, e.Command, e.Args, e.TimeoutSeconds)
	}
}

func (p *prompter) promptNewField(name string) string {
	fmt.Fprintf(p.output, "  %s: ", name)
	return p.readLine()
}

func (p *prompter) promptNewRegexField(name string) string {
	for {
		fmt.Fprintf(p.output, "  %s (regex): ", name)
		input := p.readLine()
		if input == "" {
			return ""
		}
		if _, err := regexp.Compile(input); err != nil {
			fmt.Fprintf(p.output, "  Invalid regex %q: %v, try again.\n", input, err)
			continue
		}
		return input
	}
}

func (p *prompter) promptNewPositiveIntField(name string) int {
	for {
		fmt.Fprintf(p.output, "  %s (must be > 0): ", name)
		input := p.readLine()
		if input == "" {
			fmt.Fprintf(p.output, "  Value is required and must be > 0, try again.\n")
			continue
		}
		val, err := strconv.Atoi(input)
		if err != nil {
			fmt.Fprintf(p.output, "  Invalid integer %q, try again.\n", input)
			continue
		}
		if val <= 0 {
			fmt.Fprintf(p.output, "  Value must be > 0, try again.\n")
			continue
		}
		return val
	}
}

func (p *prompter) promptNewNonNegativeIntField(name string) int {
	for {
		fmt.Fprintf(p.output, "  %s (must be >= 0): ", name)
		input := p.readLine()
		if input == "" {
			return 0
		}
		val, err := strconv.Atoi(input)
		if err != nil {
			fmt.Fprintf(p.output, "  Invalid integer %q, try again.\n", input)
			continue
		}
		if val < 0 {
			fmt.Fprintf(p.output, "  Value must be >= 0, try again.\n")
			continue
		}
		return val
	}
}

// removeByIndex is a generic helper for removing an element by index from a slice.
// It uses type parameters to work with any slice type.
func removeByIndex[T any](p *prompter, label string, items []T) []T {
	if len(items) == 0 {
		fmt.Fprintf(p.output, "  No %s entries to remove.\n", label)
		return items
	}
	fmt.Fprintf(p.output, "  Index to remove: ")
	input := p.readLine()
	idx, err := strconv.Atoi(input)
	if err != nil || idx < 0 || idx >= len(items) {
		fmt.Fprintf(p.output, "  Invalid index.\n")
		return items
	}
	return append(items[:idx], items[idx+1:]...)
}
