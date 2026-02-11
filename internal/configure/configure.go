package configure

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	cfg := loadExisting(configPath)

	p := &prompter{
		scanner: scanner,
		output:  output,
	}

	fmt.Fprintf(output, "gopgmcp configuration wizard\n")
	fmt.Fprintf(output, "Config file: %s\n\n", configPath)

	// Connection
	fmt.Fprintf(output, "=== Connection ===\n")
	cfg.Connection.Host = p.promptString("connection.host", cfg.Connection.Host)
	cfg.Connection.Port = p.promptInt("connection.port", cfg.Connection.Port)
	cfg.Connection.DBName = p.promptString("connection.dbname", cfg.Connection.DBName)
	cfg.Connection.SSLMode = p.promptString("connection.sslmode", cfg.Connection.SSLMode)

	// Server
	fmt.Fprintf(output, "\n=== Server ===\n")
	cfg.Server.Port = p.promptInt("server.port", cfg.Server.Port)
	cfg.Server.HealthCheckEnabled = p.promptBool("server.health_check_enabled", cfg.Server.HealthCheckEnabled)
	cfg.Server.HealthCheckPath = p.promptString("server.health_check_path", cfg.Server.HealthCheckPath)

	// Logging
	fmt.Fprintf(output, "\n=== Logging ===\n")
	cfg.Logging.Level = p.promptString("logging.level", cfg.Logging.Level)
	cfg.Logging.Format = p.promptString("logging.format", cfg.Logging.Format)
	cfg.Logging.Output = p.promptString("logging.output", cfg.Logging.Output)

	// Pool
	fmt.Fprintf(output, "\n=== Pool ===\n")
	cfg.Pool.MaxConns = p.promptInt("pool.max_conns", cfg.Pool.MaxConns)
	cfg.Pool.MinConns = p.promptInt("pool.min_conns", cfg.Pool.MinConns)
	cfg.Pool.MaxConnLifetime = p.promptString("pool.max_conn_lifetime", cfg.Pool.MaxConnLifetime)
	cfg.Pool.MaxConnIdleTime = p.promptString("pool.max_conn_idle_time", cfg.Pool.MaxConnIdleTime)
	cfg.Pool.HealthCheckPeriod = p.promptString("pool.health_check_period", cfg.Pool.HealthCheckPeriod)

	// Query
	fmt.Fprintf(output, "\n=== Query ===\n")
	cfg.Query.DefaultTimeoutSeconds = p.promptInt("query.default_timeout_seconds", cfg.Query.DefaultTimeoutSeconds)
	cfg.Query.ListTablesTimeoutSeconds = p.promptInt("query.list_tables_timeout_seconds", cfg.Query.ListTablesTimeoutSeconds)
	cfg.Query.DescribeTableTimeoutSeconds = p.promptInt("query.describe_table_timeout_seconds", cfg.Query.DescribeTableTimeoutSeconds)
	cfg.Query.MaxSQLLength = p.promptInt("query.max_sql_length", cfg.Query.MaxSQLLength)
	cfg.Query.MaxResultLength = p.promptInt("query.max_result_length", cfg.Query.MaxResultLength)

	// Read-only and misc
	fmt.Fprintf(output, "\n=== General ===\n")
	cfg.ReadOnly = p.promptBool("read_only", cfg.ReadOnly)
	cfg.Timezone = p.promptString("timezone", cfg.Timezone)
	cfg.DefaultHookTimeoutSeconds = p.promptInt("default_hook_timeout_seconds", cfg.DefaultHookTimeoutSeconds)

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

func loadExisting(configPath string) *pgmcp.ServerConfig {
	cfg := &pgmcp.ServerConfig{}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg
	}
	// Ignore unmarshal errors — start with whatever was parseable.
	_ = json.Unmarshal(data, cfg)
	return cfg
}

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
}

func (p *prompter) readLine() string {
	if p.scanner.Scan() {
		return strings.TrimSpace(p.scanner.Text())
	}
	return ""
}

func (p *prompter) promptString(field string, current string) string {
	fmt.Fprintf(p.output, "%s (current: %q): ", field, current)
	input := p.readLine()
	if input == "" {
		return current
	}
	return input
}

func (p *prompter) promptInt(field string, current int) int {
	fmt.Fprintf(p.output, "%s (current: %d): ", field, current)
	input := p.readLine()
	if input == "" {
		return current
	}
	val, err := strconv.Atoi(input)
	if err != nil {
		fmt.Fprintf(p.output, "  Invalid integer, keeping current value.\n")
		return current
	}
	return val
}

func (p *prompter) promptBool(field string, current bool) bool {
	fmt.Fprintf(p.output, "%s (current: %v): ", field, current)
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
		fmt.Fprintf(p.output, "  Invalid boolean, keeping current value.\n")
		return current
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
			pattern := p.promptNewField("pattern")
			timeout := p.promptNewIntField("timeout_seconds")
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
			pattern := p.promptNewField("pattern")
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
			pattern := p.promptNewField("pattern")
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
			pattern := p.promptNewField("pattern")
			command := p.promptNewField("command")
			argsStr := p.promptNewField("args (comma-separated)")
			var args []string
			if argsStr != "" {
				for _, a := range strings.Split(argsStr, ",") {
					args = append(args, strings.TrimSpace(a))
				}
			}
			timeout := p.promptNewIntField("timeout_seconds")
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

func (p *prompter) promptNewIntField(name string) int {
	fmt.Fprintf(p.output, "  %s: ", name)
	input := p.readLine()
	if input == "" {
		return 0
	}
	val, err := strconv.Atoi(input)
	if err != nil {
		fmt.Fprintf(p.output, "  Invalid integer, using 0.\n")
		return 0
	}
	return val
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
