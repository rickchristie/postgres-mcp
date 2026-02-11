package pgmcp

import (
	"context"
	"time"
)

// Config is the base configuration used by library mode via New().
type Config struct {
	Pool                      PoolConfig         `json:"pool"`
	Protection                ProtectionConfig   `json:"protection"`
	Query                     QueryConfig        `json:"query"`
	ErrorPrompts              []ErrorPromptRule  `json:"error_prompts"`
	Sanitization              []SanitizationRule `json:"sanitization"`
	ReadOnly                  bool               `json:"read_only"`
	Timezone                  string             `json:"timezone"`
	DefaultHookTimeoutSeconds int                `json:"default_hook_timeout_seconds"`

	// Library mode: Go function hooks (not serializable).
	// Mutually exclusive with ServerConfig.ServerHooks.
	BeforeQueryHooks []BeforeQueryHookEntry `json:"-"`
	AfterQueryHooks  []AfterQueryHookEntry  `json:"-"`
}

// ServerConfig embeds Config and adds server-only fields for CLI mode.
type ServerConfig struct {
	Config
	Connection  ConnectionConfig  `json:"connection"`
	Server      ServerSettings    `json:"server"`
	Logging     LoggingConfig     `json:"logging"`
	ServerHooks ServerHooksConfig `json:"server_hooks"`
}

// ConnectionConfig holds database connection parameters used by CLI mode.
type ConnectionConfig struct {
	Host    string `json:"host"`
	Port    int    `json:"port"`
	DBName  string `json:"dbname"`
	SSLMode string `json:"sslmode"`
}

// PoolConfig holds connection pool settings.
type PoolConfig struct {
	MaxConns          int    `json:"max_conns"`
	MinConns          int    `json:"min_conns"`
	MaxConnLifetime   string `json:"max_conn_lifetime"`
	MaxConnIdleTime   string `json:"max_conn_idle_time"`
	HealthCheckPeriod string `json:"health_check_period"`
}

// ServerSettings holds HTTP server settings for CLI mode.
type ServerSettings struct {
	Port               int    `json:"port"`
	HealthCheckEnabled bool   `json:"health_check_enabled"`
	HealthCheckPath    string `json:"health_check_path"`
}

// LoggingConfig holds logging settings for CLI mode.
type LoggingConfig struct {
	Level  string `json:"level"`  // debug, info, warn, error
	Format string `json:"format"` // json, text
	Output string `json:"output"` // stdout, or file path
}

// ProtectionConfig controls which SQL operations are allowed.
// All fields default to false (blocked). Set to true to allow.
type ProtectionConfig struct {
	AllowSet                bool `json:"allow_set"`
	AllowDrop               bool `json:"allow_drop"`
	AllowTruncate           bool `json:"allow_truncate"`
	AllowDo                 bool `json:"allow_do"`
	AllowCopyFrom           bool `json:"allow_copy_from"`
	AllowCopyTo             bool `json:"allow_copy_to"`
	AllowCreateFunction     bool `json:"allow_create_function"`
	AllowPrepare            bool `json:"allow_prepare"`
	AllowDeleteWithoutWhere bool `json:"allow_delete_without_where"`
	AllowUpdateWithoutWhere bool `json:"allow_update_without_where"`
	AllowAlterSystem        bool `json:"allow_alter_system"`
	AllowMerge              bool `json:"allow_merge"`
	AllowGrantRevoke        bool `json:"allow_grant_revoke"`
	AllowManageRoles        bool `json:"allow_manage_roles"`
	AllowCreateExtension    bool `json:"allow_create_extension"`
	AllowLockTable          bool `json:"allow_lock_table"`
	AllowListenNotify       bool `json:"allow_listen_notify"`
	AllowMaintenance        bool `json:"allow_maintenance"`
	AllowDDL                bool `json:"allow_ddl"`
	AllowDiscard            bool `json:"allow_discard"`
	AllowComment            bool `json:"allow_comment"`
	AllowCreateTrigger      bool `json:"allow_create_trigger"`
	AllowCreateRule         bool `json:"allow_create_rule"`
}

// QueryConfig holds query execution settings.
type QueryConfig struct {
	DefaultTimeoutSeconds       int           `json:"default_timeout_seconds"`
	ListTablesTimeoutSeconds    int           `json:"list_tables_timeout_seconds"`
	DescribeTableTimeoutSeconds int           `json:"describe_table_timeout_seconds"`
	MaxSQLLength                int           `json:"max_sql_length"`
	MaxResultLength             int           `json:"max_result_length"`
	TimeoutRules                []TimeoutRule `json:"timeout_rules"`
}

// TimeoutRule maps a SQL pattern to a specific timeout duration.
type TimeoutRule struct {
	Pattern        string `json:"pattern"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

// ErrorPromptRule maps an error message pattern to a guidance message.
type ErrorPromptRule struct {
	Pattern string `json:"pattern"`
	Message string `json:"message"`
}

// SanitizationRule defines a regex-based field sanitization rule.
type SanitizationRule struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
	Description string `json:"description"`
}

// ServerHooksConfig holds command-based hook configuration for CLI mode.
type ServerHooksConfig struct {
	BeforeQuery []HookEntry `json:"before_query"`
	AfterQuery  []HookEntry `json:"after_query"`
}

// HookEntry defines a single command-based hook.
type HookEntry struct {
	Pattern        string   `json:"pattern"`
	Command        string   `json:"command"`
	Args           []string `json:"args"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

// BeforeQueryHook can inspect and modify queries before execution.
type BeforeQueryHook interface {
	Run(ctx context.Context, query string) (string, error)
}

// AfterQueryHook can inspect and modify results after execution.
type AfterQueryHook interface {
	Run(ctx context.Context, result *QueryOutput) (*QueryOutput, error)
}

// BeforeQueryHookEntry wraps a BeforeQueryHook with metadata.
type BeforeQueryHookEntry struct {
	Name    string
	Timeout time.Duration
	Hook    BeforeQueryHook
}

// AfterQueryHookEntry wraps an AfterQueryHook with metadata.
type AfterQueryHookEntry struct {
	Name    string
	Timeout time.Duration
	Hook    AfterQueryHook
}
