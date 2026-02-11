package timeout

import (
	"fmt"
	"regexp"
	"time"
)

// Rule is the timeout manager's own rule type.
type Rule struct {
	Pattern string
	Timeout time.Duration
}

// Config is the timeout manager's own config type.
type Config struct {
	DefaultTimeout time.Duration
	Rules          []Rule
}

type compiledRule struct {
	pattern *regexp.Regexp
	timeout time.Duration
}

// Manager resolves query timeouts based on SQL pattern matching.
type Manager struct {
	rules          []compiledRule
	defaultTimeout time.Duration
}

// NewManager creates a new Manager. Panics on invalid regex patterns.
func NewManager(config Config) *Manager {
	compiled := make([]compiledRule, len(config.Rules))
	for i, r := range config.Rules {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			panic(fmt.Sprintf("timeout: invalid regex pattern %q: %v", r.Pattern, err))
		}
		compiled[i] = compiledRule{pattern: re, timeout: r.Timeout}
	}
	return &Manager{rules: compiled, defaultTimeout: config.DefaultTimeout}
}

// GetTimeout returns the timeout for the given SQL.
// First matching rule wins. Falls back to default.
func (m *Manager) GetTimeout(sql string) time.Duration {
	for _, rule := range m.rules {
		if rule.pattern.MatchString(sql) {
			return rule.timeout
		}
	}
	return m.defaultTimeout
}
