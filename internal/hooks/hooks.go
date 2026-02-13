package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Config is the hook runner's own config type.
type Config struct {
	DefaultTimeout time.Duration
	BeforeQuery    []HookEntry
	AfterQuery     []HookEntry
}

// HookEntry defines a single command-based hook.
type HookEntry struct {
	Pattern string
	Command string
	Args    []string
	Timeout time.Duration // 0 means use DefaultTimeout
}

// BeforeQueryResult is the JSON response from a before_query hook.
type BeforeQueryResult struct {
	Accept        bool   `json:"accept"`
	ModifiedQuery string `json:"modified_query,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
}

// AfterQueryResult is the JSON response from an after_query hook.
type AfterQueryResult struct {
	Accept         bool   `json:"accept"`
	ModifiedResult string `json:"modified_result,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
}

type compiledHook struct {
	pattern *regexp.Regexp
	command string
	args    []string
	timeout time.Duration
}

// Runner executes command-based hooks.
type Runner struct {
	beforeQuery    []compiledHook
	afterQuery     []compiledHook
	defaultTimeout time.Duration
	logger         zerolog.Logger
}

// NewRunner creates a new Runner. Panics on invalid regex or invalid config.
func NewRunner(config Config, logger zerolog.Logger) *Runner {
	if config.DefaultTimeout == 0 && (len(config.BeforeQuery) > 0 || len(config.AfterQuery) > 0) {
		panic("hooks: default_hook_timeout_seconds must be > 0 when hooks are configured")
	}

	compile := func(entries []HookEntry) []compiledHook {
		compiled := make([]compiledHook, len(entries))
		for i, e := range entries {
			re, err := regexp.Compile(e.Pattern)
			if err != nil {
				panic(fmt.Sprintf("hooks: invalid regex pattern %q: %v", e.Pattern, err))
			}
			timeout := e.Timeout
			if timeout == 0 {
				timeout = config.DefaultTimeout
			}
			compiled[i] = compiledHook{
				pattern: re,
				command: e.Command,
				args:    e.Args,
				timeout: timeout,
			}
		}
		return compiled
	}

	return &Runner{
		beforeQuery:    compile(config.BeforeQuery),
		afterQuery:     compile(config.AfterQuery),
		defaultTimeout: config.DefaultTimeout,
		logger:         logger,
	}
}

// HasAfterQueryHooks returns true if any AfterQuery hooks are configured.
func (r *Runner) HasAfterQueryHooks() bool {
	return len(r.afterQuery) > 0
}

// RunBeforeQuery runs matching BeforeQuery hooks in middleware chain.
func (r *Runner) RunBeforeQuery(ctx context.Context, query string) (string, error) {
	current := query
	for _, hook := range r.beforeQuery {
		if !hook.pattern.MatchString(current) {
			continue
		}
		output, err := r.executeHook(ctx, hook, current)
		if err != nil {
			return "", fmt.Errorf("before_query hook error: %w", err)
		}

		var result BeforeQueryResult
		if err := json.Unmarshal(output, &result); err != nil {
			return "", fmt.Errorf("before_query hook returned unparseable response (command: %s): %w", hook.command, err)
		}

		if !result.Accept {
			errMsg := "query rejected by hook"
			if result.ErrorMessage != "" {
				errMsg = result.ErrorMessage
			}
			return "", errors.New(errMsg)
		}
		if result.ModifiedQuery != "" {
			current = result.ModifiedQuery
		}
	}
	return current, nil
}

// RunAfterQuery runs matching AfterQuery hooks in middleware chain.
func (r *Runner) RunAfterQuery(ctx context.Context, resultJSON string) (string, error) {
	current := resultJSON
	for _, hook := range r.afterQuery {
		if !hook.pattern.MatchString(current) {
			continue
		}
		output, err := r.executeHook(ctx, hook, current)
		if err != nil {
			return "", fmt.Errorf("after_query hook error: %w", err)
		}

		var result AfterQueryResult
		if err := json.Unmarshal(output, &result); err != nil {
			return "", fmt.Errorf("after_query hook returned unparseable response (command: %s): %w", hook.command, err)
		}

		if !result.Accept {
			errMsg := "result rejected by hook"
			if result.ErrorMessage != "" {
				errMsg = result.ErrorMessage
			}
			return "", errors.New(errMsg)
		}
		if result.ModifiedResult != "" {
			current = result.ModifiedResult
		}
	}
	return current, nil
}

func (r *Runner) executeHook(ctx context.Context, hook compiledHook, input string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, hook.timeout)
	defer cancel()

	// Command and args are passed separately — no shell interpretation.
	// exec.Command(name, args...) executes the binary directly.
	cmd := exec.CommandContext(ctx, hook.command, hook.args...)
	cmd.Stdin = strings.NewReader(input)

	// Capture stderr separately for logging. Stdout is the JSON response.
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	output, err := cmd.Output()
	if err != nil {
		// Log stderr for debugging — stderr may contain diagnostic info from the hook.
		if stderr.Len() > 0 {
			r.logger.Warn().Str("command", hook.command).Str("stderr", stderr.String()).Msg("hook stderr output")
		}
		// Hooks are critical guardrails — any failure stops the pipeline.
		// This covers: non-zero exit code, crash, timeout (context deadline exceeded).
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("hook timed out: %s", hook.command)
		}
		return nil, fmt.Errorf("hook failed (command: %s): %w", hook.command, err)
	}
	// Log stderr even on success — hooks may emit warnings or debug info.
	if stderr.Len() > 0 {
		r.logger.Debug().Str("command", hook.command).Str("stderr", stderr.String()).Msg("hook stderr output")
	}
	return output, nil
}
