package timeout

import (
	"strings"
	"testing"
	"time"
)

func TestMatchFirstRule(t *testing.T) {
	t.Parallel()
	m := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: "pg_stat", Timeout: 5 * time.Second},
			{Pattern: "JOIN", Timeout: 60 * time.Second},
		},
	})

	got := m.GetTimeout("SELECT * FROM pg_stat_activity")
	if got != 5*time.Second {
		t.Errorf("expected 5s, got %v", got)
	}
}

func TestStopOnFirstMatch(t *testing.T) {
	t.Parallel()
	m := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: "pg_stat", Timeout: 5 * time.Second},
			{Pattern: "JOIN", Timeout: 60 * time.Second},
		},
	})

	got := m.GetTimeout("SELECT * FROM pg_stat JOIN x JOIN y JOIN z")
	if got != 5*time.Second {
		t.Errorf("expected 5s (first match wins), got %v", got)
	}
}

func TestDefaultTimeout(t *testing.T) {
	t.Parallel()
	m := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: "pg_stat", Timeout: 5 * time.Second},
			{Pattern: "JOIN", Timeout: 60 * time.Second},
		},
	})

	got := m.GetTimeout("SELECT 1")
	if got != 30*time.Second {
		t.Errorf("expected 30s (default), got %v", got)
	}
}

func TestNoRules(t *testing.T) {
	t.Parallel()
	m := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules:          []Rule{},
	})

	got := m.GetTimeout("SELECT 1")
	if got != 30*time.Second {
		t.Errorf("expected 30s (default), got %v", got)
	}
}

func TestNewManagerPanicsOnInvalidRegex(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for invalid regex pattern")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "invalid regex pattern") {
			t.Fatalf("expected panic message to contain 'invalid regex pattern', got: %s", msg)
		}
		if !strings.Contains(msg, "[invalid") {
			t.Fatalf("expected panic message to contain the invalid pattern, got: %s", msg)
		}
	}()
	NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: `[invalid`, Timeout: 5 * time.Second},
		},
	})
}
