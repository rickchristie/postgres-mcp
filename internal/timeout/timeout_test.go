package timeout

import (
	"strings"
	"testing"
	"time"
)

func TestMatchFirstRule(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: "pg_stat", Timeout: 5 * time.Second},
			{Pattern: "JOIN", Timeout: 60 * time.Second},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := m.GetTimeout("SELECT * FROM pg_stat_activity")
	if got != 5*time.Second {
		t.Errorf("expected 5s, got %v", got)
	}
}

func TestStopOnFirstMatch(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: "pg_stat", Timeout: 5 * time.Second},
			{Pattern: "JOIN", Timeout: 60 * time.Second},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := m.GetTimeout("SELECT * FROM pg_stat JOIN x JOIN y JOIN z")
	if got != 5*time.Second {
		t.Errorf("expected 5s (first match wins), got %v", got)
	}
}

func TestDefaultTimeout(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: "pg_stat", Timeout: 5 * time.Second},
			{Pattern: "JOIN", Timeout: 60 * time.Second},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := m.GetTimeout("SELECT 1")
	if got != 30*time.Second {
		t.Errorf("expected 30s (default), got %v", got)
	}
}

func TestNoRules(t *testing.T) {
	t.Parallel()
	m, err := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules:          []Rule{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := m.GetTimeout("SELECT 1")
	if got != 30*time.Second {
		t.Errorf("expected 30s (default), got %v", got)
	}
}

func TestNewManagerErrorsOnInvalidRegex(t *testing.T) {
	t.Parallel()
	_, err := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []Rule{
			{Pattern: `[invalid`, Timeout: 5 * time.Second},
		},
	})
	if err == nil {
		t.Fatal("expected error for invalid regex pattern")
	}
	if !strings.Contains(err.Error(), "invalid regex pattern") {
		t.Fatalf("expected error to contain 'invalid regex pattern', got: %s", err)
	}
	if !strings.Contains(err.Error(), "[invalid") {
		t.Fatalf("expected error to contain the invalid pattern, got: %s", err)
	}
}
