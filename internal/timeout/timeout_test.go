package timeout

import (
	"testing"
	"time"
)

func TestMatchFirstRule(t *testing.T) {
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
	m := NewManager(Config{
		DefaultTimeout: 30 * time.Second,
		Rules:          []Rule{},
	})

	got := m.GetTimeout("SELECT 1")
	if got != 30*time.Second {
		t.Errorf("expected 30s (default), got %v", got)
	}
}
