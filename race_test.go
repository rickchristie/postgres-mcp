package pgmcp_test

import (
	"sync"
	"testing"
	"time"

	"github.com/rickchristie/postgres-mcp/internal/errprompt"
	"github.com/rickchristie/postgres-mcp/internal/protection"
	"github.com/rickchristie/postgres-mcp/internal/sanitize"
	"github.com/rickchristie/postgres-mcp/internal/timeout"
)

func TestRace_ConcurrentSanitization(t *testing.T) {
	s := sanitize.NewSanitizer([]sanitize.Rule{
		{Pattern: `\d{3}-\d{4}`, Replacement: "***-****"},
		{Pattern: `\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`, Replacement: "[REDACTED]"},
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				// Each iteration gets a fresh copy since SanitizeRows mutates in-place.
				rows := []map[string]interface{}{
					{"phone": "555-1234", "email": "test@example.com", "name": "Alice"},
					{"phone": "555-5678", "email": "bob@test.org", "name": "Bob"},
				}
				s.SanitizeRows(rows)
			}
		}()
	}
	wg.Wait()
}

func TestRace_ConcurrentProtectionCheck(t *testing.T) {
	c := protection.NewChecker(protection.Config{})

	queries := []string{
		"SELECT * FROM users",
		"INSERT INTO users (name) VALUES ('test')",
		"UPDATE users SET name = 'test' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"DROP TABLE users",
		"CREATE TABLE foo (id int)",
		"SELECT * FROM users WHERE name = 'test'",
		"EXPLAIN ANALYZE SELECT 1",
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sql := queries[(id+j)%len(queries)]
				_ = c.Check(sql)
			}
		}(i)
	}
	wg.Wait()
}

func TestRace_ConcurrentErrorPrompt(t *testing.T) {
	m := errprompt.NewMatcher([]errprompt.Rule{
		{Pattern: `permission denied`, Message: "You don't have permission."},
		{Pattern: `syntax error`, Message: "Check your SQL syntax."},
		{Pattern: `does not exist`, Message: "The table or column may not exist."},
	})

	errors := []string{
		"permission denied for table users",
		"syntax error at or near SELECT",
		"relation \"foo\" does not exist",
		"column \"bar\" does not exist",
		"connection refused",
		"timeout expired",
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				errMsg := errors[(id+j)%len(errors)]
				_ = m.Match(errMsg)
			}
		}(i)
	}
	wg.Wait()
}

func TestRace_ConcurrentTimeout(t *testing.T) {
	m := timeout.NewManager(timeout.Config{
		DefaultTimeout: 30 * time.Second,
		Rules: []timeout.Rule{
			{Pattern: `(?i)SELECT.*pg_sleep`, Timeout: 60 * time.Second},
			{Pattern: `(?i)INSERT`, Timeout: 10 * time.Second},
			{Pattern: `(?i)DELETE`, Timeout: 15 * time.Second},
		},
	})

	queries := []string{
		"SELECT pg_sleep(1)",
		"INSERT INTO users (name) VALUES ('test')",
		"DELETE FROM users WHERE id = 1",
		"SELECT * FROM users",
		"UPDATE users SET name = 'test'",
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sql := queries[(id+j)%len(queries)]
				_ = m.GetTimeout(sql)
			}
		}(i)
	}
	wg.Wait()
}
