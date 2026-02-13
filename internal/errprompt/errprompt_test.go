package errprompt

import (
	"testing"
)

func TestMatchPermissionDenied(t *testing.T) {
	t.Parallel()
	m := NewMatcher([]Rule{
		{Pattern: `(?i)permission denied`, Message: "You do not have sufficient privileges. Ask the user to check table permissions."},
	})
	got := m.Match("permission denied for table users")
	if got == "" {
		t.Fatal("expected a match for permission denied error, got empty string")
	}
	if got != "You do not have sufficient privileges. Ask the user to check table permissions." {
		t.Fatalf("unexpected message: %s", got)
	}
}

func TestMatchRelationNotExist(t *testing.T) {
	t.Parallel()
	m := NewMatcher([]Rule{
		{Pattern: `(?i)relation.*does not exist`, Message: "The table does not exist. Use ListTables to see available tables."},
	})
	got := m.Match(`relation "foo" does not exist`)
	if got == "" {
		t.Fatal("expected a match for relation does not exist error, got empty string")
	}
	if got != "The table does not exist. Use ListTables to see available tables." {
		t.Fatalf("unexpected message: %s", got)
	}
}

func TestNoMatch(t *testing.T) {
	t.Parallel()
	m := NewMatcher([]Rule{
		{Pattern: `(?i)permission denied`, Message: "You do not have sufficient privileges."},
		{Pattern: `(?i)relation.*does not exist`, Message: "The table does not exist."},
	})
	got := m.Match("some other error")
	if got != "" {
		t.Fatalf("expected empty string for non-matching error, got: %s", got)
	}
}

func TestMultipleMatches(t *testing.T) {
	t.Parallel()
	m := NewMatcher([]Rule{
		{Pattern: `(?i)permission denied`, Message: "Check your privileges."},
		{Pattern: `(?i)denied.*table`, Message: "Verify table access grants."},
	})
	got := m.Match("permission denied for table users")
	expected := "Check your privileges.\nVerify table access grants."
	if got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestEmptyRules(t *testing.T) {
	t.Parallel()
	m := NewMatcher([]Rule{})
	got := m.Match("any error at all")
	if got != "" {
		t.Fatalf("expected empty string with no rules, got: %s", got)
	}
}

func TestMatchHookError(t *testing.T) {
	t.Parallel()
	m := NewMatcher([]Rule{
		{Pattern: `(?i)rejected`, Message: "The query was rejected by a hook. Review the hook configuration."},
	})
	got := m.Match("rejected by test hook")
	if got == "" {
		t.Fatal("expected a match for hook rejection error, got empty string")
	}
	if got != "The query was rejected by a hook. Review the hook configuration." {
		t.Fatalf("unexpected message: %s", got)
	}
}

func TestNewMatcherPanicsOnInvalidRegex(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for invalid regex pattern")
		}
	}()
	NewMatcher([]Rule{
		{Pattern: `[invalid`, Message: "should not compile"},
	})
}
