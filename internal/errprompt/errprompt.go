package errprompt

import (
	"fmt"
	"regexp"
	"strings"
)

// Rule is the error prompt matcher's own rule type.
type Rule struct {
	Pattern string
	Message string
}

type compiledRule struct {
	pattern *regexp.Regexp
	message string
}

// Matcher checks error messages against patterns and returns guidance prompts.
type Matcher struct {
	rules []compiledRule
}

// NewMatcher creates a new Matcher. Returns an error on invalid regex patterns.
func NewMatcher(rules []Rule) (*Matcher, error) {
	compiled := make([]compiledRule, len(rules))
	for i, r := range rules {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return nil, fmt.Errorf("errprompt: invalid regex pattern %q: %v", r.Pattern, err)
		}
		compiled[i] = compiledRule{pattern: re, message: r.Message}
	}
	return &Matcher{rules: compiled}, nil
}

// Match checks error message against all rules (top to bottom).
// Returns all matching prompt messages joined with newline separators.
// Returns empty string if no match.
func (m *Matcher) Match(errMsg string) string {
	var matches []string
	for _, rule := range m.rules {
		if rule.pattern.MatchString(errMsg) {
			matches = append(matches, rule.message)
		}
	}
	return strings.Join(matches, "\n")
}

// MatchedPatterns returns the regex patterns that matched the given error message.
// Returns nil if no match.
func (m *Matcher) MatchedPatterns(errMsg string) []string {
	var patterns []string
	for _, rule := range m.rules {
		if rule.pattern.MatchString(errMsg) {
			patterns = append(patterns, rule.pattern.String())
		}
	}
	return patterns
}
