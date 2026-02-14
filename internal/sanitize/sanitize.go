package sanitize

import (
	"fmt"
	"regexp"
)

// Rule is the sanitizer's own rule type.
type Rule struct {
	Pattern     string
	Replacement string
}

type compiledRule struct {
	pattern     *regexp.Regexp
	replacement string
}

// Sanitizer applies regex-based sanitization to result row field values.
type Sanitizer struct {
	rules []compiledRule
}

// NewSanitizer creates a new Sanitizer. Returns an error on invalid regex patterns.
func NewSanitizer(rules []Rule) (*Sanitizer, error) {
	compiled := make([]compiledRule, len(rules))
	for i, r := range rules {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return nil, fmt.Errorf("sanitize: invalid regex pattern %q: %v", r.Pattern, err)
		}
		compiled[i] = compiledRule{pattern: re, replacement: r.Replacement}
	}
	return &Sanitizer{rules: compiled}, nil
}

// HasRules returns true if the sanitizer has any rules configured.
func (s *Sanitizer) HasRules() bool {
	return len(s.rules) > 0
}

// SanitizeRows applies sanitization to each field value in the result rows.
// For JSONB/array fields (map[string]interface{}, []interface{}),
// recurses into primitive values.
func (s *Sanitizer) SanitizeRows(rows []map[string]interface{}) []map[string]interface{} {
	for _, row := range rows {
		for k, v := range row {
			row[k] = s.sanitizeValue(v)
		}
	}
	return rows
}

func (s *Sanitizer) sanitizeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case string:
		result := val
		for _, rule := range s.rules {
			result = rule.pattern.ReplaceAllString(result, rule.replacement)
		}
		return result
	case map[string]interface{}:
		for k, v := range val {
			val[k] = s.sanitizeValue(v)
		}
		return val
	case []interface{}:
		for i, item := range val {
			val[i] = s.sanitizeValue(item)
		}
		return val
	default:
		// Numeric, bool, nil, json.Number — return as-is.
		// json.Number (from UseNumber()) is type `string` underneath but does NOT
		// match `case string:` in Go type switches, so it correctly passes through.
		return v
	}
}
