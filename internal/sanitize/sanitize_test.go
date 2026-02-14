package sanitize

import (
	"encoding/json"
	"strings"
	"testing"
)

var phoneRule = Rule{
	Pattern:     `(\+\d{2})\d+(\d{3})`,
	Replacement: "${1}xxx${2}",
}

var ktpRule = Rule{
	Pattern:     `(\d{4})\d{8}(\d{4})`,
	Replacement: "${1}xxxxxxxx${2}",
}

func TestSanitizePhoneNumber(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue("+62821233447")
	if result != "+62xxx447" {
		t.Fatalf("expected +62xxx447, got %v", result)
	}
}

func TestSanitizeKTP(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{ktpRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue("3201234567890001")
	if result != "3201xxxxxxxx0001" {
		t.Fatalf("expected 3201xxxxxxxx0001, got %v", result)
	}
}

func TestNoMatch(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue("hello world")
	if result != "hello world" {
		t.Fatalf("expected hello world, got %v", result)
	}
}

func TestMultipleRulesOrdering(t *testing.T) {
	t.Parallel()
	// First rule masks phone number, second rule replaces "xxx" with "***".
	rules := []Rule{
		phoneRule,
		{Pattern: `xxx`, Replacement: "***"},
	}
	s, err := NewSanitizer(rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue("+62821233447")
	// After phone rule: "+62xxx447"
	// After second rule: "+62***447"
	if result != "+62***447" {
		t.Fatalf("expected +62***447, got %v", result)
	}
}

func TestSanitizeJSONBField(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	input := map[string]interface{}{
		"phone": "+62821233447",
	}
	result := s.sanitizeValue(input)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["phone"] != "+62xxx447" {
		t.Fatalf("expected +62xxx447, got %v", m["phone"])
	}
}

func TestSanitizeNestedJSONB(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	input := map[string]interface{}{
		"contact": map[string]interface{}{
			"phone": "+62821233447",
		},
	}
	result := s.sanitizeValue(input)
	m := result.(map[string]interface{})
	contact := m["contact"].(map[string]interface{})
	if contact["phone"] != "+62xxx447" {
		t.Fatalf("expected +62xxx447, got %v", contact["phone"])
	}
}

func TestSanitizeArrayField(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	input := []interface{}{"+62821233447", "+62899887766"}
	result := s.sanitizeValue(input)
	arr := result.([]interface{})
	if arr[0] != "+62xxx447" {
		t.Fatalf("expected +62xxx447 for first element, got %v", arr[0])
	}
	if arr[1] != "+62xxx766" {
		t.Fatalf("expected +62xxx766 for second element, got %v", arr[1])
	}
}

func TestSanitizeNullField(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue(nil)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestSanitizeNumericField(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue(int64(12345))
	if result != int64(12345) {
		t.Fatalf("expected 12345, got %v", result)
	}
}

func TestSanitizeJsonNumber(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	input := json.Number("9007199254740993")
	result := s.sanitizeValue(input)
	jn, ok := result.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number, got %T", result)
	}
	if jn.String() != "9007199254740993" {
		t.Fatalf("expected 9007199254740993, got %v", jn)
	}
}

func TestSanitizeBooleanField(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue(true)
	if result != true {
		t.Fatalf("expected true, got %v", result)
	}
}

func TestSanitizeEmptyRules(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := s.sanitizeValue("+62821233447")
	if result != "+62821233447" {
		t.Fatalf("expected unchanged value, got %v", result)
	}
}

func TestSanitizeRows(t *testing.T) {
	t.Parallel()
	s, err := NewSanitizer([]Rule{phoneRule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rows := []map[string]interface{}{
		{
			"name":  "Alice",
			"phone": "+62821233447",
			"age":   int64(30),
			"active": true,
			"extra": nil,
		},
		{
			"name":  "Bob",
			"phone": "+62899887766",
			"age":   int64(25),
			"active": false,
			"data":  json.Number("42"),
		},
	}

	result := s.SanitizeRows(rows)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}

	// Row 0: phone sanitized, others unchanged
	if result[0]["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", result[0]["name"])
	}
	if result[0]["phone"] != "+62xxx447" {
		t.Fatalf("expected +62xxx447, got %v", result[0]["phone"])
	}
	if result[0]["age"] != int64(30) {
		t.Fatalf("expected 30, got %v", result[0]["age"])
	}
	if result[0]["active"] != true {
		t.Fatalf("expected true, got %v", result[0]["active"])
	}
	if result[0]["extra"] != nil {
		t.Fatalf("expected nil, got %v", result[0]["extra"])
	}

	// Row 1: phone sanitized, others unchanged
	if result[1]["name"] != "Bob" {
		t.Fatalf("expected Bob, got %v", result[1]["name"])
	}
	if result[1]["phone"] != "+62xxx766" {
		t.Fatalf("expected +62xxx766, got %v", result[1]["phone"])
	}
	if result[1]["age"] != int64(25) {
		t.Fatalf("expected 25, got %v", result[1]["age"])
	}
	if result[1]["active"] != false {
		t.Fatalf("expected false, got %v", result[1]["active"])
	}
	jn, ok := result[1]["data"].(json.Number)
	if !ok {
		t.Fatalf("expected json.Number, got %T", result[1]["data"])
	}
	if jn.String() != "42" {
		t.Fatalf("expected 42, got %v", jn)
	}
}

func TestNewSanitizerErrorsOnInvalidRegex(t *testing.T) {
	t.Parallel()
	_, err := NewSanitizer([]Rule{
		{Pattern: `[invalid`, Replacement: "x"},
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
