package pgmcp

import (
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestRequestLength_WithArguments(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      "query",
			Arguments: map[string]any{"sql": "SELECT 1"},
		},
	}
	length := requestLength(req)
	// {"sql":"SELECT 1"} = 18 bytes
	if length != 18 {
		t.Fatalf("expected request length 18, got %d", length)
	}
}

func TestRequestLength_NoArguments(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name: "list_tables",
		},
	}
	length := requestLength(req)
	if length != 0 {
		t.Fatalf("expected request length 0 for no arguments, got %d", length)
	}
}

func TestRequestLength_EmptyArguments(t *testing.T) {
	t.Parallel()
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      "query",
			Arguments: map[string]any{},
		},
	}
	length := requestLength(req)
	if length != 0 {
		t.Fatalf("expected request length 0 for empty arguments, got %d", length)
	}
}

func TestResultLength_TextResult(t *testing.T) {
	t.Parallel()
	result := mcp.NewToolResultText(`{"columns":["id"],"rows":[]}`)
	length := resultLength(result)
	if length != 28 {
		t.Fatalf("expected result length 28, got %d", length)
	}
}

func TestResultLength_ErrorResult(t *testing.T) {
	t.Parallel()
	result := mcp.NewToolResultError("something failed")
	length := resultLength(result)
	if length != 16 {
		t.Fatalf("expected result length 16, got %d", length)
	}
}

func TestResultLength_NilResult(t *testing.T) {
	t.Parallel()
	length := resultLength(nil)
	if length != 0 {
		t.Fatalf("expected result length 0 for nil, got %d", length)
	}
}
