package pgmcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	pgmcp "github.com/rickchristie/postgres-mcp"

	"github.com/mark3labs/mcp-go/server"
)

// mcpTestServer bundles everything needed for an MCP HTTP server test.
type mcpTestServer struct {
	pgMcp      *pgmcp.PostgresMcp
	port       int
	baseURL    string
	httpServer *server.StreamableHTTPServer
}

// startMCPTestServer creates a PostgresMcp instance, registers MCP tools,
// starts an HTTP server on a free port, and returns the test server.
// The optional healthCheckPath enables the health check endpoint.
func startMCPTestServer(t *testing.T, config pgmcp.Config, healthCheckPath string) *mcpTestServer {
	t.Helper()

	p, _ := newTestInstance(t, config)

	// Find a free port.
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	mcpServer := server.NewMCPServer("gopgmcp-test", "1.0.0",
		server.WithToolCapabilities(true),
	)
	pgmcp.RegisterMCPTools(mcpServer, p)

	addr := fmt.Sprintf(":%d", port)
	mux := http.NewServeMux()

	if healthCheckPath != "" {
		mux.HandleFunc(healthCheckPath, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
	}

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	streamableServer := server.NewStreamableHTTPServer(mcpServer,
		server.WithEndpointPath("/mcp"),
		server.WithStateLess(true),
		server.WithStreamableHTTPServer(httpSrv),
	)

	// Manually register MCP handler.
	mux.Handle("/mcp", streamableServer)

	go func() {
		if err := streamableServer.Start(addr); err != nil && err != http.ErrServerClosed {
			t.Logf("server error: %v", err)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	t.Cleanup(func() { streamableServer.Shutdown(context.Background()) })

	return &mcpTestServer{
		pgMcp:      p,
		port:       port,
		baseURL:    fmt.Sprintf("http://localhost:%d", port),
		httpServer: streamableServer,
	}
}

// jsonRPC sends a JSON-RPC request to the MCP endpoint and returns the parsed response.
func (s *mcpTestServer) jsonRPC(t *testing.T, method string, params interface{}) map[string]interface{} {
	t.Helper()

	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
	}
	if params != nil {
		reqBody["params"] = params
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	resp, err := http.Post(
		s.baseURL+"/mcp",
		"application/json",
		strings.NewReader(string(bodyBytes)),
	)
	if err != nil {
		t.Fatalf("JSON-RPC request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response JSON: %v; body: %s", err, string(respBody))
	}

	return result
}

func TestMCPServer_QueryTool(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	s := startMCPTestServer(t, config, "")

	// Setup: create table and insert data via the Go API.
	setupTable(t, s.pgMcp, "CREATE TABLE mcp_test_query (id serial PRIMARY KEY, name text)")
	setupTable(t, s.pgMcp, "INSERT INTO mcp_test_query (name) VALUES ('alice'), ('bob')")

	// Call query tool via JSON-RPC.
	result := s.jsonRPC(t, "tools/call", map[string]interface{}{
		"name": "query",
		"arguments": map[string]interface{}{
			"sql": "SELECT id, name FROM mcp_test_query ORDER BY id",
		},
	})

	// Verify response structure.
	resultObj, ok := result["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected result object, got %T: %v", result["result"], result["result"])
	}

	content, ok := resultObj["content"].([]interface{})
	if !ok || len(content) == 0 {
		t.Fatalf("expected content array, got %v", resultObj["content"])
	}

	// The content should be a text result with JSON output.
	firstContent := content[0].(map[string]interface{})
	if firstContent["type"] != "text" {
		t.Fatalf("expected content type 'text', got %q", firstContent["type"])
	}

	// Parse the text to verify it contains query results.
	var queryOutput pgmcp.QueryOutput
	if err := json.Unmarshal([]byte(firstContent["text"].(string)), &queryOutput); err != nil {
		t.Fatalf("failed to parse query output: %v", err)
	}

	if len(queryOutput.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(queryOutput.Rows))
	}
	if queryOutput.Rows[0]["name"] != "alice" {
		t.Fatalf("expected 'alice', got %v", queryOutput.Rows[0]["name"])
	}
}

func TestMCPServer_ListTablesTool(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	s := startMCPTestServer(t, config, "")

	// Setup: create tables.
	setupTable(t, s.pgMcp, "CREATE TABLE mcp_test_lt1 (id serial PRIMARY KEY)")
	setupTable(t, s.pgMcp, "CREATE TABLE mcp_test_lt2 (id serial PRIMARY KEY)")

	result := s.jsonRPC(t, "tools/call", map[string]interface{}{
		"name":      "list_tables",
		"arguments": map[string]interface{}{},
	})

	resultObj := result["result"].(map[string]interface{})
	content := resultObj["content"].([]interface{})
	firstContent := content[0].(map[string]interface{})

	var listOutput pgmcp.ListTablesOutput
	if err := json.Unmarshal([]byte(firstContent["text"].(string)), &listOutput); err != nil {
		t.Fatalf("failed to parse list tables output: %v", err)
	}

	names := map[string]bool{}
	for _, tbl := range listOutput.Tables {
		names[tbl.Name] = true
	}
	if !names["mcp_test_lt1"] || !names["mcp_test_lt2"] {
		t.Fatalf("expected mcp_test_lt1 and mcp_test_lt2 in list, got %v", names)
	}
}

func TestMCPServer_DescribeTableTool(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	s := startMCPTestServer(t, config, "")

	setupTable(t, s.pgMcp, "CREATE TABLE mcp_test_dt (id serial PRIMARY KEY, name text NOT NULL, email text)")

	result := s.jsonRPC(t, "tools/call", map[string]interface{}{
		"name": "describe_table",
		"arguments": map[string]interface{}{
			"table": "mcp_test_dt",
		},
	})

	resultObj := result["result"].(map[string]interface{})
	content := resultObj["content"].([]interface{})
	firstContent := content[0].(map[string]interface{})

	var descOutput pgmcp.DescribeTableOutput
	if err := json.Unmarshal([]byte(firstContent["text"].(string)), &descOutput); err != nil {
		t.Fatalf("failed to parse describe table output: %v", err)
	}

	if descOutput.Name != "mcp_test_dt" {
		t.Fatalf("expected table name 'mcp_test_dt', got %q", descOutput.Name)
	}
	if len(descOutput.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(descOutput.Columns))
	}
}

func TestMCPServer_HealthCheck(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	s := startMCPTestServer(t, config, "/health")

	resp, err := http.Get(s.baseURL + "/health")
	if err != nil {
		t.Fatalf("health check request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	expected := `{"status":"ok"}`
	if strings.TrimSpace(string(body)) != expected {
		t.Fatalf("expected exact body %s, got %q", expected, string(body))
	}
}

func TestMCPServer_HealthCheckAndMCPCoexist(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	config.Protection.AllowDDL = true
	s := startMCPTestServer(t, config, "/healthz")

	// Verify health check works.
	resp, err := http.Get(s.baseURL + "/healthz")
	if err != nil {
		t.Fatalf("health check request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health check: expected 200, got %d", resp.StatusCode)
	}

	// Verify MCP endpoint works.
	setupTable(t, s.pgMcp, "CREATE TABLE mcp_coexist (id serial PRIMARY KEY)")

	result := s.jsonRPC(t, "tools/call", map[string]interface{}{
		"name": "query",
		"arguments": map[string]interface{}{
			"sql": "SELECT 1 AS val",
		},
	})

	resultObj := result["result"].(map[string]interface{})
	if resultObj["isError"] == true {
		t.Fatalf("MCP query returned error: %v", resultObj)
	}
}

func TestMCPServer_ToolsList(t *testing.T) {
	t.Parallel()
	config := defaultConfig()
	s := startMCPTestServer(t, config, "")

	result := s.jsonRPC(t, "tools/list", map[string]interface{}{})

	resultObj := result["result"].(map[string]interface{})
	tools, ok := resultObj["tools"].([]interface{})
	if !ok {
		t.Fatalf("expected tools array, got %T: %v", resultObj["tools"], resultObj["tools"])
	}

	if len(tools) != 3 {
		t.Fatalf("expected 3 tools, got %d", len(tools))
	}

	toolNames := map[string]bool{}
	for _, tool := range tools {
		toolMap := tool.(map[string]interface{})
		toolNames[toolMap["name"].(string)] = true
	}

	for _, expected := range []string{"query", "list_tables", "describe_table"} {
		if !toolNames[expected] {
			t.Fatalf("expected tool %q in list, got %v", expected, toolNames)
		}
	}
}
