package pgmcp_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// getFreePort returns an available TCP port.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// TestStreamableHTTP_CustomServer_DoesNotRegisterHandler verifies that when
// WithStreamableHTTPServer is used with a custom *http.Server, Start() does
// NOT register the MCP handler on the server's mux. This means the MCP
// endpoint will return 404 unless you register the handler yourself.
func TestStreamableHTTP_CustomServer_DoesNotRegisterHandler(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf(":%d", port)

	mcpServer := server.NewMCPServer("test", "1.0.0")

	// Create a mux with only a health check — do NOT register the MCP handler.
	mux := http.NewServeMux()
	mux.HandleFunc("/health-check", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	streamableServer := server.NewStreamableHTTPServer(mcpServer,
		server.WithEndpointPath("/mcp"),
		server.WithStreamableHTTPServer(httpSrv),
	)

	go func() {
		if err := streamableServer.Start(addr); err != nil && err != http.ErrServerClosed {
			t.Logf("server error: %v", err)
		}
	}()

	// Wait for server to start.
	time.Sleep(200 * time.Millisecond)
	defer streamableServer.Shutdown(context.Background())

	// Health check should work.
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health-check", port))
	if err != nil {
		t.Fatalf("health check request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("health check: expected 200, got %d", resp.StatusCode)
	}

	// MCP endpoint should return 404 because Start() did not register it.
	mcpResp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/mcp", port),
		"application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`),
	)
	if err != nil {
		t.Fatalf("MCP request failed: %v", err)
	}
	defer mcpResp.Body.Close()

	// This is the bug: the MCP endpoint is NOT registered, so we expect 404.
	if mcpResp.StatusCode == http.StatusOK {
		t.Log("MCP endpoint returned 200 — Start() DID register the handler (unexpected)")
	} else {
		t.Logf("MCP endpoint returned %d — confirms Start() does NOT register handler when custom server provided", mcpResp.StatusCode)
	}
}

// TestStreamableHTTP_ManualRegistration_Works verifies the correct approach:
// manually register the StreamableHTTPServer as a handler on the mux before
// calling Start(). This way both health check and MCP endpoint work.
func TestStreamableHTTP_ManualRegistration_Works(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf(":%d", port)

	mcpServer := server.NewMCPServer("test", "1.0.0",
		server.WithToolCapabilities(true),
	)

	// Register a simple test tool.
	mcpServer.AddTool(mcp.NewTool("ping",
		mcp.WithDescription("Returns pong"),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("pong"), nil
	})

	// Step 1: Create the mux.
	mux := http.NewServeMux()

	// Step 2: Register health check.
	mux.HandleFunc("/health-check", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Step 3: Create custom http.Server with the mux.
	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Step 4: Create StreamableHTTPServer with the custom server.
	streamableServer := server.NewStreamableHTTPServer(mcpServer,
		server.WithEndpointPath("/mcp"),
		server.WithStreamableHTTPServer(httpSrv),
	)

	// Step 5: Manually register the StreamableHTTPServer on the mux.
	mux.Handle("/mcp", streamableServer)

	go func() {
		if err := streamableServer.Start(addr); err != nil && err != http.ErrServerClosed {
			t.Logf("server error: %v", err)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	defer streamableServer.Shutdown(context.Background())

	// Health check should work.
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health-check", port))
	if err != nil {
		t.Fatalf("health check request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("health check: expected 200, got %d", resp.StatusCode)
	}

	// MCP endpoint should work — we registered it manually.
	mcpResp, err := http.Post(
		fmt.Sprintf("http://localhost:%d/mcp", port),
		"application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`),
	)
	if err != nil {
		t.Fatalf("MCP request failed: %v", err)
	}
	defer mcpResp.Body.Close()
	body, _ := io.ReadAll(mcpResp.Body)

	if mcpResp.StatusCode != http.StatusOK {
		t.Errorf("MCP endpoint: expected 200, got %d; body: %s", mcpResp.StatusCode, string(body))
	} else {
		t.Logf("MCP endpoint returned 200 — manual registration works. Body: %s", string(body))
	}
}
