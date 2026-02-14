package pgmcp

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// RegisterMCPTools registers Query, ListTables, and DescribeTable
// as MCP tools on the given MCP server.
func RegisterMCPTools(mcpServer *server.MCPServer, pgMcp *PostgresMcp) {
	// Query tool
	queryTool := mcp.NewTool("query",
		mcp.WithDescription("Execute a SQL query against the PostgreSQL database. Returns results as JSON."),
		mcp.WithString("sql",
			mcp.Required(),
			mcp.Description("The SQL query to execute"),
		),
	)

	mcpServer.AddTool(queryTool, pgMcp.loggedToolHandler("query", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		sql, err := req.RequireString("sql")
		if err != nil {
			return mcp.NewToolResultError("sql parameter is required"), nil
		}
		output := pgMcp.Query(ctx, QueryInput{SQL: sql})
		if output.Error != "" {
			return mcp.NewToolResultError(output.Error), nil
		}
		jsonBytes, err := json.Marshal(output)
		if err != nil {
			return mcp.NewToolResultError("failed to marshal query result"), nil
		}
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}))

	// ListTables tool
	listTablesTool := mcp.NewTool("list_tables",
		mcp.WithDescription("List all tables, views, materialized views, and foreign tables in the database that are accessible to the current user."),
		mcp.WithReadOnlyHintAnnotation(true),
	)

	mcpServer.AddTool(listTablesTool, pgMcp.loggedToolHandler("list_tables", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		output, err := pgMcp.ListTables(ctx, ListTablesInput{})
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		jsonBytes, err := json.Marshal(output)
		if err != nil {
			return mcp.NewToolResultError("failed to marshal list tables result"), nil
		}
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}))

	// DescribeTable tool
	describeTableTool := mcp.NewTool("describe_table",
		mcp.WithDescription("Describe the schema of a table including columns, types, indexes, constraints, and foreign keys."),
		mcp.WithString("table",
			mcp.Required(),
			mcp.Description("The table name to describe"),
		),
		mcp.WithString("schema",
			mcp.Description("The schema name (defaults to 'public')"),
		),
		mcp.WithReadOnlyHintAnnotation(true),
	)

	mcpServer.AddTool(describeTableTool, pgMcp.loggedToolHandler("describe_table", func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		table, err := req.RequireString("table")
		if err != nil {
			return mcp.NewToolResultError("table parameter is required"), nil
		}
		schema := req.GetString("schema", "")

		output, err := pgMcp.DescribeTable(ctx, DescribeTableInput{Table: table, Schema: schema})
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		jsonBytes, err := json.Marshal(output)
		if err != nil {
			return mcp.NewToolResultError("failed to marshal describe table result"), nil
		}
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}))
}

// loggedToolHandler wraps a tool handler to log request and response lengths.
func (p *PostgresMcp) loggedToolHandler(tool string, handler server.ToolHandlerFunc) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		reqLen := requestLength(req)
		result, err := handler(ctx, req)
		respLen := resultLength(result)
		p.logger.Info().
			Str("tool", tool).
			Int("request_bytes", reqLen).
			Int("response_bytes", respLen).
			Msg("tool call")
		return result, err
	}
}

// requestLength returns the JSON-encoded byte length of the request arguments.
func requestLength(req mcp.CallToolRequest) int {
	args := req.GetArguments()
	if len(args) == 0 {
		return 0
	}
	b, err := json.Marshal(args)
	if err != nil {
		return 0
	}
	return len(b)
}

// resultLength returns the total byte length of text content in a CallToolResult.
func resultLength(result *mcp.CallToolResult) int {
	if result == nil {
		return 0
	}
	total := 0
	for _, c := range result.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			total += len(tc.Text)
		}
	}
	return total
}
