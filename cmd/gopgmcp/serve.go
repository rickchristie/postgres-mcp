package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	pgmcp "github.com/rickchristie/postgres-mcp"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/rs/zerolog"
	"golang.org/x/term"
)

func runServe() error {
	ctx := context.Background()

	// 1. Load ServerConfig
	serverConfig, err := loadServerConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if serverConfig.Server.Port <= 0 {
		panic("gopgmcp: server.port must be > 0")
	}

	// 2. Resolve connection string
	connString := os.Getenv("GOPGMCP_PG_CONNSTRING")
	if connString == "" {
		username := promptInput("Username: ")
		password := promptPassword("Password: ")
		connString = buildConnString(serverConfig.Connection, username, password)
	}

	// 3. Setup logger
	logger := setupLogger(serverConfig.Logging)

	// 4. Create PostgresMcp instance
	var opts []pgmcp.Option
	if len(serverConfig.ServerHooks.BeforeQuery) > 0 || len(serverConfig.ServerHooks.AfterQuery) > 0 {
		opts = append(opts, pgmcp.WithServerHooks(serverConfig.ServerHooks))
	}
	pgMcp, err := pgmcp.New(ctx, connString, serverConfig.Config, logger, opts...)
	if err != nil {
		return fmt.Errorf("failed to create PostgresMcp: %w", err)
	}
	defer pgMcp.Close(ctx)

	// 5. Test database connection
	logger.Info().Msg("testing database connection")
	if err := pgMcp.Ping(ctx); err != nil {
		logger.Error().Err(err).Msg("database connection test failed")
		return fmt.Errorf("database connection test failed: %w", err)
	}
	logger.Info().Msg("database connection test successful")

	// 6. Create MCP server with initialize lifecycle logging
	hooks := &server.Hooks{}
	hooks.AddAfterInitialize(func(ctx context.Context, id any, req *mcp.InitializeRequest, result *mcp.InitializeResult) {
		clientName := req.Params.ClientInfo.Name
		clientVersion := req.Params.ClientInfo.Version
		logger.Info().
			Str("client_name", clientName).
			Str("client_version", clientVersion).
			Msg("AI agent connected (MCP initialize)")
	})

	mcpServer := server.NewMCPServer("gopgmcp", "1.0.0",
		server.WithToolCapabilities(true),
		server.WithHooks(hooks),
	)

	pgmcp.RegisterMCPTools(mcpServer, pgMcp)

	// 6. Start HTTP server with optional health check
	addr := fmt.Sprintf(":%d", serverConfig.Server.Port)
	mux := http.NewServeMux()

	// Health check endpoint (process liveness only, not DB connectivity)
	if serverConfig.Server.HealthCheckEnabled {
		if serverConfig.Server.HealthCheckPath == "" {
			panic("gopgmcp: health_check_path must be set when health_check_enabled is true")
		}
		mux.HandleFunc(serverConfig.Server.HealthCheckPath, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
	}

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Create StreamableHTTPServer with custom http.Server
	streamableServer := server.NewStreamableHTTPServer(mcpServer,
		server.WithEndpointPath("/mcp"),
		server.WithStateLess(true),
		server.WithStreamableHTTPServer(httpSrv),
	)

	// Manually register the MCP handler — Start() does NOT register
	// when a custom *http.Server is provided via WithStreamableHTTPServer.
	mux.Handle("/mcp", streamableServer)

	logger.Info().Int("port", serverConfig.Server.Port).Msg("starting gopgmcp server")
	return streamableServer.Start(addr)
}

func loadServerConfig() (*pgmcp.ServerConfig, error) {
	configPath := os.Getenv("GOPGMCP_CONFIG_PATH")
	if configPath == "" {
		configPath = ".gopgmcp/config.json"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	var config pgmcp.ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func buildConnString(conn pgmcp.ConnectionConfig, username, password string) string {
	parts := []string{}
	if conn.Host != "" {
		parts = append(parts, fmt.Sprintf("host=%s", conn.Host))
	}
	if conn.Port > 0 {
		parts = append(parts, fmt.Sprintf("port=%d", conn.Port))
	}
	if conn.DBName != "" {
		parts = append(parts, fmt.Sprintf("dbname=%s", conn.DBName))
	}
	if username != "" {
		parts = append(parts, fmt.Sprintf("user=%s", username))
	}
	if password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", password))
	}
	if conn.SSLMode != "" {
		parts = append(parts, fmt.Sprintf("sslmode=%s", conn.SSLMode))
	}
	return strings.Join(parts, " ")
}

func setupLogger(config pgmcp.LoggingConfig) zerolog.Logger {
	level := zerolog.InfoLevel
	switch strings.ToLower(config.Level) {
	case "debug":
		level = zerolog.DebugLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	}

	var output io.Writer = os.Stderr
	if config.Output == "stdout" {
		output = os.Stdout
	} else if config.Output != "" && config.Output != "stderr" {
		f, err := os.OpenFile(config.Output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			output = f
		}
	}

	if config.Format == "text" {
		output = zerolog.ConsoleWriter{Out: output}
	}

	return zerolog.New(output).Level(level).With().Timestamp().Logger()
}

func promptInput(prompt string) string {
	fmt.Fprint(os.Stderr, prompt)
	var input string
	fmt.Scanln(&input)
	return input
}

func promptPassword(prompt string) string {
	fmt.Fprint(os.Stderr, prompt)
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr) // newline after password input
	if err != nil {
		return ""
	}
	return string(password)
}
