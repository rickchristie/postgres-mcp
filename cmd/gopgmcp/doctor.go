package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"

	pgmcp "github.com/rickchristie/postgres-mcp"
	"github.com/rickchristie/postgres-mcp/internal/meta"
)

func runDoctor() error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	configPath := fs.String("config", ".gopgmcp/config.json", "Path to configuration file")
	fs.Parse(os.Args[2:])

	useColor := isTTY(os.Stderr.Fd())
	return doctor(os.Stderr, useColor, *configPath)
}

func doctor(w io.Writer, useColor bool, configPath string) error {
	printBanner(w, useColor)
	fmt.Fprintf(w, "gopgmcp %s\n\n", meta.Version)

	// Load and validate config
	config, ok := doctorValidateConfig(w, useColor, configPath)
	if !ok {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Fix the issues above and run 'gopgmcp doctor' again.")
		return nil
	}

	// Print agent connection snippets
	fmt.Fprintln(w)
	printAgentSnippets(w, useColor, config)
	return nil
}

// doctorValidateConfig loads and validates the config file, printing check results.
// Returns the parsed config and true if all checks passed.
func doctorValidateConfig(w io.Writer, useColor bool, configPath string) (*pgmcp.ServerConfig, bool) {
	allPassed := true

	// Check 1: Config file exists and is valid JSON
	data, err := os.ReadFile(configPath)
	if err != nil {
		printCheck(w, useColor, false, fmt.Sprintf("Config file readable (%s)", configPath))
		allPassed = false
		return nil, allPassed
	}
	printCheck(w, useColor, true, fmt.Sprintf("Config file readable (%s)", configPath))

	var config pgmcp.ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		printCheck(w, useColor, false, fmt.Sprintf("Config file is valid JSON: %v", err))
		allPassed = false
		return nil, allPassed
	}
	printCheck(w, useColor, true, "Config file is valid JSON")

	// Check 2: connection.dbname is set
	if config.Connection.DBName == "" {
		printCheck(w, useColor, false, "connection.dbname is set")
		allPassed = false
	} else {
		printCheck(w, useColor, true, fmt.Sprintf("connection.dbname is set (%s)", config.Connection.DBName))
	}

	// Check 3: server.port > 0
	if config.Server.Port <= 0 {
		printCheck(w, useColor, false, "server.port is > 0")
		allPassed = false
	} else {
		printCheck(w, useColor, true, fmt.Sprintf("server.port is > 0 (%d)", config.Server.Port))
	}

	// Check 4: Health check path set when enabled
	if config.Server.HealthCheckEnabled {
		if config.Server.HealthCheckPath == "" {
			printCheck(w, useColor, false, "health_check_path is set (required when health_check_enabled)")
			allPassed = false
		} else {
			printCheck(w, useColor, true, fmt.Sprintf("health_check_path is set (%s)", config.Server.HealthCheckPath))
		}
	}

	// Check 5: Regex patterns compile
	regexOK := true

	for i, rule := range config.ErrorPrompts {
		if _, err := regexp.Compile(rule.Pattern); err != nil {
			printCheck(w, useColor, false, fmt.Sprintf("error_prompts[%d] regex compiles: %v", i, err))
			regexOK = false
			allPassed = false
		}
	}

	for i, rule := range config.Sanitization {
		if _, err := regexp.Compile(rule.Pattern); err != nil {
			printCheck(w, useColor, false, fmt.Sprintf("sanitization[%d] regex compiles: %v", i, err))
			regexOK = false
			allPassed = false
		}
	}

	for i, rule := range config.Query.TimeoutRules {
		if _, err := regexp.Compile(rule.Pattern); err != nil {
			printCheck(w, useColor, false, fmt.Sprintf("timeout_rules[%d] regex compiles: %v", i, err))
			regexOK = false
			allPassed = false
		}
	}

	for i, hook := range config.ServerHooks.BeforeQuery {
		if _, err := regexp.Compile(hook.Pattern); err != nil {
			printCheck(w, useColor, false, fmt.Sprintf("server_hooks.before_query[%d] regex compiles: %v", i, err))
			regexOK = false
			allPassed = false
		}
	}

	for i, hook := range config.ServerHooks.AfterQuery {
		if _, err := regexp.Compile(hook.Pattern); err != nil {
			printCheck(w, useColor, false, fmt.Sprintf("server_hooks.after_query[%d] regex compiles: %v", i, err))
			regexOK = false
			allPassed = false
		}
	}

	if regexOK {
		printCheck(w, useColor, true, "All regex patterns compile")
	}

	return &config, allPassed
}

// printCheck prints a colored ✓ or ✗ check line.
func printCheck(w io.Writer, useColor bool, pass bool, msg string) {
	if pass {
		if useColor {
			fmt.Fprintf(w, "  \033[32m✓\033[0m %s\n", msg)
		} else {
			fmt.Fprintf(w, "  ✓ %s\n", msg)
		}
	} else {
		if useColor {
			fmt.Fprintf(w, "  \033[31m✗\033[0m %s\n", msg)
		} else {
			fmt.Fprintf(w, "  ✗ %s\n", msg)
		}
	}
}

// printAgentSnippets prints MCP connection config snippets for various AI agents.
func printAgentSnippets(w io.Writer, useColor bool, config *pgmcp.ServerConfig) {
	port := config.Server.Port
	url := fmt.Sprintf("http://localhost:%d/mcp", port)

	heading := func(title string) {
		if useColor {
			fmt.Fprintf(w, "\033[1;36m%s\033[0m\n", title)
		} else {
			fmt.Fprintln(w, title)
		}
	}

	subheading := func(title string) {
		if useColor {
			fmt.Fprintf(w, "  \033[1m%s\033[0m\n", title)
		} else {
			fmt.Fprintf(w, "  %s\n", title)
		}
	}

	heading("Agent Connection Snippets")
	fmt.Fprintln(w)

	// Claude Code
	subheading("Claude Code")
	fmt.Fprintf(w, "  Run this command to add the server:\n\n")
	fmt.Fprintf(w, "    claude mcp add --transport http postgres %s\n\n", url)
	fmt.Fprintf(w, "  Or add to .mcp.json (project scope):\n\n")
	fmt.Fprintf(w, `  {
    "mcpServers": {
      "postgres": {
        "type": "http",
        "url": "%s"
      }
    }
  }
`, url)
	fmt.Fprintln(w)

	// Copilot CLI
	subheading("Copilot CLI (~/.copilot/mcp-config.json)")
	fmt.Fprintf(w, `  {
    "mcpServers": {
      "postgres": {
        "type": "http",
        "url": "%s"
      }
    }
  }
`, url)
	fmt.Fprintln(w)

	// Gemini CLI
	subheading("Gemini CLI (~/.gemini/settings.json)")
	fmt.Fprintf(w, `  {
    "mcpServers": {
      "postgres": {
        "httpUrl": "%s"
      }
    }
  }
`, url)
	fmt.Fprintln(w)

	// OpenCode
	subheading("OpenCode (opencode.json)")
	fmt.Fprintf(w, `  {
    "mcp": {
      "postgres": {
        "type": "remote",
        "url": "%s"
      }
    }
  }
`, url)
	fmt.Fprintln(w)

	// Cursor
	subheading("Cursor (.cursor/mcp.json)")
	fmt.Fprintf(w, `  {
    "mcpServers": {
      "postgres": {
        "url": "%s"
      }
    }
  }
`, url)
	fmt.Fprintln(w)

	// Windsurf
	subheading("Windsurf (~/.codeium/windsurf/mcp_config.json)")
	fmt.Fprintf(w, `  {
    "mcpServers": {
      "postgres": {
        "serverUrl": "%s"
      }
    }
  }
`, url)
}
