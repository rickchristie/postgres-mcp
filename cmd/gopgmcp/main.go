package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		if err := runServe(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "configure":
		if err := runConfigure(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "--help", "-h", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("gopgmcp — PostgreSQL MCP Server")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  gopgmcp serve       Start the MCP server")
	fmt.Println("  gopgmcp configure   Run interactive configuration wizard")
	fmt.Println("  gopgmcp --help      Show this help message")
}
