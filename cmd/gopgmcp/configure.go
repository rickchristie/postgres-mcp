package main

import (
	"flag"
	"os"

	"github.com/rickchristie/postgres-mcp/internal/configure"
)

func runConfigure() error {
	fs := flag.NewFlagSet("configure", flag.ExitOnError)
	configPath := fs.String("config", ".gopgmcp/config.json", "Path to configuration file")
	fs.Parse(os.Args[2:])

	printBanner(os.Stderr, isTTY(os.Stderr.Fd()))
	return configure.Run(*configPath)
}
