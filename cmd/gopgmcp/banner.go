package main

import (
	"fmt"
	"io"

	"golang.org/x/term"
)

// isTTY returns true if the given file descriptor is a terminal.
func isTTY(fd uintptr) bool {
	return term.IsTerminal(int(fd))
}

// printBanner prints the gopgmcp ASCII art banner. When useColor is true,
// ANSI escape codes are used for a cyan/blue/magenta gradient.
func printBanner(w io.Writer, useColor bool) {
	// ASCII art lines for "gopgmcp"
	lines := []string{
		`                                                  `,
		`   __ _  ___  _ __   __ _ _ __ ___   ___ _ __    `,
		`  / _' |/ _ \| '_ \ / _' | '_ ' _ \ / __| '_ \  `,
		` | (_| | (_) | |_) | (_| | | | | | | (__| |_) | `,
		`  \__, |\___/| .__/ \__, |_| |_| |_|\___| .__/  `,
		`  |___/      |_|    |___/               |_|     `,
		`                                                  `,
	}

	if useColor {
		// Bold + Cyan → Blue → Magenta gradient
		colors := []string{
			"\033[1;36m", // bold cyan
			"\033[1;36m", // bold cyan
			"\033[1;96m", // bold bright cyan
			"\033[1;34m", // bold blue
			"\033[1;35m", // bold magenta
			"\033[1;95m", // bold bright magenta
			"\033[0m",    // reset (blank line)
		}
		for i, line := range lines {
			color := colors[i%len(colors)]
			fmt.Fprintf(w, "%s%s\033[0m\n", color, line)
		}
	} else {
		for _, line := range lines {
			fmt.Fprintln(w, line)
		}
	}
}
