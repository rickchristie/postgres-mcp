package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrintBannerWithColor(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	printBanner(&buf, true)
	output := buf.String()

	// Should contain ANSI escape codes
	if !strings.Contains(output, "\033[") {
		t.Fatal("expected ANSI escape codes in colored banner output")
	}

	// Should contain reset codes
	if !strings.Contains(output, "\033[0m") {
		t.Fatal("expected ANSI reset code in colored banner output")
	}

	// Should contain ASCII art fragments
	if !strings.Contains(output, `___`) {
		t.Fatal("expected ASCII art underscores in banner output")
	}
	if !strings.Contains(output, `|`) {
		t.Fatal("expected ASCII art pipes in banner output")
	}
}

func TestPrintBannerWithoutColor(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	printBanner(&buf, false)
	output := buf.String()

	// Should NOT contain ANSI escape codes
	if strings.Contains(output, "\033[") {
		t.Fatal("expected no ANSI escape codes in plain banner output")
	}

	// Should still contain ASCII art fragments
	if !strings.Contains(output, `___`) {
		t.Fatal("expected ASCII art underscores in plain banner output")
	}
	if !strings.Contains(output, `|`) {
		t.Fatal("expected ASCII art pipes in plain banner output")
	}
}
