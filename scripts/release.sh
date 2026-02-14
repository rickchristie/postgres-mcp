#!/usr/bin/env bash
set -euo pipefail

# Extract version from source code
version_file="internal/meta/version.go"
if [[ ! -f "$version_file" ]]; then
  echo "Error: $version_file not found. Run from project root."
  exit 1
fi

version=$(grep -oP 'Version\s*=\s*"\K[^"]+' "$version_file")
if [[ -z "$version" ]]; then
  echo "Error: could not parse version from $version_file"
  exit 1
fi

new_tag="v${version}"
echo "Version from source: $new_tag"

# Find last version tag
last_tag=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || echo "")

if [[ -n "$last_tag" ]]; then
  echo "Last release tag:    $last_tag"

  # Check if this tag already exists
  if [[ "$last_tag" == "$new_tag" ]]; then
    echo "Error: tag $new_tag already exists. Bump the version in $version_file first."
    exit 1
  fi

  # Compare versions — new must be greater than last
  last_version="${last_tag#v}"
  highest=$(printf '%s\n' "$last_version" "$version" | sort -V | tail -1)
  if [[ "$highest" != "$version" ]]; then
    echo "Error: version $version is not newer than $last_version. Bump the version in $version_file first."
    exit 1
  fi

  log_range="${last_tag}..HEAD"
else
  echo "No previous release tags found. This will be the first release."
  log_range="HEAD"
fi

# Collect git log
tmpfile=$(mktemp /tmp/release-notes-XXXXXX)
trap 'rm -f "$tmpfile"' EXIT

echo "# Release $new_tag" > "$tmpfile"
echo "#" >> "$tmpfile"
echo "# Edit release notes below. Lines starting with # will be removed." >> "$tmpfile"
echo "# Save an empty file (no non-comment lines) to abort the release." >> "$tmpfile"
echo "" >> "$tmpfile"

if [[ "$log_range" == "HEAD" ]]; then
  git log --oneline >> "$tmpfile"
else
  git log "${log_range}" --oneline >> "$tmpfile"
fi

# Open editor for release notes
editor="${EDITOR:-vi}"
"$editor" "$tmpfile"

# Strip comment lines and check if anything remains
notes=$(grep -v '^#' "$tmpfile" | sed '/^[[:space:]]*$/d' || true)
if [[ -z "$notes" ]]; then
  echo "Release notes empty — aborting release."
  exit 0
fi

# Write cleaned notes back to temp file for gh
echo "$notes" > "$tmpfile"

echo ""
echo "--- Release Notes ---"
echo "$notes"
echo "---------------------"
echo ""

# Create and push tag
echo "Creating tag $new_tag..."
git tag "$new_tag"
echo "Pushing tag $new_tag..."
git push origin "$new_tag"

# Trigger Go module proxy
echo "Triggering Go module proxy..."
GOPROXY=proxy.golang.org go list -m "github.com/rickchristie/postgres-mcp@${new_tag}" 2>/dev/null || true

echo ""
echo "Release $new_tag complete!"
echo "https://pkg.go.dev/github.com/rickchristie/postgres-mcp@${new_tag}"
