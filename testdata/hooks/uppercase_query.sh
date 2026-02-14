#!/bin/bash
# Reads stdin, uppercases the query, returns as modified_query
INPUT=$(cat /dev/stdin)
UPPER=$(echo "$INPUT" | tr '[:lower:]' '[:upper:]')
echo "{\"accept\": true, \"modified_query\": \"$UPPER\"}"
