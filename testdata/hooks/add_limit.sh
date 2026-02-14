#!/bin/bash
# Reads stdin query string, appends LIMIT 1 if it contains SELECT
INPUT=$(cat /dev/stdin)
if echo "$INPUT" | grep -qi "SELECT"; then
    echo "{\"accept\": true, \"modified_query\": \"${INPUT} LIMIT 1\"}"
else
    echo "{\"accept\": true}"
fi
