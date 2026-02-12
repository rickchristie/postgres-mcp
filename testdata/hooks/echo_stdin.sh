#!/bin/bash
# Reads stdin and returns it as modified_query to verify stdin passing
INPUT=$(cat /dev/stdin)
echo "{\"accept\": true, \"modified_query\": \"STDIN: $INPUT\"}"
