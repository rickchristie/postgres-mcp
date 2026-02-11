#!/bin/bash
cat /dev/stdin > /dev/null
echo "{\"accept\": true, \"modified_query\": \"ARGS: $*\"}"
