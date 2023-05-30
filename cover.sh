#!/bin/bash

# Create a temporary file for coverage output
TMP=$(mktemp /tmp/olric-coverage-XXXXX.txt)

# Set the output file name
OUT=$1

# Exit immediately if a command exits with a non-zero status
set -e

# Initialize the coverage output file
echo 'mode: atomic' > "$OUT"

# Iterate through all packages, excluding specific ones, and run tests with coverage
for PKG in $(go list ./... | grep -v -E 'vendor|hasher|internal/bufpool|internal/flog|serializer|stats|cmd'); do
  go test -covermode=atomic -coverprofile="$TMP" "$PKG"
  
  # Append the coverage results to the output file, skipping the first line (header)
  tail -n +2 "$TMP" >> "$OUT"
done
