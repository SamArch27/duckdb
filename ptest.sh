#!/usr/bin/env bash

# Script to run all Python test files in a directory
# Usage: ./run_tests.sh [directory]

# Set directory to first argument or current directory if not provided
TEST_DIR="../tests/"

# Check if directory exists
if [ ! -d "$TEST_DIR" ]; then
	echo "Error: Directory '$TEST_DIR' does not exist."
	exit 1
fi

# Find all Python files in the directory
PYTHON_FILES=$(find "$TEST_DIR" -name "*.py")

# Check if any Python files were found
if [ -z "$PYTHON_FILES" ]; then
	echo "No Python files found in '$TEST_DIR'."
	exit 0
fi

# Count the number of files
NUM_FILES=$(echo "$PYTHON_FILES" | wc -l)
echo "Found $NUM_FILES Python files."

# Run each Python file
for FILE in $PYTHON_FILES; do
	echo -e "\nRunning: $FILE"
	echo "----------------------------------------"

	# Run the Python file and capture the exit code
	python3.9 "$FILE"
	EXIT_CODE=$?

	echo "Exit code: $EXIT_CODE"
done

echo -e "\nAll tests completed."
