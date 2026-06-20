#!/bin/bash

# Update CLAUDE.md with current project statistics
# This script scans the ByConity codebase and updates metadata in CLAUDE.md

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLAUDE_MD="$PROJECT_ROOT/CLAUDE.md"

echo "🔍 Scanning ByConity project structure..."
echo "📂 Project root: $PROJECT_ROOT"

# Check if CLAUDE.md exists
if [ ! -f "$CLAUDE_MD" ]; then
    echo "❌ Error: CLAUDE.md not found at $CLAUDE_MD"
    exit 1
fi

# Create backup
BACKUP_FILE="${CLAUDE_MD}.backup.$(date +%Y%m%d-%H%M%S)"
cp "$CLAUDE_MD" "$BACKUP_FILE"
echo "💾 Backup created: $BACKUP_FILE"

# Gather statistics
echo ""
echo "📊 Gathering project statistics..."

# Count source files in src/
SRC_DIR="$PROJECT_ROOT/src"
if [ -d "$SRC_DIR" ]; then
    SRC_FILE_COUNT=$(find "$SRC_DIR" -type f \( -name "*.cpp" -o -name "*.h" \) | wc -l)
    SRC_MODULE_COUNT=$(find "$SRC_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)
    echo "  ✓ Source files: $SRC_FILE_COUNT"
    echo "  ✓ Source modules: $SRC_MODULE_COUNT"
else
    echo "  ⚠ Warning: src/ directory not found"
    SRC_FILE_COUNT="N/A"
    SRC_MODULE_COUNT="N/A"
fi

# Count programs
PROGRAMS_DIR="$PROJECT_ROOT/programs"
if [ -d "$PROGRAMS_DIR" ]; then
    PROGRAMS_COUNT=$(find "$PROGRAMS_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)
    echo "  ✓ Programs: $PROGRAMS_COUNT"
else
    echo "  ⚠ Warning: programs/ directory not found"
    PROGRAMS_COUNT="N/A"
fi

# Count contrib dependencies
CONTRIB_DIR="$PROJECT_ROOT/contrib"
if [ -d "$CONTRIB_DIR" ]; then
    CONTRIB_COUNT=$(find "$CONTRIB_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)
    echo "  ✓ Contrib dependencies: $CONTRIB_COUNT"
else
    echo "  ⚠ Warning: contrib/ directory not found"
    CONTRIB_COUNT="N/A"
fi

# Count CMake modules
CMAKE_DIR="$PROJECT_ROOT/cmake"
if [ -d "$CMAKE_DIR" ]; then
    CMAKE_MODULE_COUNT=$(find "$CMAKE_DIR" -type f -name "*.cmake" | wc -l)
    echo "  ✓ CMake modules: $CMAKE_MODULE_COUNT"
else
    CMAKE_MODULE_COUNT="N/A"
fi

# Count test categories
TESTS_DIR="$PROJECT_ROOT/tests/queries"
if [ -d "$TESTS_DIR" ]; then
    TEST_CATEGORY_COUNT=$(find "$TESTS_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)
    echo "  ✓ Test categories: $TEST_CATEGORY_COUNT"
else
    TEST_CATEGORY_COUNT="N/A"
fi

# Get current date
CURRENT_DATE=$(date +%Y-%m-%d)
echo "  ✓ Current date: $CURRENT_DATE"

# Update CLAUDE.md
echo ""
echo "📝 Updating CLAUDE.md..."

# Use a temp file for atomic updates
TEMP_FILE=$(mktemp)

# Update statistics in the document
sed -e "s/\*\*Total Source Files\*\*: ~[0-9,]* in \`\/src\`/**Total Source Files**: ~$SRC_FILE_COUNT in \`\/src\`/" \
    -e "s/\*\*Major Modules\*\*: [0-9]* primary components/**Major Modules**: $SRC_MODULE_COUNT primary components/" \
    -e "s/\*\*Programs\*\*: [0-9]* executable tools/**Programs**: $PROGRAMS_COUNT executable tools/" \
    -e "s/\*\*External Dependencies\*\*: [0-9,+]* in \`\/contrib\`/**External Dependencies**: $CONTRIB_COUNT+ in \`\/contrib\`/" \
    -e "s/\*\*Test Categories\*\*: [0-9+]* specialized test directories/**Test Categories**: $TEST_CATEGORY_COUNT+ specialized test directories/" \
    -e "s/\*\*Last Updated:\*\* [0-9-]*/**Last Updated:** $CURRENT_DATE/" \
    "$CLAUDE_MD" > "$TEMP_FILE"

# Check if updates were successful
if [ $? -eq 0 ]; then
    mv "$TEMP_FILE" "$CLAUDE_MD"
    echo "✅ CLAUDE.md updated successfully!"
    echo ""
    echo "📋 Summary of updates:"
    echo "  • Source files: $SRC_FILE_COUNT"
    echo "  • Major modules: $SRC_MODULE_COUNT"
    echo "  • Programs: $PROGRAMS_COUNT"
    echo "  • External dependencies: $CONTRIB_COUNT+"
    echo "  • Test categories: $TEST_CATEGORY_COUNT+"
    echo "  • Last updated: $CURRENT_DATE"
    echo ""
    echo "💡 Backup saved to: $BACKUP_FILE"
    echo "💡 You can restore from backup using: cp $BACKUP_FILE $CLAUDE_MD"
else
    rm -f "$TEMP_FILE"
    echo "❌ Error: Failed to update CLAUDE.md"
    exit 1
fi

# Show diff
echo ""
echo "📊 Changes made:"
diff -u "$BACKUP_FILE" "$CLAUDE_MD" | head -n 50 || true

echo ""
echo "✨ Done!"
