#!/bin/bash

# Update CLAUDE.md based on user prompts
# Usage: ./update-with-prompt.sh "your prompt here"
# Or interactive mode: ./update-with-prompt.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLAUDE_MD="$PROJECT_ROOT/CLAUDE.md"
TEMP_DIR="$PROJECT_ROOT/.claude/tmp"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Claude Code - CLAUDE.md Update Tool                      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if CLAUDE.md exists
if [ ! -f "$CLAUDE_MD" ]; then
    echo -e "${RED}❌ Error: CLAUDE.md not found at $CLAUDE_MD${NC}"
    exit 1
fi

# Create temp directory
mkdir -p "$TEMP_DIR"

# Create backup
BACKUP_FILE="${CLAUDE_MD}.backup.$(date +%Y%m%d-%H%M%S)"
cp "$CLAUDE_MD" "$BACKUP_FILE"
echo -e "${GREEN}💾 Backup created: $BACKUP_FILE${NC}"
echo ""

# Get user prompt
USER_PROMPT=""
if [ $# -eq 0 ]; then
    # Interactive mode
    echo -e "${YELLOW}📝 Interactive Mode${NC}"
    echo -e "Enter your prompt (what you want to add/update in CLAUDE.md):"
    echo -e "${BLUE}Tip: Be specific about what section to update or what to add${NC}"
    echo -e "Examples:"
    echo -e "  - 'Add a new section about WebAssembly support'"
    echo -e "  - 'Update the Testing Infrastructure section with new test categories'"
    echo -e "  - 'Add tips about debugging memory leaks'"
    echo ""
    echo -e "${YELLOW}Your prompt (press Ctrl+D when done):${NC}"
    USER_PROMPT=$(cat)
else
    # Command line argument mode
    USER_PROMPT="$*"
fi

if [ -z "$USER_PROMPT" ]; then
    echo -e "${RED}❌ Error: No prompt provided${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✅ Received prompt:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo "$USER_PROMPT"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Save prompt to file
PROMPT_FILE="$TEMP_DIR/prompt_$(date +%Y%m%d-%H%M%S).txt"
echo "$USER_PROMPT" > "$PROMPT_FILE"
echo -e "${GREEN}💾 Prompt saved to: $PROMPT_FILE${NC}"
echo ""

# Create a template file for user to fill in
CONTENT_FILE="$TEMP_DIR/content_to_add.md"
cat > "$CONTENT_FILE" << 'EOF'
# Content to Add to CLAUDE.md
# ================================
#
# INSTRUCTIONS:
# 1. Write the content you want to add below the "CONTENT START" marker
# 2. Save this file
# 3. The script will help you insert it into CLAUDE.md
#
# TIPS:
# - Use proper Markdown formatting
# - Follow the existing style in CLAUDE.md
# - Be clear and concise
# - Add examples where appropriate
#
# ================================

# CONTENT START (write your content below this line)

EOF

# Append user prompt as context
cat >> "$CONTENT_FILE" << EOF

## Based on your prompt:
$USER_PROMPT

## Suggested sections to update:
EOF

# Analyze prompt and suggest sections
echo "" >> "$CONTENT_FILE"
if echo "$USER_PROMPT" | grep -qi "test\|testing"; then
    echo "- Testing Infrastructure (line ~513)" >> "$CONTENT_FILE"
fi
if echo "$USER_PROMPT" | grep -qi "build\|cmake\|compile"; then
    echo "- Build System (line ~197)" >> "$CONTENT_FILE"
fi
if echo "$USER_PROMPT" | grep -qi "debug\|troubleshoot"; then
    echo "- Debugging Tips (line ~885)" >> "$CONTENT_FILE"
fi
if echo "$USER_PROMPT" | grep -qi "optimizer\|query"; then
    echo "- Query Optimizer (line ~607)" >> "$CONTENT_FILE"
fi
if echo "$USER_PROMPT" | grep -qi "storage\|engine"; then
    echo "- Storage Engines (line ~644)" >> "$CONTENT_FILE"
fi

cat >> "$CONTENT_FILE" << 'EOF'

## Write your content here:
<!-- Start writing below this line -->


<!-- End of content -->

EOF

echo -e "${YELLOW}📋 Next Steps:${NC}"
echo -e "1. ${GREEN}Edit the content file:${NC}"
echo -e "   ${BLUE}nano $CONTENT_FILE${NC}"
echo -e "   or"
echo -e "   ${BLUE}\$EDITOR $CONTENT_FILE${NC}"
echo ""
echo -e "2. ${GREEN}After editing, run the insertion script:${NC}"
echo -e "   ${BLUE}./.claude/scripts/insert-content.sh $CONTENT_FILE${NC}"
echo ""
echo -e "   Or let me open the editor for you? (y/n)"
read -r OPEN_EDITOR

if [[ "$OPEN_EDITOR" =~ ^[Yy]$ ]]; then
    # Try to find a suitable editor
    if [ -n "$EDITOR" ]; then
        $EDITOR "$CONTENT_FILE"
    elif command -v nano &> /dev/null; then
        nano "$CONTENT_FILE"
    elif command -v vi &> /dev/null; then
        vi "$CONTENT_FILE"
    else
        echo -e "${YELLOW}⚠ No editor found. Please edit manually: $CONTENT_FILE${NC}"
    fi

    echo ""
    echo -e "${YELLOW}Would you like to insert the content now? (y/n)${NC}"
    read -r INSERT_NOW

    if [[ "$INSERT_NOW" =~ ^[Yy]$ ]]; then
        # Extract content (everything after "Write your content here:")
        EXTRACTED_CONTENT=$(sed -n '/Write your content here:/,/<!-- End of content -->/p' "$CONTENT_FILE" | \
                           sed '1d;$d' | \
                           sed '/^<!--.*-->$/d' | \
                           sed '/^$/d')

        if [ -z "$EXTRACTED_CONTENT" ]; then
            echo -e "${YELLOW}⚠ No content found to insert${NC}"
            exit 0
        fi

        echo -e "${GREEN}📝 Content extracted:${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo "$EXTRACTED_CONTENT"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""

        # Add to end of CLAUDE.md before the last section
        TEMP_FILE=$(mktemp)

        # Insert before "Last Updated" line
        sed "/\*\*Last Updated:\*\*/i\\
\\
---\\
\\
## Additional Notes\\
\\
$EXTRACTED_CONTENT\\
" "$CLAUDE_MD" > "$TEMP_FILE"

        mv "$TEMP_FILE" "$CLAUDE_MD"

        echo -e "${GREEN}✅ Content added to CLAUDE.md!${NC}"
        echo -e "${YELLOW}💡 Review the changes with: git diff CLAUDE.md${NC}"
    fi
fi

echo ""
echo -e "${GREEN}✨ Done!${NC}"
echo ""
echo -e "${YELLOW}📚 Files created:${NC}"
echo -e "  - Backup: $BACKUP_FILE"
echo -e "  - Prompt: $PROMPT_FILE"
echo -e "  - Content: $CONTENT_FILE"
