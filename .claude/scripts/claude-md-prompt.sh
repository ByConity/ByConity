#!/bin/bash

# CLAUDE.md Update via Prompt (similar to # function)
# Usage:
#   ./claude-md-prompt.sh "add section about performance tuning"
#   ./claude-md-prompt.sh --interactive

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLAUDE_MD="$PROJECT_ROOT/CLAUDE.md"
TEMP_DIR="$PROJECT_ROOT/.claude/tmp"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║     📝 CLAUDE.MD Smart Update Tool (# Function)              ║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if CLAUDE.md exists
if [ ! -f "$CLAUDE_MD" ]; then
    echo -e "${RED}❌ Error: CLAUDE.md not found${NC}"
    exit 1
fi

mkdir -p "$TEMP_DIR"

# Get user prompt
PROMPT=""
if [ "$1" = "--interactive" ] || [ $# -eq 0 ]; then
    echo -e "${YELLOW}💭 What would you like to update in CLAUDE.md?${NC}"
    echo -e "${BLUE}Examples:${NC}"
    echo -e "  • Add a section about Docker deployment best practices"
    echo -e "  • Update the testing section with performance benchmarks"
    echo -e "  • Add tips for debugging transaction issues"
    echo ""
    read -p "Your prompt: " PROMPT
else
    PROMPT="$*"
fi

if [ -z "$PROMPT" ]; then
    echo -e "${RED}❌ No prompt provided${NC}"
    exit 1
fi

# Create backup
BACKUP_FILE="${CLAUDE_MD}.backup.$(date +%Y%m%d-%H%M%S)"
cp "$CLAUDE_MD" "$BACKUP_FILE"

echo ""
echo -e "${GREEN}✅ Prompt received:${NC} $PROMPT"
echo -e "${GREEN}💾 Backup created:${NC} $BACKUP_FILE"
echo ""

# Generate context file for Claude
CONTEXT_FILE="$TEMP_DIR/claude_context_$(date +%Y%m%d-%H%M%S).md"

cat > "$CONTEXT_FILE" << EOF
# Context for CLAUDE.md Update

## User Request
\`\`\`
$PROMPT
\`\`\`

## Current CLAUDE.md Length
$(wc -l < "$CLAUDE_MD") lines

## Task
Based on the user's prompt above, please:
1. Analyze what section(s) of CLAUDE.md should be updated
2. Generate the appropriate content in Markdown format
3. Indicate where it should be inserted

## Instructions for Content Generation
- Follow the existing style and format of CLAUDE.md
- Use proper Markdown syntax
- Be concise but comprehensive
- Include code examples where relevant
- Follow ByConity coding conventions

## CLAUDE.md Current Sections
EOF

# Extract table of contents
echo "" >> "$CONTEXT_FILE"
echo "\`\`\`" >> "$CONTEXT_FILE"
grep "^##" "$CLAUDE_MD" | head -20 >> "$CONTEXT_FILE"
echo "\`\`\`" >> "$CONTEXT_FILE"

cat >> "$CONTEXT_FILE" << 'EOF'

## Output Format Requested
Please provide the content in this format:

```markdown
<!-- INSERT: [Section Name] | [Position: after/before line X or "append"] -->

[Your generated content here in Markdown]

<!-- END INSERT -->
```

## Response Template
```markdown
<!-- CLAUDE.MD UPDATE -->
## Analysis
- Target Section: [section name]
- Insert Position: [where to insert]
- Change Type: [add/update/replace]

## Generated Content
<!-- INSERT: ... -->
[content]
<!-- END INSERT -->

## Instructions
[How to apply this update]
<!-- END CLAUDE.MD UPDATE -->
```

EOF

echo -e "${CYAN}📄 Context file created:${NC} $CONTEXT_FILE"
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}  Next Steps:${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}Option 1: Use Claude Code (Recommended)${NC}"
echo -e "  Copy and paste this to Claude:"
echo ""
echo -e "${CYAN}─────────────────────────────────────────────────────────────${NC}"
cat << 'PROMPT_END'
Please read the context file and help me update CLAUDE.md based on the user prompt.
Read: .claude/tmp/claude_context_[timestamp].md
Then generate the appropriate content and tell me exactly where to insert it.
PROMPT_END
echo -e "${CYAN}─────────────────────────────────────────────────────────────${NC}"
echo ""

echo -e "${GREEN}Option 2: Manual Edit${NC}"
echo -e "  1. Review the context: ${BLUE}cat $CONTEXT_FILE${NC}"
echo -e "  2. Edit CLAUDE.md: ${BLUE}\$EDITOR $CLAUDE_MD${NC}"
echo -e "  3. Save and commit"
echo ""

echo -e "${GREEN}Option 3: Generate Template${NC}"
echo -e "  Create a template section to fill in:"
echo ""

# Generate a template based on the prompt
TEMPLATE_FILE="$TEMP_DIR/template_$(date +%Y%m%d-%H%M%S).md"

cat > "$TEMPLATE_FILE" << EOF
---

## [Your Section Title Here]

### Overview
[Brief description of this section]

### Key Points
- Point 1
- Point 2
- Point 3

### Example
\`\`\`cpp
// Example code here
\`\`\`

### Related Sections
- See [Related Section](#related-section)

---
EOF

echo -e "  Template created: ${BLUE}$TEMPLATE_FILE${NC}"
echo -e "  Edit template: ${BLUE}\$EDITOR $TEMPLATE_FILE${NC}"
echo -e "  Then append: ${BLUE}cat $TEMPLATE_FILE >> $CLAUDE_MD${NC}"
echo ""

echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Ask if user wants to see the context
echo -e "${YELLOW}Would you like to see the context file now? (y/n)${NC}"
read -r SHOW_CONTEXT

if [[ "$SHOW_CONTEXT" =~ ^[Yy]$ ]]; then
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    cat "$CONTEXT_FILE"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
fi

echo ""
echo -e "${GREEN}✨ Prompt processed!${NC}"
echo ""
echo -e "${BLUE}📂 Generated files:${NC}"
echo -e "  • Context: $CONTEXT_FILE"
echo -e "  • Template: $TEMPLATE_FILE"
echo -e "  • Backup: $BACKUP_FILE"
echo ""
echo -e "${YELLOW}💡 Tip: You can restore from backup anytime:${NC}"
echo -e "   ${BLUE}cp $BACKUP_FILE $CLAUDE_MD${NC}"
