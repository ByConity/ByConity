# Claude Prompt Command - Quick Guide

## 🎯 What is this?

The `/claude-prompt` command is like the `#` function - you tell it what you want in natural language, and it helps you update CLAUDE.md intelligently.

## 🚀 Quick Start

### Basic Usage

```bash
# From project root
./.claude/scripts/claude-md-prompt.sh "your request here"
```

### Examples

```bash
# Example 1: Add a new section
./.claude/scripts/claude-md-prompt.sh "add a section about debugging memory leaks with valgrind"

# Example 2: Update existing content
./.claude/scripts/claude-md-prompt.sh "update the testing section to include performance benchmarks"

# Example 3: Add tips
./.claude/scripts/claude-md-prompt.sh "add tips for optimizing query performance in the optimizer section"

# Example 4: Interactive mode
./.claude/scripts/claude-md-prompt.sh --interactive
```

## 📝 Prompt Ideas

### Adding New Sections

```bash
"add a section about WebAssembly support in ByConity"
"create a troubleshooting guide for common deployment issues"
"add documentation about the resource manager configuration"
```

### Updating Existing Sections

```bash
"update the build system section with Apple Silicon instructions"
"add more examples to the query optimizer section"
"expand the testing infrastructure with performance testing details"
```

### Adding Tips and Best Practices

```bash
"add debugging tips for transaction timeout issues"
"include best practices for production deployment"
"add performance tuning tips for large datasets"
```

### Documentation Improvements

```bash
"add more code examples to the storage engines section"
"clarify the difference between ByConity and ClickHouse"
"add diagrams explaining the transaction flow"
```

## 🔄 Workflow

### Step 1: Run the command with your prompt

```bash
./.claude/scripts/claude-md-prompt.sh "add section about monitoring and observability"
```

### Step 2: Review the generated context

The tool will create:
- 📄 **Context file**: Analysis of your request
- 📋 **Template file**: Pre-formatted content template
- 💾 **Backup**: Automatic backup of CLAUDE.md

### Step 3: Choose your approach

**Option A: Use Claude Code (Recommended)**
```
Copy the suggested prompt and ask Claude to:
1. Read the context file
2. Generate appropriate content
3. Tell you where to insert it
```

**Option B: Edit template manually**
```bash
# Edit the generated template
$EDITOR .claude/tmp/template_[timestamp].md

# Append to CLAUDE.md
cat .claude/tmp/template_[timestamp].md >> CLAUDE.md
```

**Option C: Direct insertion**
The tool can guide you through direct insertion interactively.

## 📊 Example Session

```bash
$ ./.claude/scripts/claude-md-prompt.sh "add debugging tips for TSO issues"

╔═══════════════════════════════════════════════════════════════╗
║     📝 CLAUDE.MD Smart Update Tool (# Function)              ║
╚═══════════════════════════════════════════════════════════════╝

✅ Prompt received: add debugging tips for TSO issues
💾 Backup created: CLAUDE.md.backup.20260113-143022

📄 Context file created: .claude/tmp/claude_context_20260113-143022.md

═══════════════════════════════════════════════════════════════
  Next Steps:
═══════════════════════════════════════════════════════════════

Option 1: Use Claude Code (Recommended)
  Copy and paste this to Claude:

─────────────────────────────────────────────────────────────
Please read the context file and help me update CLAUDE.md based on the user prompt.
Read: .claude/tmp/claude_context_[timestamp].md
Then generate the appropriate content and tell me exactly where to insert it.
─────────────────────────────────────────────────────────────

Option 2: Manual Edit
  1. Review the context: cat .claude/tmp/claude_context_20260113-143022.md
  2. Edit CLAUDE.md: $EDITOR CLAUDE.md
  3. Save and commit

Option 3: Generate Template
  Template created: .claude/tmp/template_20260113-143022.md
  Edit template: $EDITOR .claude/tmp/template_20260113-143022.md
  Then append: cat .claude/tmp/template_20260113-143022.md >> CLAUDE.md

═══════════════════════════════════════════════════════════════

Would you like to see the context file now? (y/n)
```

## 🎨 Features

### Intelligent Section Detection

The tool analyzes your prompt and suggests relevant sections:

| Keywords | Suggested Section |
|----------|-------------------|
| test, testing | Testing Infrastructure |
| build, cmake, compile | Build System |
| debug, troubleshoot | Debugging Tips |
| optimizer, query | Query Optimizer |
| storage, engine | Storage Engines |

### Automatic Backup

Every run creates a timestamped backup:
```bash
CLAUDE.md.backup.20260113-143022
```

Restore anytime:
```bash
cp CLAUDE.md.backup.20260113-143022 CLAUDE.md
```

### Context Generation

Creates a structured context file that includes:
- Your original prompt
- Current CLAUDE.md structure
- Section analysis
- Output format template

### Template Generation

Provides a ready-to-use Markdown template:
```markdown
---

## [Your Section Title Here]

### Overview
[Brief description]

### Key Points
- Point 1
- Point 2

### Example
```cpp
// Code example
```

### Related Sections
- See [Related](#related)

---
```

## 💡 Tips

### Write Clear Prompts

✅ **Good prompts:**
```
"add a section explaining the catalog metadata schema"
"update debugging tips to include gdb usage examples"
"add performance benchmarks for the optimizer"
```

❌ **Vague prompts:**
```
"update docs"
"make it better"
"add stuff"
```

### Be Specific About Location

```
"add to the Testing Infrastructure section: examples of stress testing"
"in the Optimizer section, add details about cost model tuning"
"append to Tips for AI Assistants: common pitfalls in transactions"
```

### Request Examples

```
"add code examples showing how to use the catalog API"
"include example queries for the storage engine section"
"add shell command examples for debugging"
```

## 🔧 Advanced Usage

### Chaining with Git

```bash
# Update, review, and commit in one workflow
./.claude/scripts/claude-md-prompt.sh "add section X" && \
  git diff CLAUDE.md && \
  read -p "Commit? (y/n) " confirm && \
  [[ $confirm == "y" ]] && git add CLAUDE.md && git commit -m "docs: Add section X"
```

### Using with Aliases

Add to your `.bashrc` or `.zshrc`:
```bash
alias claude-prompt='~/.claude/scripts/claude-md-prompt.sh'
alias claude-update='~/.claude/scripts/update-claude-md.sh'
```

Then use:
```bash
claude-prompt "your request"
```

### Integration with Claude Code

Create a custom Claude Code command:
```json
{
  "commands": {
    "update-docs": {
      "description": "Update CLAUDE.md with prompt",
      "command": ".claude/scripts/claude-md-prompt.sh",
      "acceptsArgs": true
    }
  }
}
```

## 📚 Related Commands

- **`/update-claude-md`** - Auto-update statistics only
- **`/claude-interactive`** - Full interactive mode with editor
- **`git diff CLAUDE.md`** - Review changes before committing

## 🐛 Troubleshooting

### Script not found
```bash
chmod +x .claude/scripts/claude-md-prompt.sh
```

### No editor available
```bash
export EDITOR=nano  # or vim, vi, etc.
```

### Context file not created
```bash
# Check temp directory
ls -la .claude/tmp/

# Create if needed
mkdir -p .claude/tmp/
```

## 📖 Learn More

- See [README.md](./.claude/README.md) for full documentation
- See [USAGE.md](./.claude/USAGE.md) for other commands
- See [CLAUDE.md](../../CLAUDE.md) for the main documentation

---

**Happy documenting! 📝✨**
