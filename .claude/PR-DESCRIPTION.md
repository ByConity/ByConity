# Add Claude Code CLI Commands for CLAUDE.md Updates

## Summary

This PR adds custom Claude Code CLI commands to intelligently update CLAUDE.md documentation using natural language prompts (similar to the `#` function).

## New Features

### 1. 🎯 `/claude-prompt` - Smart Update with Natural Language
- Accepts natural language prompts like "add section about debugging"
- Analyzes user intent and suggests relevant sections
- Creates structured context files for Claude AI to process
- Generates ready-to-use Markdown templates
- Automatic backups before any changes

**Usage:**
```bash
./.claude/scripts/claude-md-prompt.sh "add debugging tips for TSO issues"
./.claude/scripts/claude-md-prompt.sh --interactive
```

### 2. 💬 `/claude-interactive` - Full Interactive Mode
- Multi-line prompt support (Ctrl+D to finish)
- Built-in editor integration
- Direct content insertion capability
- Automatic section analysis

**Usage:**
```bash
./.claude/scripts/update-with-prompt.sh
```

### 3. 🔄 `/update-claude-md` - Auto-Update Statistics
- Automatically updates project statistics
- Counts source files, modules, programs, etc.
- Updates "Last Updated" date
- Creates timestamped backups

**Usage:**
```bash
./.claude/scripts/update-claude-md.sh
```

## Files Added

- `.claude/scripts/claude-md-prompt.sh` - Main prompt-based update script (217 lines)
- `.claude/scripts/update-with-prompt.sh` - Interactive mode script (210 lines)
- `.claude/scripts/update-claude-md.sh` - Statistics update script (128 lines)
- `.claude/commands.json` - Command configurations
- `.claude/README.md` - Comprehensive documentation (262 lines)
- `.claude/USAGE.md` - Quick usage guide (120 lines)
- `.claude/PROMPT-GUIDE.md` - Detailed prompt examples and tips (314 lines)

## Files Updated

- `.gitignore` - Added patterns for backup files and temp directories
- `CLAUDE.md` - Updated statistics (2026-01-13)

## Example Workflows

### Quick Documentation Update
```bash
# User provides a prompt
./.claude/scripts/claude-md-prompt.sh "add performance tuning tips for the optimizer"

# Script creates context file and template
# User can then:
# Option A: Ask Claude to read context and generate content
# Option B: Edit the template manually
# Option C: Use interactive mode
```

### Statistics Update
```bash
# One command updates all statistics
./.claude/scripts/update-claude-md.sh

# Output shows:
# ✓ Source files: 7072
# ✓ Major modules: 45
# ✓ Programs: 23
# ✓ Test categories: 20+
# ✓ Last updated: 2026-01-13
```

## Benefits

- 🤖 **Natural Language Interface**: Works like the `#` function, accepts conversational prompts
- 📝 **Reduced Friction**: Makes documentation updates easier and faster
- 📋 **Structured Context**: Provides Claude with organized information for better responses
- 💾 **Safe Operations**: Automatic backups before every change
- 🎨 **User-Friendly**: Color-coded output and clear instructions
- 🎯 **Smart Analysis**: Keywords detection for relevant section suggestions
- 📊 **Auto Statistics**: Keep documentation metadata up-to-date automatically

## Testing Results

All scripts have been tested successfully:
- ✅ Prompt-based command works correctly
- ✅ Context file generation verified
- ✅ Template generation confirmed
- ✅ Statistics update tested and working
- ✅ Backup mechanism validated
- ✅ Color output displays correctly
- ✅ Interactive mode flows properly

## Technical Details

### Command Configuration (`.claude/commands.json`)
```json
{
  "commands": {
    "update-claude-md": {
      "description": "Update CLAUDE.md with current project statistics",
      "command": ".claude/scripts/update-claude-md.sh"
    },
    "claude-prompt": {
      "description": "Update CLAUDE.md using natural language prompt",
      "command": ".claude/scripts/claude-md-prompt.sh",
      "acceptsArgs": true
    },
    "claude-interactive": {
      "description": "Interactive mode for updating CLAUDE.md",
      "command": ".claude/scripts/update-with-prompt.sh"
    }
  }
}
```

### Safety Features
- Timestamped backups before every operation
- Atomic file updates using temp files
- Clear diff output for verification
- No destructive operations without confirmation

### Documentation Coverage
- Quick start guide (USAGE.md)
- Comprehensive reference (README.md)
- Example-driven tutorial (PROMPT-GUIDE.md)
- Inline script documentation

## Commits

1. `ea2d7695` - feat: Add custom Claude Code CLI command to update CLAUDE.md
   - Initial implementation of statistics update script
   - Basic command configuration
   - Initial documentation

2. `bbc05a62` - feat: Add prompt-based Claude Code commands for CLAUDE.md updates
   - Added natural language prompt processing
   - Interactive mode with editor integration
   - Comprehensive documentation and examples

## Related Issues

This PR enhances the documentation workflow by providing:
- Natural language interface for documentation updates (addresses need for easier doc maintenance)
- Automated statistics tracking (keeps metadata current)
- Safe update mechanism (prevents documentation corruption)

## Screenshots/Examples

Example of prompt-based update:
```
$ ./.claude/scripts/claude-md-prompt.sh "add debugging tips for performance issues"

╔═══════════════════════════════════════════════════════════════╗
║     📝 CLAUDE.MD Smart Update Tool (# Function)              ║
╚═══════════════════════════════════════════════════════════════╝

✅ Prompt received: add debugging tips for performance issues
💾 Backup created: CLAUDE.md.backup.20260113-132838

📄 Context file created: .claude/tmp/claude_context_20260113-132838.md

Option 1: Use Claude Code (Recommended)
  Copy and paste this to Claude:
  "Please read the context file and help me update CLAUDE.md..."
```

## Documentation

Full documentation available in:
- `.claude/README.md` - Complete reference
- `.claude/USAGE.md` - Quick start
- `.claude/PROMPT-GUIDE.md` - Examples and tips

## Breaking Changes

None. This PR only adds new functionality without modifying existing code.

## Checklist

- [x] Code follows project conventions
- [x] Scripts are executable and tested
- [x] Documentation is comprehensive
- [x] Backup mechanisms in place
- [x] .gitignore updated appropriately
- [x] All commits have clear messages
- [x] No sensitive information exposed

## Future Enhancements

Potential future improvements:
- Integration with CI/CD for automatic statistics updates
- Web interface for documentation updates
- AI-powered content suggestions
- Version control for documentation history
- Multi-language documentation support
