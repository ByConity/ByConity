# Claude Code Configuration for ByConity

This directory contains Claude Code configuration and custom commands for the ByConity project.

## Directory Structure

```
.claude/
├── README.md                     # This file
├── commands.json                 # Custom CLI commands configuration
└── scripts/
    └── update-claude-md.sh       # Script to update CLAUDE.md statistics
```

## Custom Commands

### `/update-claude-md` - Update CLAUDE.md Documentation

This command automatically updates the CLAUDE.md file with current project statistics.

**Usage:**

In Claude Code CLI, you can run:

```bash
# Option 1: Run directly via bash
./.claude/scripts/update-claude-md.sh

# Option 2: Use the custom command (if supported by your Claude Code version)
/update-claude-md
```

**What it updates:**

- ✅ Total source files count in `/src`
- ✅ Number of major modules
- ✅ Number of executable programs in `/programs`
- ✅ Number of external dependencies in `/contrib`
- ✅ Number of test categories
- ✅ "Last Updated" date

**Features:**

- 🔍 Automatically scans project structure
- 💾 Creates timestamped backup before updating
- 📊 Shows detailed statistics and changes
- ✅ Provides clear success/error messages
- 🔄 Atomic file updates (uses temp file)

**Example output:**

```
🔍 Scanning ByConity project structure...
📂 Project root: /home/user/ByConity
💾 Backup created: /home/user/ByConity/CLAUDE.md.backup.20260113-120000

📊 Gathering project statistics...
  ✓ Source files: 7270
  ✓ Source modules: 47
  ✓ Programs: 23
  ✓ Contrib dependencies: 200
  ✓ CMake modules: 25
  ✓ Test categories: 40
  ✓ Current date: 2026-01-13

📝 Updating CLAUDE.md...
✅ CLAUDE.md updated successfully!

📋 Summary of updates:
  • Source files: 7270
  • Major modules: 47
  • Programs: 23
  • External dependencies: 200+
  • Test categories: 40+
  • Last updated: 2026-01-13
```

## Manual Script Execution

You can also run the update script manually:

```bash
# From project root
./.claude/scripts/update-claude-md.sh

# Or with bash
bash .claude/scripts/update-claude-md.sh
```

## Backup and Recovery

The script automatically creates backups before updating. If you need to restore:

```bash
# List available backups
ls -lh CLAUDE.md.backup.*

# Restore from a specific backup
cp CLAUDE.md.backup.20260113-120000 CLAUDE.md
```

## Customization

### Adding More Statistics

To add more statistics to the update script, edit `.claude/scripts/update-claude-md.sh`:

1. Add a new statistics gathering section
2. Add a corresponding `sed` command to update the CLAUDE.md file
3. Update the summary output

### Adding More Commands

To add more custom commands, edit `.claude/commands.json`:

```json
{
  "commands": {
    "your-command-name": {
      "description": "Description of what your command does",
      "command": ".claude/scripts/your-script.sh",
      "runInBackground": false,
      "confirmBeforeRun": false
    }
  }
}
```

## Maintenance

### When to Run

Run the update command when:
- 📦 New modules are added to `/src`
- 🚀 New programs are added to `/programs`
- 📚 External dependencies are updated
- 🧪 New test categories are added
- 📅 Periodic documentation updates (e.g., monthly)

### Best Practices

1. **Review changes**: Always review the diff output before committing
2. **Keep backups**: Don't delete backup files immediately
3. **Commit separately**: Commit CLAUDE.md updates separately from code changes
4. **Update regularly**: Keep documentation in sync with codebase evolution

## Troubleshooting

### Script fails to run

```bash
# Make sure script is executable
chmod +x .claude/scripts/update-claude-md.sh

# Check if CLAUDE.md exists
ls -l CLAUDE.md
```

### Statistics seem incorrect

```bash
# Manually verify counts
find src -type f \( -name "*.cpp" -o -name "*.h" \) | wc -l
find src -mindepth 1 -maxdepth 1 -type d | wc -l
find programs -mindepth 1 -maxdepth 1 -type d | wc -l
```

### Need to restore backup

```bash
# List all backups
ls -lht CLAUDE.md.backup.* | head -5

# Restore most recent
cp CLAUDE.md.backup.$(ls -t CLAUDE.md.backup.* | head -1 | sed 's/CLAUDE.md.backup.//') CLAUDE.md
```

## Contributing

When contributing improvements to these Claude Code utilities:

1. Test thoroughly in a local environment
2. Update this README if adding new commands
3. Follow ByConity coding conventions
4. Document any new dependencies

## Resources

- [Claude Code Documentation](https://docs.anthropic.com/claude/docs)
- [ByConity Documentation](https://byconity.github.io/docs)
- [ByConity GitHub](https://github.com/ByConity/ByConity)

---

**Last Updated:** 2026-01-13
**Maintained By:** ByConity Community
