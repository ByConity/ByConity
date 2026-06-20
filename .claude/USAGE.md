# Quick Usage Guide - Claude Code Custom Commands

## Update CLAUDE.md

To update the CLAUDE.md documentation with current project statistics:

### Method 1: Direct Script Execution (Recommended)

```bash
# From project root
./.claude/scripts/update-claude-md.sh
```

### Method 2: Via Bash

```bash
bash .claude/scripts/update-claude-md.sh
```

### Method 3: Using Claude Code (if supported)

```
/update-claude-md
```

## What Gets Updated

- 📊 **Total Source Files**: Counts `.cpp` and `.h` files in `/src`
- 📁 **Major Modules**: Counts top-level directories in `/src`
- 🚀 **Programs**: Counts executable programs in `/programs`
- 📦 **External Dependencies**: Counts libraries in `/contrib`
- 🧪 **Test Categories**: Counts test directories in `/tests/queries`
- 📅 **Last Updated Date**: Sets to current date

## Safety Features

- ✅ **Automatic Backup**: Creates timestamped backup before updating
- ✅ **Atomic Updates**: Uses temp file to prevent corruption
- ✅ **Diff Display**: Shows changes made for review
- ✅ **Error Handling**: Exits cleanly on errors

## Example Output

```
🔍 Scanning ByConity project structure...
📂 Project root: /home/user/ByConity
💾 Backup created: CLAUDE.md.backup.20260113-131825

📊 Gathering project statistics...
  ✓ Source files: 7072
  ✓ Source modules: 45
  ✓ Programs: 23
  ✓ Contrib dependencies: 204
  ✓ Test categories: 20
  ✓ Current date: 2026-01-13

📝 Updating CLAUDE.md...
✅ CLAUDE.md updated successfully!

📋 Summary of updates:
  • Source files: 7072
  • Major modules: 45
  • Programs: 23
  • External dependencies: 204+
  • Test categories: 20+
  • Last updated: 2026-01-13

📊 Changes made:
[diff output showing specific line changes]

✨ Done!
```

## Troubleshooting

### Permission Denied

```bash
chmod +x ./.claude/scripts/update-claude-md.sh
```

### Restore from Backup

```bash
# Find your backup
ls -lh CLAUDE.md.backup.*

# Restore
cp CLAUDE.md.backup.20260113-131825 CLAUDE.md
```

## When to Run

Run this command:
- After adding new modules
- After adding new programs
- After updating dependencies
- Before creating documentation PRs
- Periodically (monthly/quarterly)

## Git Workflow

```bash
# 1. Update CLAUDE.md
./.claude/scripts/update-claude-md.sh

# 2. Review changes
git diff CLAUDE.md

# 3. Commit (if changes look good)
git add CLAUDE.md
git commit -m "docs: Update CLAUDE.md statistics"

# 4. Push
git push origin your-branch-name
```

---

For more details, see [README.md](./.claude/README.md)
