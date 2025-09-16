# Claude Code Project Configuration

*This configuration is intended to help Claude Code to work efficiently with this large repository by avoiding unnecessary file system operations.*

## Repository Overview
This is a large repository (approximately 6GB) with complex directory structure, mostly for Chinese classic literatures. Performance and efficiency are critical when working with this codebase.

## Important Performance Guidelines

### ⚠️ Large Directory Restrictions

**DO NOT browse or analyze these large directories unless explicitly requested:**

1. `佛藏/`
2. `儒藏/`
3. `医藏/`
4. `史藏/`
5. `子藏/`
6. `易藏/`
7. `艺藏/`
8. `诗藏/`
9. `道藏/`
10. `集藏/`

### Working Principles

- **Minimal File Access**: Only access files when explicitly mentioned or absolutely necessary
- **Targeted Operations**: Work on specific files I point out rather than exploring directories
- **Ask Before Browsing**: If you need to explore large directories, ask permission first
- **Focus on Essentials**: Prioritize understanding project structure from key configuration files rather than deep directory traversal

### Preferred Workflow

1. **Start Small**: Begin with root-level files (package.json, README.md, etc.)
2. **Wait for Direction**: Let me specify which files or directories to work on
3. **Targeted Changes**: Make precise edits to specified files only
4. **Confirm Scope**: Ask if you're unsure whether to access a particular directory
5. **Reference File Lists**: If needed, Claude can partially examine `all_files.txt` to understand directory structure (note: this file contains a snapshot from a specific time period and may not be completely accurate)

## Project Context

- **Size**: ~6GB repository
- **Structure**: Multiple large subdirectories with various purposes
- **Usage Pattern**: Selective file editing rather than full codebase analysis
- **Performance Priority**: Speed and minimal resource usage
- **Primary Task**: Claude Code assists with modifying scripts in the `scripts/` directory. These scripts are designed to help update and maintain files within the 10 large directories (佛藏/, 儒藏/, 医藏/, 史藏/, 子藏/, 易藏/, 艺藏/, 诗藏/, 道藏/, 集藏/)

## Communication

- Always confirm the scope before large operations
- Ask for specific file paths when you need to make changes
- Use `find` or `ls` commands sparingly and only when necessary
- Focus on the immediate task rather than comprehensive codebase understanding
