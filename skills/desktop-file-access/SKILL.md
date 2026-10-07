---
name: desktop-file-access
description: Read, inspect, search, create, edit, move, copy, and delete local files and folders through controlled filesystem and terminal tools. Read binary files.
tags: [dateisystem, lokale-dateien, desktop-automation, dateizugriff, shell]
tools:
  - fs_read_command
  - fs_write_command
  - edit_file
  - write_file
  - exec_terminal_command
---

# Goal

Use this skill when the user wants to interact with the local filesystem of the configured desktop or project environment. It provides controlled local file access for reading text files, inspecting binary documents, searching folders, and performing simple filesystem mutations such as creating, copying, moving, or deleting files and directories.

The backend enforces allowed roots, permissions, approvals, and write access. The assistant should focus on choosing the right command and using explicit absolute paths.

## Rule 1

Check if an AGENTS.md (alternatively CLAUDE.md) is available in the root of the Repository or Directory you are going to work on.
If so, ALWAYS read it first and follow its instructions.

## Tool Selection

### `fs_read_command`

Use this first for read-only filesystem inspection. It supports a strict subset of shell-like read commands:

- `ls`
- `cat`
- `head`
- `tail`
- `sed -n`
- `wc`
- `stat`
- `readlink -f`
- `find`
- `rg`, including `rg --files`
- `grep`

Use it for directory discovery, text inspection, exact search, semantic preparation before edits, and safe evidence gathering.

### `fs_write_command`

Use this for simple filesystem mutations after the user intent is clear. It supports:

- `mkdir`
- `touch`
- `cp`
- `mv`
- `rm`

By default, `rm` moves paths to the operating system trash. Use `rm --permanent` only when the user explicitly wants irreversible deletion.

### `edit_file`

Use this to change parts of one existing UTF-8 text file without rewriting it whole. It is anchor-based: every edit gives a one-based `start_line` (a positional hint, matching may drift nearby), an `expected_text` anchor, and the full `replacement` for that span. Read the file with `fs_read_command` first to get current line numbers and content. All edits in one call apply atomically to the same file state and must not overlap. Do not send legacy fields such as `revisions`, `end_line_exclusive`, or `old_text`; they are not part of this API and cause a binding error.

### `write_file`

Use this to create a new text file or fully regenerate an existing one. `mode="overwrite"` replaces the whole file, `mode="append"` adds content at the end without reconstructing prior content. Prefer `edit_file` when only part of an existing file changes; reserve `write_file` for new files or small files rewritten from scratch.

### `exec_terminal_command`

Use this when normal command execution is needed and the strict filesystem tools are not enough, for example:

- running project scripts,
- executing tests,
- invoking formatters or build tools,
- inspecting tool help,
- using commands outside the supported `fs_read_command` or `fs_write_command` subset.

Prefer `fs_read_command` and `fs_write_command` for normal local file access because they are narrower and easier to audit.

## Path Rules

- Use explicit absolute paths in filesystem commands.
- Do not rely on relative paths for `fs_read_command` or `fs_write_command`.
- Quote paths when they contain spaces or shell-sensitive characters.
- Use `readlink -f` when a user gives a path that may be a symlink and the resolved location matters.
- Let the backend handle allowed roots, access checks, approvals, and denied paths.

## Reading Workflow

1. Start by orienting with `ls`, `find`, or `rg --files` on the relevant absolute directory.
2. Use `stat`, `wc`, `head`, `tail`, or `sed -n` to inspect size, type, and relevant excerpts before reading large files fully.
3. Use `rg -n` for code and text search when possible.
4. Use `cat` only when the file is small enough or when you intentionally need the file payload or document metadata.
5. Increase or lower `max_output_chars` based on the task, but do not dump huge files unnecessarily.

## Binary and Document Reading

`fs_read_command` has a special MIME-aware `cat` behavior:

- Text files return normal shell-like stdout.
- Image files emit a short text note plus an inline image payload.
- Supported Office, PDF, image, and similar binary documents emit a short text note plus structured attachment metadata for downstream document tooling.

Use `cat /absolute/path/to/file` when the user wants to open or ingest a supported local binary document. This is the preferred way to hand local binary documents to later document-analysis, indexing, or extraction workflows.

Do not assume binary files will print meaningful text. If `cat` returns structured metadata instead of text, treat that as the successful read surface and continue with the appropriate downstream document tooling if available.

## Writing Workflow

1. Confirm the intended mutation from the user request and identify the exact absolute target paths.
2. To create or change the content of a text file, use `edit_file` (surgical anchor edits) or `write_file` (whole file); use `fs_write_command` only for the path-level operations below.
3. Use `mkdir -p` before writing workflows that require missing parent directories.
4. Use `cp` for duplication and `mv` for rename or relocation.
5. Use `rm` for trash deletion when the user asks to delete something.
6. Use `rm --permanent` only when irreversible deletion was explicitly requested.
7. After a mutation, verify the result with `fs_read_command`, usually `ls -la`, `stat`, or `find`.

## Terminal Workflow

1. Use `exec_terminal_command` only when the strict filesystem tools are insufficient.
2. Keep commands scoped to the relevant project or folder.
3. Prefer read-only terminal commands before modifying anything.
4. Avoid destructive shell operations unless the user clearly requested them and safer tools are not adequate.
5. For code or project tasks, inspect files first, then run targeted validation commands.

## Safety and Output Rules

- Do not invent local paths. Discover them or use paths provided by the user.
- For text content changes prefer `edit_file` / `write_file`; do not rewrite file content with in-place `sed -i` in the terminal, which can corrupt files and mishandle quoting.
- Do not modify files when the user only asked to inspect or explain them.
- Before broad deletion, moving, or overwriting, prefer showing the matched paths first unless the request is already precise.
- Report what was read or changed using concise path-based summaries.
- If access is denied, a command is unsupported, or a path is outside the allowed root, state the limitation and choose the nearest safe alternative.
- When a binary document was read through MIME-aware `cat`, say that the document payload or metadata was obtained, not that the raw binary text was read.
