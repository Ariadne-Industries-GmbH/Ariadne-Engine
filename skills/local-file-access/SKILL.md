---
name: local-file-access
description: Browse configured local folders safely, then open the exact file you need.
tools: [list_local_directory, read_local_file]
tags: [files, local-files, browsing]
---

# Goal
Use this skill to inspect files inside the configured local read-only roots.

## Use when
- The user wants to inspect or read local files.
- You know the needed file is on disk but not yet its exact path.

## Workflow
1. Start with `list_local_directory` to locate the right folder and exact filename.
2. Page through large directories or narrow the path before reading.
3. Once the exact path is known, use `read_local_file`.

## Rules
- If either tool is unavailable, explain that local file access is not enabled in this context.
- Both tools are read-only. They cannot create, edit, move, or delete files.
- Both tools are limited to the configured allowed root directories.
- If multiple allowed roots exist, start with the most relevant root instead of scanning everything.
- If the filename is not known, browse first and do not guess paths.
