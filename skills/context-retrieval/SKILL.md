---
name: context-retrieval
description: Browse available contexts with a compact overview, then inspect one context in detail by key, including chats, tools, MCPs, files, skills, and optional descriptions.
tools: [get_contexts_overview, get_context_details_by_key, get_context_details_by_name]
---

# Goal
Use this skill to inspect available contexts and understand which chats, tools, MCPs, files, and skills are connected to a specific context.

## Use when
- The user asks which contexts exist.
- You need to inspect the current setup before acting.
- A tool, file, MCP, or skill seems to be missing.
- You need to explain or debug context-specific behavior.

## Workflow
1. Call `get_contexts_overview` first to get compact context names and keys.
2. Pick the relevant context key.
3. Call `get_context_details_by_key` for the selected context.
4. Use pagination for large contexts with `chat_limit`, `chat_offset`, `tool_limit`, and `tool_offset`.
5. Analyze only the linked resources relevant to the task.

## Rules
- `get_context_details_by_key` accepts a context key, not a chat key.
- Prefer `get_context_details_by_key` over name lookup when the key is available.
- Use `get_context_details_by_name` only when you do not have the key yet.
- Inspect the context before diagnosing missing tools, MCPs, files, or skills.
- Explain relationships between linked resources when that helps the user act on the result.

## Typical Checks
- For missing tools: inspect linked tools and MCP servers.
- For missing knowledge: inspect linked files and skills.
- For setup questions: inspect chats, tools, MCPs, files, and skill discovery settings together.
