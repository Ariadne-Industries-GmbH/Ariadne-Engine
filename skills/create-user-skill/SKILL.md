---
name: create-user-skill
description: Create, update, and delete user-defined skills using available internal tools and MCP servers (explain MCP servers to users as plugins).
tools:
  - get_available_skill_creation_resources
  - get_user_skill_markdown
  - build_and_save_user_skill
  - update_user_skill
  - validate_and_save_user_skill_markdown
  - update_user_skill_markdown
  - delete_user_skill
tags:
  - skills
  - authoring
  - mcp
---

# Goal
Create, update, and delete user-scoped skills. When talking to non-technical users, explain MCP servers as plugins first.

## Default Workflow
1. Start with `get_available_skill_creation_resources(mode="overview")`.
2. Browse `internal_tools` or `mcp_tools` only for the capabilities you actually need.
3. Design the smallest skill that solves the user's goal.
4. Prefer `build_and_save_user_skill` for create and `update_user_skill` for updates.
5. Use raw markdown tools only when you need manual control over the full `SKILL.md`.
6. Use `delete_user_skill` only when the user explicitly wants removal.

## Discovery Rules
- Page through results with `limit` and `offset` instead of loading everything at once.
- Request `include_descriptions=true` or `include_parameters=true` only when narrowing down candidates.
- Do not invent internal tool names, MCP keys, or MCP tool names.
- Always copy MCP tool references from discovery results.

## Skill Writing Rules
- Assume the model already understands generic Markdown and general programming advice. Only include instructions that are specific to this skill.
- Prefer headings, flat lists, and short code blocks.
- In skill bodies, do not use typographic emphasis such as bold or italic. Use structure instead.
- Use clear headings for sections.
- Use lists for concise workflow steps, rules, and decisions.
- Use code blocks for examples of payloads, parameter shapes, or exact syntax.
- Keep frontmatter minimal. Use `name`, `description`, and `tools` by default. Add `mcps` only for MCP-backed skills. Add `tags` only when they help categorization.
- Prefer the smallest useful set of `tools` and `mcps`. The runtime uses these declarations to prefilter capabilities and reduce prompt size, which matters especially for local LLMs.
- Keep `description` to one short sentence that says what the skill does and when to use it.
- The body should usually fit into a small set of sections: `Goal`, `Use when`, `Workflow`, `Rules`, and optional `Examples`.
- Do not dump full tool documentation into the skill body. Only document parameters or edge cases that are easy to misuse.
- Prefer explicit workflows and decision rules over long explanatory prose.

## MCP Rules
- Internal tools and subagents go into `tools` by direct name.
- MCP tools go into `tools` as `mcp:<mcp_key>:<tool_name>`.
- Every MCP referenced by a namespaced MCP tool must also be listed in `mcps`.
- Multiple MCPs per skill are supported.
- Skill frontmatter must use the namespaced MCP format, not runtime tool names with engine suffixes.
- If an MCP is listed in `mcps` without explicit namespaced MCP tools in `tools`, runtime may load all tools from that MCP.
- For efficiency, prefer explicit MCP tool references unless the user intentionally wants the whole MCP surface.
- The old single `mcp` field is obsolete. Use `mcps`.

## Canonical Template
````md
---
name: my-skill-name
description: One sentence saying what the skill does and when to use it.
tools:
  - internal_tool_name
  - mcp:my-mcp-key:tool_name
mcps:
  - my-mcp-key
tags:
  - optional-tag
---

# Goal
One short paragraph.

## Use when
- Case 1
- Case 2

## Workflow
1. Step 1
2. Step 2
3. Step 3

## Rules
- Constraint 1
- Constraint 2

## Examples
- Optional short example.
````

## Tool Map
- `get_available_skill_creation_resources`: browse internal tools, MCP servers, and MCP tool references.
- `build_and_save_user_skill`: preferred create path from structured inputs.
- `update_user_skill`: preferred structured update path.
- `validate_and_save_user_skill_markdown`: save raw markdown when you need full manual control.
- `update_user_skill_markdown`: overwrite an existing skill from raw markdown.
- `get_user_skill_markdown`: inspect the current `SKILL.md` before editing.
- `delete_user_skill`: remove a skill after unlinking it from contexts.

## Update and Delete Rules
- For updates, inspect the current skill first with `get_user_skill_markdown` unless the content is already known.
- For deletes, name the exact skill and explain that linked contexts are cleaned up first.
- If validation fails, treat the error as actionable feedback and correct the markdown or frontmatter.
- These authoring tools are scoped to the active user's skill directory. Do not suggest generic filesystem editing as the normal path.

## Validation Rules
- Duplicate `tools` or duplicate `mcps` are invalid.
- Wrong field names such as `mcp` instead of `mcps` are invalid.
- Namespaced MCP tool references must follow `mcp:<mcp_key>:<tool_name>`.
- Unknown internal tools, unknown MCPs, and unknown MCP tools must be corrected before saving.

## User-Facing Behavior
- Explain MCP servers as plugins or plugin integrations first.
- Compare options in plain language and recommend the smallest reliable toolset.
- Remind the user that a skill must be linked to a context, or the context must allow skill discovery, before the runtime can use it.
