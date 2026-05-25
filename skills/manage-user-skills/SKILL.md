---
name: manage-user-skills
description: Inspect available engine capabilities and create, read, update, or delete user-defined skills so the assistant can load the right tools on demand.
tags: [skill-verwaltung, benutzer-skills, skill-management, skill-erstellung]
tools:
  - get_available_agent_capabilities
  - fs_read_command
  - build_and_save_user_skill
  - update_user_skill
  - delete_user_skill
---

# Manage User Skills

## Purpose

Use this skill when existing loaded skills, global skills, and discovered user-skill frontmatter are not enough to solve the task cleanly.

This skill is the main gateway for inspecting available internal capabilities and turning them into reusable user skills. It can:
- create new user skills,
- read existing user skills,
- update existing user skills,
- and delete obsolete user skills.

User-created skills are stored in the active user's storage area and are discovered by identity key.

When talking to non-technical users, explain MCP servers as "plugins" or "plugin integrations" first, and only use the technical MCP wording when it is actually helpful.

## What This Skill Is For

Use this skill when one of these is true:
- the user wants the assistant to perform tasks that require tools, but no fitting skill is already available,
- an existing user skill is close to the solution but needs different tools or better instructions,
- the user wants to inspect, repair, or delete a user skill,
- the assistant needs to understand what tools, subagents, flows, or MCP/plugin capabilities exist before proposing a reusable workflow.

Do not treat this skill as a form-filling interview. Its job is to infer as much as possible from the current request and context, inspect capabilities, and move directly toward a concrete skill draft or update.

## Capability Model

This skill supports user skills that use:
- internal engine tools,
- internal subagents,
- internal flows,
- MCP tools via namespaced tool references plus a frontmatter `mcps` list,
- or a combination of internal capabilities and MCP tools.

Important flow restriction:
- If a skill references one or more flows, that skill must be flow-only.
- A flow skill must not also reference internal tools, subagents, or MCP tools.
- A flow skill must not declare `mcps`.
- Reason: a flow takes over execution and returns its own result, so mixing flows with normal callable tools is not a coherent runtime model.

## Tool Overview

- `get_available_agent_capabilities`
  - Primary discovery tool for internal tools, subagents, flows, and MCP/plugin servers.
  - `mode="overview"` is the compact default. It returns counts plus paginated MCP server summaries.
  - `mode="internal_tools"` browses internal tools, subagents, and flows with pagination.
  - `mode="mcp_tools"` browses tools for one specific MCP/plugin server with pagination.
  - Only request `include_descriptions=true` or `include_parameters=true` when you really need the extra detail.

- `build_and_save_user_skill`
  - Preferred structured tool for creating a new user skill.
  - Builds normalized markdown, validates it, and saves it.

- `update_user_skill`
  - Preferred structured tool for updating an existing skill.
  - Validates and overwrites the skill, but does not create missing skills.

- `fs_read_command`
  - Reads and inspects existing `SKILL.md` files through a shell-like read-only interface.
  - Use commands like `cat`, `head`, `sed -n`, `find`, or `rg --files` inside the allowed user-skill root.

- `delete_user_skill`
  - Deletes a user skill after removing its links from user contexts.

## Default Workflow

1. Understand the task with minimal friction.
   - Infer the likely goal from the user's request and the current conversation.
   - Do not start with a long questionnaire.
   - Ask only for information that is truly blocking correct skill construction or update.

2. Inspect capabilities before designing the skill.
   - Start with `get_available_agent_capabilities(mode="overview")`.
   - If internal capabilities matter, browse `mode="internal_tools"` with pagination.
   - If MCP/plugins matter, browse `mode="mcp_tools"` for the relevant server with pagination.
   - Use returned names and `skill_tool_reference` values exactly as provided.

3. Decide whether to create or update.
   - If a fitting user skill already exists and only needs refinement, update it.
   - If a user skill exists but you need to inspect the exact content first, use `fs_read_command` with a shell-like read command such as `cat /allowed/user-skill-root/<skill_name>/SKILL.md`.
   - If no fitting user skill exists, create a new one.

4. Produce a concrete skill, not just commentary.
   - Propose or infer a short lowercase-hyphen skill name.
   - Write a short frontmatter description that says what the skill does and when to use it.
   - Write the markdown body as operational guidance:
     - purpose,
     - when to use,
     - strategy/workflow,
     - examples,
     - failure handling.

5. Save with the most structured tool that fits.
   - For new skills, prefer `build_and_save_user_skill`.
   - For existing skills, prefer `update_user_skill`.

6. Confirm the outcome and next action.
   - State whether the skill was created, updated, read, or deleted.
   - Name the affected skill.
   - If relevant, remind the user that the assistant can later discover or load the skill through the `skill` tool.

## Canonical Frontmatter

Use this frontmatter shape:

```yaml
---
name: my-skill-name
description: Short summary of what this skill does and when to use it.
tools:
  - internal_tool_name
  - another_internal_or_subagent_tool
  - internal_flow_name
  - mcp:my-mcp-key:search
  - mcp:my-mcp-key:scrape
  - mcp:second-mcp-key:lookup
mcps:
  - my-mcp-key
  - second-mcp-key
tags:
  - optional
  - skill-tag
---
```

Put the detailed workflow and examples into the markdown body, not into the frontmatter.

Optional Dreaming control:
- You may set `metadata.ariadneanyverse.de.dreaming: "true"` to exclude a skill from the Dreaming `skill_review` phase.
- Omitted metadata is treated like `false`, so Dreaming may still review the skill.
- If you set the key explicitly, use only `true` or `false` semantics. The structured tools also support this through the optional `exclude_from_dreaming` parameter.

## Important Rules

- For MCP-backed skills, frontmatter must use `mcps` as a list of MCP server keys.
- If any tool in frontmatter `tools` comes from an MCP, that MCP key must be present in frontmatter `mcps`.
- MCP tools in frontmatter `tools` must be namespaced as `mcp:<mcp_key>:<tool_name>`.
- Internal tools, subagents, and flows are referenced directly by name.
- Flow-based skills are exclusive and must not mix with normal tools or MCP capabilities.
- Flow-based skills must not declare any `mcps`.
- Duplicate entries in `tools` or `mcps` are invalid.
- The old single `mcp` field is obsolete and invalid.
- Keep frontmatter `description` short and precise.
- `metadata.ariadneanyverse.de.dreaming` is optional. If present, it must resolve to `true` or `false`.
- Read existing skills through `fs_read_command` from the allowed user-skill root and keep writes on the structured skill-authoring tools.

## Runtime Behavior You Must Explain Correctly

- If a declared MCP has namespaced MCP tool entries in `tools`, only those MCP tools are loaded for that MCP.
- If a declared MCP has no namespaced MCP tool entries in `tools`, all tools from that MCP are loaded.
- MCP tools are exposed at runtime with the engine postfix (`<tool_name>_<mcp_key>`), but frontmatter must still use the namespaced skill format (`mcp:<mcp_key>:<tool_name>`).
- If an MCP or requested MCP tool is missing later, completion generation should still continue.
- The runtime injects warning text into the skill context so the assistant can inform the user that parts of the skill are unavailable.

## Validation and Error Handling

- Treat validation errors as actionable feedback, not as conversation failure.
- Read the exact returned errors and correct them directly.
- Common issues include:
  - invalid YAML frontmatter,
  - wrong field names such as `mcp` instead of `mcps`,
  - non-namespaced MCP tool references,
  - MCP keys missing from `mcps`,
  - missing MCPs or missing MCP tools.

## Collaboration Style

- Be decisive. Move from user intent to capability discovery to a concrete skill draft quickly.
- Prefer doing the discovery work yourself instead of asking the user to enumerate tools.
- Compare options briefly when needed, then recommend the smallest reliable toolset.
- Do not invent tool names or MCP keys. Always derive them from discovery output.
- Use repeated paginated discovery calls instead of trying to load everything at once.
- If the user wants to modify an existing skill, inspect `<allowed_user_skill_root>/<skill_name>/SKILL.md` with `fs_read_command` before changing it.
- If an MCP/plugin no longer exists, explain that clearly and adapt the skill design to currently available capabilities.
- When deleting a skill, explain that linked contexts are cleaned up first so broken links are not left behind.
