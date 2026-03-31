---
name: use-longterm-memory
description: Retrieve or store persistent facts in long-term memory when chat context alone is not enough.
tools: [search_longterm_memory, create_longterm_memory]
---

# Goal
Use long-term memory for persistent facts, preferences, progress, and time-anchored information. Do not use it when the current chat already answers the question.

## Use when
- The user refers to past interactions, prior decisions, or stored preferences.
- You need persistent information that may outlive the current chat.
- You want to store a new fact that should be retrievable later.

## Retrieval Workflow
1. Check whether the current chat already contains the answer.
2. If not, use `search_longterm_memory`.
3. Add `reference_time_iso` when the user refers to a specific past time.
4. Use `dataspace_ids` only when you need to limit the search scope.

## Write Workflow
1. Store information only when it is worth keeping beyond the current exchange.
2. Before calling `create_longterm_memory`, make sure the text contains:
   - at least two concrete entities
   - at least one explicit relation
   - a declarative statement
   - resolved pronouns and time references where possible
3. If the snippet is too weak, extend it with a small amount of surrounding context.
4. Prefer one larger, well-formed text over many tiny fragments.

## Rules
- `reference_time_iso` describes when the remembered event happened, not when you are calling the tool.
- `dataspace_id` and `group_id` refer to the same scope concept.
- Do not store generic, hypothetical, unresolved, or single-entity snippets.
- Recent chat turns are usually more relevant than old memory entries.

## Examples
- Retrieval: search for a stored theme preference or a past project decision.
- Write: store a resolved preference or status update with enough context to make the fact reusable later.
