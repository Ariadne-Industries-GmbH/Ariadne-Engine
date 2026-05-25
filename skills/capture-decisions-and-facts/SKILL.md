---
name: capture-decisions-and-facts
description: Capture important outcomes as notes, durable memory, or both so they can be reused later.
tags: [notizen, gedächtnis, entscheidungen, präferenzen, erfassen]
tools:
  - search_message_chains
  - create_ai_note
  - update_ai_note
  - search_longterm_memory
  - create_longterm_memory
  - replace_longterm_memory_episode
---

# Goal
Transfer durable knowledge from the current work into the right long-lived form and keep the knowledge graph compact instead of creating avoidable duplicates.

## Use when
- The user asks to remember something.
- A decision, preference, fact, or lesson should survive the current chat.
- You want to capture the outcome of a conversation before it is forgotten.
- Existing remembered knowledge may need to be corrected, merged, or replaced.

## Storage Choice
- Use `AI Notes` for lightweight summaries, task notes, working logs, and project-local context.
- Use `Long-Term Memory` for durable facts, preferences, recurring relationships, and resolved decisions that should be found later across chats.
- Use both when a decision needs a readable working note and a durable structured memory.

## Workflow
1. If useful, inspect recent context with `search_message_chains` so the stored result reflects the actual conversation outcome.
2. Before writing long-term memory, call `search_longterm_memory` with focused queries to find existing episodes, nearby facts, and possible duplicates.
3. Decide whether the outcome belongs in a note, in long-term memory, or in both.
4. Write or update an AI Note when the user wants a readable record or when the information is too soft for structured memory.
5. If the durable knowledge is new, create a new structured episode with `create_longterm_memory`.
6. If an existing episode already represents the same fact cluster but is incomplete, outdated, or conflicting, prefer `replace_longterm_memory_episode` over adding a second competing episode.
7. After writing durable memory, search again when needed to verify that the graph now contains one clear canonical representation instead of multiple near-duplicates.

## Long-Term Memory Rules
- Convert the information into the structured episode schema expected by `create_longterm_memory`.
- Do not store unresolved ideas, weak guesses, or one-off fragments as durable memory.
- Prefer explicit entity names and short declarative statements.
- Treat long-term memory as a shared knowledge graph, not as a raw append-only log.
- `create_longterm_memory` and `replace_longterm_memory_episode` work best with small, focused, semantically coherent entries.
- Prefer iterative writes over one large all-in-one memory payload.

## Graph Compaction Rules
- Search before every durable write.
- Prefer one canonical episode for one resolved knowledge unit.
- Replace an older episode when the new version supersedes it instead of creating parallel memories with the same meaning.
- Keep separate episodes only when the time dimension matters and both states should remain queryable.
- Merge repeated learnings from the same topic into a denser replacement episode when that reduces fragmentation.
- If the evidence is still ambiguous, store only a note and wait for confirmation before changing the graph.

## Output Rules
- Tell the user what was stored and where.
- If you chose not to store something durably, explain why briefly.
