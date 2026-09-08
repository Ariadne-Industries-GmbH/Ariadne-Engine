---
name: find-chats-and-context
description: Find and read past chats, messages, other contexts, subagent runs, and durable facts or decisions across the user's history.
tags: [chats, kontexte, messages, subagent-runs, recall]
tools:
  - search_message_chains
  - get_contexts_overview
  - get_context_details
  - get_subagent_execution_trace_by_initiating_message
  - read_agent_audit_log
  - search_background_processes
  - search_longterm_memory
---

# Goal
Recover useful past context instead of answering from guesswork. Use this skill when the user refers to earlier work, another chat, another context, a subagent run, or a remembered fact that is not visible in the current chat.

## Use when
- The user says things like "we discussed this before", "what was the last status", "did we already decide this", or "in the other project...".
- You need message content from another chat, from your own current chat, or from any of the user's chats.
- You need to see which contexts and chats exist, or what a specific context contains.
- You need the outcome or trace of an earlier subagent run.
- You need to confirm whether a stable fact, preference, or decision is already stored in long-term memory.

## Tool Map
- `search_message_chains`: message-level text search and chain reading. The workhorse for "what was said where".
- `get_contexts_overview`: browse all contexts (names + keys). Entry point when you do not know the context.
- `get_context_details`: one context in depth - metadata, linked chats (names + keys), linked skills, tools, files, and LTM spaces.
- `get_subagent_execution_trace_by_initiating_message`: full trace of a subagent run from the tool-call message key that started it.
- `read_agent_audit_log`: action/audit history with filters (action, tool, status, since/until, chat, context) - "what was done where and when".
- `search_background_processes`: locate background processes and runs by name, type, or status.
- `search_longterm_memory`: durable facts, preferences, and decisions. When the `explore-longterm-memory` skill is loaded, follow its strategy for detailed LTM work.

## Workflow
1. Check the current chat context first; answer from it if it already contains the answer.
2. Known chat or context key: run `search_message_chains` scoped with `chat_key` or `context_key`.
3. Unknown context: `get_contexts_overview` to list contexts, then `get_context_details` for the candidate, then scope `search_message_chains` with one of its chat keys.
4. Subagent runs: find the tool-call message that started the run via `search_message_chains`, then call `get_subagent_execution_trace_by_initiating_message` with that message key.
5. Durable facts, preferences, and settled decisions: `search_longterm_memory`.
6. Action-level recall (which tools were run, where, when): `read_agent_audit_log`. Live or finished background runs: `search_background_processes`.
7. If sources disagree, say so and surface the conflict instead of silently choosing one.

## Search Strategy
- `search_message_chains` is an AND search: every term must occur in the same message. Start narrow with names, project terms, decision words, or exact phrases; broaden only when a search returns nothing.
- Scope with `chat_key` or `context_key` as soon as you know them to cut noise.
- Continue paginated results with the returned `cursor`; keep `limit` between 10 and 20.
- Narrow `read_agent_audit_log` results with its filters (`action`, `tool`, `status`, `since`/`until`) instead of scanning raw entries.
- For long-term memory, use focused natural-language queries; add `reference_time_iso` only when the user anchors the request to a specific past time.

## Output Rules
- Report where the answer came from: chat (name/key), context, subagent trace, audit log, or long-term memory.
- Quote or summarize only the evidence needed to answer the user.
- If nothing reliable is found, say that clearly and offer the closest partial context.
