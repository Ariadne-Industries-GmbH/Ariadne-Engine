---
name: find-past-context
description: Find relevant past chats, prior decisions, durable facts, and earlier subagent results across the user's history.
tags: [chats, memory, vergangenheit, kontext-suche, erinnerung]
tools:
  - search_message_chains
  - search_longterm_memory
---

# Goal
Recover useful past context before answering from guesswork. Use this skill when the user refers to earlier work, prior conversations, or a remembered fact that is not visible in the current chat.

## Use when
- The user says things like "we discussed this before", "what was the last status", or "did we already decide this".
- You need to find information from another chat, another run, or an earlier subagent trace.
- You need to confirm whether a stable fact or preference was already stored in long-term memory.

## Workflow
1. Check whether the current chat already contains the answer.
2. Use `search_message_chains` first for prior conversations, execution traces, and message-level evidence.
3. Search long-term memory with `search_longterm_memory` for durable facts, preferences, and decisions that may outlive any single chat.
4. Prefer message-chain evidence for recent or task-specific details.
5. Prefer long-term memory for stable facts, preferences, and already-settled decisions.
6. If both sources disagree, say so and surface the conflict instead of silently choosing one.

## Search Strategy
- Start narrow with names, project terms, decision words, or exact phrases.
- Use `include_messages=true` when you need to inspect the matching content instead of just locating a chain.
- Filter `chain_types` when the request is clearly about normal chat history or execution traces.
- For long-term memory, use focused natural-language queries and add `reference_time_iso` only when the user anchors the request to a specific past time.

## Output Rules
- Report where the answer came from: prior chat/execution trace, long-term memory, or both.
- Quote or summarize only the evidence needed to answer the user.
- If nothing reliable is found, say that clearly and offer the closest partial context.
