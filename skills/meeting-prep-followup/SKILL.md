---
name: meeting-prep-followup
description: Prepare for meetings, gather relevant history, and capture follow-ups, decisions, and reminders afterward.
tags: [vorbereitung, nachbereitung, protokoll, besprechung]
tools:
  - search_message_chains
  - search_longterm_memory
  - create_longterm_memory
  - replace_longterm_memory_episode
  - search_in_embedding_collections
  - get_document_content_sample
  - search_for_document_content
  - browse_document_content
  - create_ai_note
  - update_ai_note
  - create_scheduled_job
  - get_all_scheduled_jobs
  - update_scheduled_job
  - remove_job
---

# Goal
Support the full meeting cycle: prepare beforehand, summarize afterward, and turn outcomes into durable follow-ups.

## Use when
- The user asks for preparation before a meeting, check-in, or call.
- The user wants a post-meeting summary, decision log, or action list.
- The user wants reminders or scheduled checks related to meeting outcomes.

## Preparation Workflow
1. Use `search_message_chains` to gather recent discussion, prior commitments, and unresolved issues.
2. Use `search_longterm_memory` for stable preferences, ongoing relationships, or past decisions.
3. Search indexed knowledge or meeting documents when the discussion depends on written material.
4. Produce a short prep brief with goals, prior context, likely questions, and open risks.

## Follow-up Workflow
1. Capture the important outcomes in AI Notes.
2. Search for prior stored decisions or facts when the meeting may confirm, supersede, or correct them.
3. Store durable decisions or stable new facts in long-term memory when they should matter later.
4. If the meeting clearly replaces or corrects an earlier remembered decision, prefer `replace_longterm_memory_episode` over creating a second conflicting memory.
5. Turn explicit follow-ups into scheduled jobs when the user wants a reminder, recurring check, or delayed review.
6. Update or remove existing scheduled jobs when the meeting changed the plan.

## Decision Rules
- Use AI Notes for summaries, raw notes, and project-local working records.
- Use long-term memory for durable decisions, stable facts, and reusable preferences.
- Use `create_longterm_memory` and `replace_longterm_memory_episode` in small, focused entries. Do not try to store the whole meeting in one giant episode.
- Use scheduled jobs only when the user wants future action, a reminder, or a recurring review.

## Output Rules
- Separate `Preparation` from `Follow-up`.
- Always identify open questions and clear owners when they are known.
- Do not silently schedule anything unless the user explicitly wants a reminder or follow-up action.
