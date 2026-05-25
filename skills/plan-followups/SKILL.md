---
name: plan-followups
description: Plan reminders, recurring checks, delegated subagent work, and future follow-ups that should happen after this chat.
tags: [nachverfolgung, planung, follow-up, wiederkehrend]
tools:
  - create_scheduled_job
  - get_all_scheduled_jobs
  - update_scheduled_job
  - remove_job
  - delegate_subagent_task
  - get_subagent_execution_trace_by_initiating_message
  - inspect_background_process
  - search_background_processes
  - cancel_background_process
  - search_message_chains
---

# Goal
Turn today's conversation into future action: reminders, recurring checks, scheduled reviews, or delegated background work.

## Use when
- The user wants to be reminded later.
- A task should be revisited on a future date or cadence.
- A substantial follow-up can be delegated to a subagent now or in the background.
- The user wants to inspect, update, or cancel an existing follow-up plan.

## Workflow
1. Inspect existing jobs with `get_all_scheduled_jobs` before editing or deleting anything.
2. Create a scheduled job when the user wants a one-time or recurring future action.
3. Use `search_message_chains` when the follow-up prompt must refer back to earlier discussion.
4. Use `delegate_subagent_task` when a focused follow-up should happen as a separate delegated task.
5. For background runs, monitor with `inspect_background_process` and `get_subagent_execution_trace_by_initiating_message`.
6. Update or remove scheduled work when priorities change.

## Scheduling Rules
- Do not schedule anything unless the user explicitly wants future execution.
- Keep the scheduled prompt self-contained and specific.
- Prefer one clean job over several overlapping reminders for the same outcome.

## Delegation Rules
- Use a subagent when the follow-up is substantial and bounded.
- Keep the subagent prompt narrow enough that the result can be verified later.
- Cancel background work only when the user wants it stopped or it is clearly obsolete.

## Output Rules
- Always state what will happen, when it will happen, and how it can be changed later.
- If you create delegated background work, tell the user how you will verify completion.
