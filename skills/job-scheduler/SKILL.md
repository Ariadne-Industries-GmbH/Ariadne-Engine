---
name: job-scheduler
description: Create, inspect, update, and remove scheduled jobs with date, interval, or cron triggers.
tools: [create_scheduled_job, get_all_scheduled_jobs, update_scheduled_job, remove_job]
tags: [scheduling, automation, jobs]
---

# Goal
Use this skill to create, inspect, update, and remove scheduled jobs. Only schedule work when the user explicitly wants future or recurring execution.

## Use when
- The user wants a task to run once in the future.
- The user wants a recurring job.
- The user wants to inspect, reschedule, edit, or delete an existing job.

## Workflow
1. Use `get_all_scheduled_jobs` when the user asks what already exists or before editing/removing a job.
2. For a new job, choose one trigger kind:
   - `date_trigger` for one-time execution
   - `interval_trigger` for every N minutes, hours, or days
   - `cron_trigger` for calendar schedules such as weekdays or fixed times
3. Build a clear `job_payload` with `job_name`, `job_description`, and a self-contained `job_prompt`.
4. Use `create_scheduled_job` to create jobs.
5. Use `update_scheduled_job` to change the trigger, payload, or both.
6. Use `remove_job` only when the user explicitly wants cancellation or deletion.

## Job Prompt Rules
- Write only the actual task prompt. Do not add scheduler wrapper markup yourself.
- The scheduler tools inject and normalize runtime context around `job_prompt` automatically.
- Make the prompt self-contained: goal, required checks, expected output, and any ordering constraints.
- Prefer numbered steps when the task has multiple phases.
- Put only the needed runtime tools into `llm_tools`.
- Keep `subagents` empty unless runtime support is explicitly available.

## Trigger Rules
- Exactly one trigger kind is allowed per create or update call.
- Use RFC3339 datetimes for `run_at`, `start_date`, and `end_date`.
- Prefer explicit `timezone` values to avoid DST surprises.
- `calendar_interval_trigger` is not supported.

## Chat Targeting Rules
- If `target_chat_key` is omitted, the job runs in the current chat.
- Tell the user when the current chat will be used implicitly.
- Use `target_chat_key` only when the user wants the job to run in a different chat context.

## Examples
```json
{
  "job_payload": {
    "job_name": "weekday-weather",
    "job_description": "Check the weather every weekday morning",
    "job_prompt": "Create a concise weather report for Berlin. Include temperature, conditions, and today's forecast.",
    "llm_tools": ["web_search"],
    "subagents": []
  },
  "trigger": {
    "kind": "cron_trigger",
    "params": {
      "day_of_week": "mon,tue,wed,thu,fri",
      "hour": "7",
      "minute": "0",
      "timezone": "Europe/Berlin"
    }
  }
}
```

```json
{
  "job_id": "existing-job-id",
  "input": {
    "job_payload_updates": {
      "job_prompt": "Create the same report, but include wind speed as well."
    }
  }
}
```
