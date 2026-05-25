---
name: delegate-subagent-task
description: Delegate work to an isolated agent task, either inline or as a background run, and verify sequential or parallel execution results.
tags: [subagent, aufgabenteilung, delegation, hintergrund-prozess]
tools: [delegate_subagent_task, get_subagent_execution_trace_by_initiating_message, inspect_background_process, search_background_processes, cancel_background_process]
---

# Goal
Use this skill to delegate a focused, bounded task to `delegate_subagent_task`. The child agent executes the task once in an isolated context, returns its result to the master agent, and terminates. It does NOT create an interactive session with the end user or switch roles in the current chat.

## Use when
- The user asks to delegate, split, investigate, or run an isolated subtask with a separate agent.
- The task benefits from a separate chat/context or a shorter, focused prompt.
- The user explicitly wants background execution or the work is long-running and does not block the next master step.

## Sequential Workflow
1. Call `delegate_subagent_task` with a self-contained `prompt`.
2. Omit `parallel` or set it to `false`.
3. Use the returned tool result as the subagent answer.
4. Continue only after integrating the returned result into the master response or next step.

## Parallel Workflow
1. Call `delegate_subagent_task` with `parallel: true`.
2. Read the immediate tool result and keep both identifiers:
   - `background_process_id`
   - `initiating_message_key`
3. Continue with master work that does not depend on the subagent result.
4. Use `inspect_background_process` with `background_process_id` to check status and stream events.
5. Use `get_subagent_execution_trace_by_initiating_message` with `initiating_message_key` to inspect the subagent trace and final answer.
6. If the result is not ready, inspect again later with the newest `last_event_counter`.
7. Use `cancel_background_process` only when the user wants the background run stopped or when the run is clearly obsolete.

## Verification Rules
- For sequential runs, the tool response is the verification surface.
- For parallel runs, check the background process first. A completed process can still have trace details that should be read through `get_subagent_execution_trace_by_initiating_message`.
- If `get_subagent_execution_trace_by_initiating_message` returns no trace yet, the subagent may still be running or may not have persisted a trace. Use `inspect_background_process` before treating that as failure.
- If the `background_process_id` is lost, use `search_background_processes` with `process_type: "subagent_execution"` and `process_subject: "delegate_subagent_task"`.

## Prompt Rules
- Make the prompt self-contained: goal, constraints, expected output, and what not to change.
- Mention whether the subagent should only inspect, produce a proposal, or implement changes.
- Include relevant keys or names when targeting another chat/context.
- Keep the prompt narrow enough that the master can evaluate the result.

## Targeting
- `chat_key` selects an exact target chat and takes priority over context/name targeting.
- `context_name` selects a target context when no `chat_key` is given.
- `chat_name` selects or creates a chat inside the target context.
- `history_length` controls how much target-chat history the subagent sees. Use `0` for an isolated run.

## Examples
Sequential:

```json
{
  "prompt": "Inspect the current context and summarize the risks in the proposed scheduler change. Return concrete findings only.",
  "history_length": 0,
  "parallel": false
}
```

Parallel:

```json
{
  "prompt": "Investigate the generic-agent-interface tests for brittle assumptions around subagent tool schemas. Return a concise report with file references.",
  "history_length": 0,
  "parallel": true
}
```
