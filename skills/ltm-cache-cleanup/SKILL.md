---
name: ltm-cache-cleanup
description: Review, fix, requeue, or delete problematic long-term memory cache entries.
tags: [gedächtnis-pflege, cache-bereinigung, ltm-verwaltung]
tools: [review_ltm_cache_entries, requeue_ltm_cache_entry, delete_ltm_cache_entry, create_longterm_memory, search_longterm_memory]
---

# LTM Cache Cleanup Skill

## When to Use

Use this skill when:
- The user asks why a memory was not stored correctly
- The user wants to inspect, clean up, or repair the LTM cache
- You need to review non-retryable or failed long-term memory cache entries
- Search results or warnings indicate that cached LTM entries need attention

## Cleanup Strategy

1. Start with `review_ltm_cache_entries`.
2. Focus on `status="failed"` and `retryable=false` first.
3. Inspect `failure_class`, `last_error`, `content`, and extraction counts.
4. Decide one of these actions:
   - `requeue_ltm_cache_entry` if the entry should be retried
   - `requeue_ltm_cache_entry` with `replacement_episode` if the text is weak or malformed and should be rebuilt as a structured episode
   - `delete_ltm_cache_entry` if the entry is obsolete, wrong, or should never be ingested
5. If a broken cache entry should be replaced from scratch instead of retried, delete it and then use `create_longterm_memory` with a corrected structured episode.

## Decision Rules

- `low_signal_extraction`:
  - Usually rewrite the text into clearer factual statements before requeueing
  - Delete instead if the memory is not worth storing
- `unknown_error`:
  - Review carefully before requeueing
  - Prefer requeueing only if the text itself still looks valid
- Non-retryable validation failures:
  - Fix the underlying dataspace or payload problem before requeueing
  - Delete the entry if it is no longer useful

## Episode Repair Guidance

When rebuilding a replacement episode:
- Put durable declarative facts into `replacement_episode.facts`
- Use explicit names instead of unresolved pronouns
- Include time only when it materially affects validity or retrieval
- Use `replacement_episode.context` only for short background
- Prefer one coherent episode over many tiny repairs
- `create_longterm_memory` works best with small, focused, semantically coherent entries
- Rebuild iteratively when needed instead of packing multiple unrelated repairs into one episode

## Long-Term Memory Write Rule

- When you need a fresh durable write, prefer a series of focused corrected episodes over one oversized catch-all repair payload.

## Tools

### review_ltm_cache_entries

Use this to inspect parked or failed cache entries before taking action.

### requeue_ltm_cache_entry

Use this to retry a cache entry. Prefer passing a corrected `replacement_episode` when the original text is weak or malformed.

### delete_ltm_cache_entry

Use this when the cache entry should be removed instead of retried.

### create_longterm_memory

Use this after deleting a broken cache entry if it is cleaner to create a fresh corrected structured episode instead of requeueing the old one.

### search_longterm_memory

Use this to verify whether the repaired memory later became searchable or to inspect surrounding long-term memory context before cleanup.
