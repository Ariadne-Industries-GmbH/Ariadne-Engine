---
name: prepare-briefing
description: Prepare a compact briefing from past chats, memory, indexed knowledge, documents, notes, and optional web research.
tags: [briefing, planning, status, briefing-vorbereitung, zusammenfassung, information]
tools:
  - search_message_chains
  - search_longterm_memory
  - search_in_embedding_collections
  - get_document_content_sample
  - search_for_document_content
  - browse_document_content
  - create_ai_note
  - update_ai_note
  - mcp:ariadne-webresearch-mcp:search
  - mcp:ariadne-webresearch-mcp:scrape
  - mcp:ariadne-webresearch-mcp:get_stored_content
mcps:
  - ariadne-webresearch-mcp
---

# Goal
Produce a useful briefing for a day, meeting, project, person, topic, or decision. This skill is for synthesis, not just raw search.

## Use when
- The user asks for a daily, project, customer, topic, or status briefing.
- You need a compact "what matters" view before a meeting or decision.
- You need to combine internal context with documents and optional current web information.

## Workflow
1. Gather recent and relevant conversational context with `search_message_chains`.
2. Pull durable background from `search_longterm_memory`.
3. Search indexed collections with `search_in_embedding_collections`.
4. If the embedding results point to document `file_key` values, use those as `document_ids` for the document-reader tools.
5. Use `get_document_content_sample` first when document coverage is unclear, then `search_for_document_content` or `browse_document_content` for evidence.
6. Use Ariadne web research only when current external information materially improves the briefing.
7. Synthesize the result into a short briefing with clear sections such as status, open questions, risks, and next actions.
8. If the user wants persistence, store the briefing or its distilled version in AI Notes.

## Evidence Rules
- Favor internal evidence before optional web research.
- Distinguish between durable facts, current status, and external updates.
- If a document, prior chat, and web source disagree, surface the discrepancy explicitly.

## Output Shape
- Keep the briefing compact and decision-oriented.
- Prefer sections like `Current Status`, `Key Facts`, `Open Risks`, and `Suggested Next Steps`.
- Write a reusable note only when the user asks for it or when preserving the briefing is clearly valuable.
