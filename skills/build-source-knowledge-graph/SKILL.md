---
name: build-source-knowledge-graph
description: Turn documents or external sources into a semantically coherent long-term knowledge graph by deriving fact types first and then ingesting focused graph episodes iteratively.
tags: [wissensgraph, ontologie, datenerfassung, dokumente, quellendaten, gedächtnis]
tools:
  - search_in_embedding_collections
  - get_document_content_sample
  - search_for_document_content
  - browse_document_content
  - search_longterm_memory
  - create_longterm_memory
  - replace_longterm_memory_episode
---

# Goal
Build a semantically useful long-term knowledge graph from a source that the user brought into context. Do not jump straight into memory writes. First search for existing ontology and graph structure in long-term memory, then extend or refine it and ingest the source iteratively in focused graph episodes until the important semantic structure is covered.

## Use when
- The user wants a document, corpus, or external source transformed into durable graph knowledge.
- You need more than a summary: the source should become queryable as entities, relations, and time-bound facts.
- The source is broad enough that one-pass extraction would miss structure, create duplicates, or flatten meaning.

## Source Discovery
1. Identify the source type: indexed documents or a linked embedding collection.
2. For indexed material, start with `search_in_embedding_collections` and gather candidate `file_key` values from the returned metadatas.
3. Use those `file_key` values as `document_ids` for the document-reader tools.
4. Use `get_document_content_sample` first when the source coverage is unclear.
5. Use `search_for_document_content` and `browse_document_content` to inspect sections, entities, repeated patterns, and terminology.

## Ontology-First Workflow
1. Before creating new graph structure, use `search_longterm_memory` to look for existing ontology episodes, existing fact types, and already used relation patterns for the same domain or source family.
2. Read enough of the source to identify the recurring entity types, relation types, and time dimensions.
3. Reuse existing ontology patterns when they already fit the source well enough.
4. Create or refine ontology only where the current graph does not already provide a good reusable pattern.
5. Define fact types as relation patterns that are likely to recur across the source, for example `organization owns product`, `policy applies to region`, or `person reports_to manager`.
6. Keep relation labels stable, concrete, and reusable. Avoid inventing a new predicate for every sentence.
7. When you store ontology explicitly, write it as one or more focused ontology episodes with names and descriptions that clearly mark them as ontology or fact-type definitions for this domain.
8. Validate the ontology against multiple source sections before treating it as stable enough for broader ingestion.

## Ingestion Workflow
1. Split the source into semantic units such as sections, topics, entities, or event clusters.
2. Before writing, use `search_longterm_memory` to check whether that unit is already represented, partially covered, or already linked through an existing ontology pattern.
3. Convert one semantic unit at a time into a focused structured episode.
4. Use `create_longterm_memory` for new graph knowledge.
5. Use `replace_longterm_memory_episode` when a prior episode is too weak, uses the wrong fact pattern, or should be merged into a denser canonical representation.
6. After each batch, search again to verify the graph state and detect duplicates, ontology drift, reused relation patterns, or uncovered areas.
7. Continue iteratively until new source passes stop revealing important uncovered fact types, missing entities, or missing reusable ontology structure.

## Episode Design Rules
- `create_longterm_memory` and `replace_longterm_memory_episode` work best with small, focused, semantically coherent entries.
- Do not dump a whole source, chapter, or report into one episode.
- Prefer one episode per coherent fact cluster, entity cluster, event, policy unit, or relation bundle.
- Keep ontology episodes separate from normal source-fact episodes when that makes the graph easier to reuse.
- Name ontology episodes so they are easy to find again through `search_longterm_memory`, for example by including terms like `ontology`, `fact types`, the domain name, or the source family in `episode_name` or `source_description`.
- Keep `episode.context` short and use it only to preserve interpretation that the structured facts alone would lose.
- Use time fields only when they materially change the meaning or validity of the facts.

## Graph Compaction Rules
- Search before every durable write.
- Reuse existing ontology and relation patterns before inventing new ones.
- Prefer one canonical episode per resolved knowledge unit.
- Replace outdated or low-quality episodes instead of stacking near-duplicates.
- Keep multiple episodes only when the source clearly describes distinct time states, competing claims, or separate events that should remain independently queryable.
- If the ontology changes during ingestion, revise only the affected episodes instead of rewriting unrelated graph areas.

## Coverage Check
- After each iteration, ask whether the current ontology explains the next unseen source slice without awkward new predicates.
- If not, search again for reusable ontology patterns first, then refine or extend the ontology episodes, then continue ingestion.
- Stop only when the remaining uncovered source content is low-value, repetitive, or already represented in the graph.

## Output Rules
- Tell the user which source was mapped, which ontology themes were used, and how far ingestion progressed.
- Say whether the ontology was mostly reused, newly created, or revised during ingestion.
- Be explicit about whether the result is a first pass, a partial graph, or a semantically dense coverage pass.
- If the source is too ambiguous for durable graph writes, explain that and, if useful, create only focused ontology episodes first.
