---
name: build-source-knowledge-graph
description: Turn documents or external sources into a semantically coherent temporal long-term knowledge graph by deriving fact types first and ingesting focused graph episodes iteratively.
tags: [wissensgraph, ontologie, datenerfassung, dokumente, quellendaten, gedächtnis, temporal]
tools:
  - fs_read_command
  - exec_terminal_command
  - search_longterm_memory
  - create_longterm_memory
  - replace_longterm_memory_episode
---

# Goal
Build a semantically useful temporal long-term knowledge graph from a source that the user brought into context. Do not jump straight into memory writes. First search for existing ontology and graph structure in long-term memory, then extend or refine it and ingest the source iteratively in focused graph episodes until the important semantic structure is covered.

## Use when
- The user wants a document, corpus, workspace file, indexed document, or external source transformed into durable graph knowledge.
- You need more than a summary: the source should become queryable as entities, relations, time-bound facts, and reusable context.
- The source is broad enough that one-pass extraction would miss structure, create duplicates, flatten meaning, or lose temporal validity.
- The user asks to create or update long-term memory from source material.

## Source Modes

### Deterministic Markdown Window Mode
Use this when the pipeline provides a concrete Markdown window or excerpt directly.

Rules:
- Treat the provided Markdown window as the current work scope.
- Extract only facts supported by the chunk or by directly referenced local/export context that is necessary to interpret the chunk.
- Do not run a full-document ingestion for every window.
- If the chunk is purely structural, navigational, repetitive, or too ambiguous, create no episode and say why.
- Prefer dense fact-cluster episodes over sentence-level episodes.
- Set `episode_name`, `source_description`, and temporal fields whenever the chunk gives enough information.

### Agentic Full-Coverage Mode
Use this when the assistant has access to a file, indexed document, exported Markdown, workspace path, or external source and must process the complete source itself.

Rules:
- Build a simple coverage plan before ingestion.
- Use exported Markdown as the preferred linear coverage path when available.
- Use search and browse for orientation, clarification, and targeted inspection, not as the only coverage mechanism.
- Track processed Markdown line ranges, relevant elements, and image files internally.
- Inspect referenced export images when they may contain substantive facts.
- Continue until the source has been processed from start to end or until the unavailable part is explicitly identified.

## Source Discovery
1. Identify the source type: exported Markdown, workspace files, local files, or external source references.
2. If the runtime context provides `Document Export Markdown` or `Document Export Directory`, use those absolute paths directly.
3. Orient with `rg '^#{1,6} '` and locate focused terms with `rg -n -i`.
4. Read bounded source regions with `sed -n`; use the Markdown path as the linear coverage path for full-source work.
5. Inspect referenced assets whenever visual details may contribute facts.

## Ontology-First Workflow
1. Before creating new graph structure, use `search_longterm_memory` to look for existing ontology episodes, existing fact types, and already used relation patterns for the same domain or source family.
2. Read enough of the source to identify recurring entity types, relation types, time dimensions, statuses, versions, validity ranges, and source-specific terminology.
3. Reuse existing ontology patterns when they already fit the source well enough.
4. Create or refine ontology only where the current graph does not already provide a good reusable pattern.
5. Define fact types as relation patterns that are likely to recur across the source, for example `organization owns product`, `policy applies to region`, `person reports_to manager`, `contract starts on date`, or `system uses component`.
6. Keep relation labels stable, concrete, and reusable. Avoid inventing a new predicate for every sentence.
7. When you store ontology explicitly, write it as one or more focused ontology episodes with names and descriptions that clearly mark them as ontology or fact-type definitions for this domain.
8. Validate the ontology against multiple source sections before treating it as stable enough for broader ingestion.

## Episode Structure Rules
`create_longterm_memory` expects a structured episode. Follow these rules for every durable write.

### Episode Fields
- Always set `episode_name` when creating an episode. Keep it short, descriptive, and at most 30 characters.
- Avoid generic episode names such as `Facts`, `Document`, `Content`, or `Episode`.
- Use names that describe the fact cluster, for example `Access roles`, `Contract term`, `System ownership`, `Project scope`, `Policy validity`, or `Meeting decision`.
- Set `dataspace_name` when the target dataspace is clear and allowed.
- Always set `source_description`. Keep it at most 120 characters and include the best available provenance: document name, file path, heading, chunk, line range, element offset, or image filename.
- Set `reference_time_iso` when the source provides a reliable temporal anchor such as document date, status date, meeting date, publication date, effective date, contract date, or event time.
- Use ISO8601 UTC timestamps, for example `2026-03-07T21:44:00Z`.
- If only a date is available, use `YYYY-MM-DDT00:00:00Z`.
- Do not invent timestamps. Omit temporal fields when the source gives no reliable anchor.
- Use `context` only for short background, source caveats, scope, interpretation notes, or temporal qualifications that do not fit cleanly into individual facts.

### Fact Fields
- Every fact must include `subject_name`, `predicate`, `object_name`, and `statement`.
- `subject_name` and `object_name` must be explicit, resolved, and different from each other.
- The `statement` must be one complete, pronoun-resolved declarative sentence.
- Include important dates, times, validity periods, versions, statuses, and scope in the `statement` when they affect meaning.
- Use `valid_at_iso` when a fact begins to apply at a known time.
- Use `invalid_at_iso` when a fact stops applying at a known time.
- If both `valid_at_iso` and `invalid_at_iso` are set, the invalid time must be later than the valid time.
- Do not store facts that only have one concrete entity.
- Do not store speculative facts from weak, implied, or contradictory wording.

## Ingestion Workflow
1. Split the source into semantic units such as sections, topics, entities, event clusters, requirements, table groups, image groups, policy units, or relation bundles.
2. Before writing, use `search_longterm_memory` to check whether that unit is already represented, partially covered, or already linked through an existing ontology pattern.
3. Before writing episodes for a source slice, check whether the slice is semantically complete.
4. If the slice is only a fragment of a paragraph, table, list, image group, or section, read the neighboring continuation first when tools allow it.
5. Decide whether the slice contributes new facts, repeated facts, or no durable graph knowledge.
6. Convert one semantic unit at a time into a focused structured episode.
7. Use `create_longterm_memory` for new graph knowledge.
8. Use `replace_longterm_memory_episode` when a prior graph episode is too weak, uses the wrong fact pattern, has missing temporal anchors, or should be merged into a denser canonical representation. The old graph episode is removed and the replacement is queued for background processing with a new episode UUID.
9. After each batch, search again when useful to verify the graph state and detect duplicates, ontology drift, reused relation patterns, or uncovered areas.
10. Continue iteratively until new source passes stop revealing important uncovered fact types, missing entities, missing temporal anchors, or missing reusable ontology structure.

## Export Markdown and Image Rules
- If export Markdown exists and full coverage is required, use it as the linear coverage control path.
- Use `fs_read_command` with `stat`, `wc -l`, `head`, and `sed -n` to traverse text or Markdown files.
- If Markdown contains image references such as `![Image](001_image.png)`, resolve them relative to `Document Export Directory`.
- If indexed content mentions `Document export file: 001_image.png`, look for that file inside the export directory.
- Inspect referenced images with `fs_read_command cat` when they may contain substantive information.
- For knowledge-graph ingestion, inspect referenced images unless they are clearly decorative, repeated, or irrelevant.
- Do not create facts from image filenames or placeholders alone.
- If image inspection fails, state the limitation in the final status.

## Episode Design Rules
- `create_longterm_memory` and `replace_longterm_memory_episode` work best with small, focused, semantically coherent entries.
- Do not dump a whole source, chapter, or report into one episode.
- Prefer one episode per coherent fact cluster, entity cluster, event, policy unit, table group, image fact group, or relation bundle.
- Keep ontology episodes separate from normal source-fact episodes when that makes the graph easier to reuse.
- Name ontology episodes so they are easy to find again through `search_longterm_memory`, for example by including terms like `ontology`, `fact types`, the domain name, or the source family in `episode_name` or `source_description`.
- Keep `episode.context` short and use it only to preserve interpretation that the structured facts alone would lose.
- Use time fields whenever they materially change the meaning or validity of the facts.

## Graph Compaction Rules
- Search before every durable write when practical.
- Reuse existing ontology and relation patterns before inventing new ones.
- Prefer one canonical episode per resolved knowledge unit.
- Replace outdated, duplicate, temporally weak, or low-quality graph episodes instead of stacking near-duplicates. A replacement is not immediately searchable because it is first queued in the LTM cache.
- Keep multiple episodes only when the source clearly describes distinct time states, competing claims, or separate events that should remain independently queryable.
- If the ontology changes during ingestion, revise only the affected episodes instead of rewriting unrelated graph areas.

## Coverage Check
- After each iteration, ask whether the current ontology explains the next unseen source slice without awkward new predicates.
- Ask whether the current temporal model captures dates, validity ranges, status changes, and version information that matter.
- If not, search again for reusable ontology patterns first, then refine or extend the ontology episodes, then continue ingestion.
- Stop only when the remaining uncovered source content is low-value, repetitive, already represented in the graph, or unavailable for a clearly stated reason.

## Output Rules
- Tell the user which source was mapped, which ontology themes were used, and how far ingestion progressed.
- Say whether the ontology was mostly reused, newly created, or revised during ingestion.
- Say how many episodes were created or replaced.
- Mention whether temporal anchors were found and used.
- Mention whether relevant exported images were inspected.
- Be explicit about whether the result is a first pass, a partial graph, or a semantically dense coverage pass.
- If the source is too ambiguous for durable graph writes, explain that and, if useful, create only focused ontology episodes first.
