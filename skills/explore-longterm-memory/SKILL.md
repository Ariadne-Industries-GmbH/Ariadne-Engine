---
name: explore-longterm-memory
description: Retrieve and analyze graph-backed long-term memory with batched semantic search, local expansion, explicit structural rankings, and fact evidence.
tags: [ltm, long-term-memory, graph-search, retrieval, memory]
tools:
  - search_longterm_memory
  - expand_longterm_memory_graph
  - search_longterm_memory_structure
  - analyze_longterm_memory_subgraph
  - open_longterm_memory_evidence
  - create_longterm_memory
---

# Goal

Answer questions that depend on remembered history, decisions, states, people,
projects, systems, incidents, or prior observations.

Long-term memory combines semantic retrieval with a graph. Use each tool for its
specific job:

1. `search_longterm_memory` finds relevant entity and fact anchors.
2. `expand_longterm_memory_graph` opens the local relational neighborhood.
3. `search_longterm_memory_structure` ranks entities by an explicit global metric.
4. `analyze_longterm_memory_subgraph` measures a seed-bounded local graph.
5. `open_longterm_memory_evidence` opens the original episodes behind facts.

A plausible hit is evidence to inspect, not automatically the final answer.

# When to Use Memory

Use memory when the request may depend on something previously observed, stored,
decided, discussed, or changed. This includes historical questions, status
reconstruction, summaries, diagnoses, dependencies, prior incidents, and source
verification.

Do not use memory for general knowledge, pure rewriting, calculations whose input
is already present, or tasks fully answerable from the visible chat.

# Tool Selection

## Semantic Search

Use `search_longterm_memory` as the normal entry point. Send all independent
search needs together in `queries`:

```json
{
  "queries": [
    "Software",
    "Module der Software"
  ],
  "limit_per_query": 5
}
```

Each list item is one focused natural-language query. Multi-word names remain one
query. Do not use `AND`, `OR`, long keyword lists, or repeat the same weak query in
separate calls.

The output contains one `results` group per input query in the same order. Each
group repeats the compact query text so its matches remain unambiguous; no query
index is returned.

Matches are either:

- `kind: entity`, with an entity UUID and optional structural profile;
- `kind: fact`, with complete source, relation, fact text, target, times, and
  evidence references.

There is no query target and no semantic score in the output. Decide relevance
from the user question and the returned content. Episodes are not regular search
matches.

`cross_query_entities.query_coverage` only counts in how many result groups an
entity occurred during this single tool call. It is not a relevance score and is
not persisted.

## Local Expansion

Use `expand_longterm_memory_graph` after finding entity UUIDs when surrounding
relations matter. Expansion is undirected for traversal: both endpoints are
followed, while the stored source and target remain unchanged in the output.

Start with `depth: 1`. Use `depth: 2` only when the first neighborhood leaves an
important gap. Depth greater than two is unsupported.

Depth counts complete entity-to-entity relation hops. Graphiti may physically
store a fact as an intermediate node, but that storage node does not consume a
public hop: depth one already returns entity, relation, and the connected entity.

Optional filters are exact:

- `edge_types` matches exact relation names;
- `node_labels` matches available Graphiti entity labels.

There is no direction parameter and no time filter. If `truncated` is true and
the remaining graph matters, repeat the identical request with `next_cursor`.
Never edit or reuse a cursor with changed seeds, scope, depth, or filters.

## Global Structural Search

Use `search_longterm_memory_structure` when the question concerns the graph as a
whole, such as highly connected entities, entities supported by many episodes, or
entities with varied relation types. Ask only for the metric that has a clear
meaning for the question:

```json
{
  "rankings": [
    {
      "metric": "distinct_neighbors",
      "top_k": 10
    },
    {
      "metric": "distinct_episode_count",
      "top_k": 10
    }
  ],
  "filters": {
    "relation_types": [],
    "node_labels": [],
    "minimum_relation_count": 0
  }
}
```

Supported metrics are:

- `relation_count`: adjacent fact edges;
- `distinct_neighbors`: directly connected entities;
- `distinct_episode_count`: distinct supporting episode UUIDs;
- `relation_type_count`: distinct exact relation names.

Rankings are separate. There is no balanced ranking or hidden combined score.
`ranking_coverage` only says that an entity appeared in several explicitly
requested top-k lists in this call.

## Local Structural Analysis

Use `analyze_longterm_memory_subgraph` to measure the graph bounded by seeds and
depth. It separates:

- `local_metrics`, calculated only inside the expanded subgraph;
- `global_profile`, calculated across the selected dataspace scope.

Local values include relation and neighbor counts, seed coverage, repeated
relation types, and same-source or same-target patterns. They are measurements,
not a declaration of importance.

## Evidence

Use `open_longterm_memory_evidence` when the user asks for sources or original
wording, when facts conflict, when chronology matters, or when an important claim
needs verification.

Pass fact `edge_uuids` directly. The tool resolves and groups the supporting
episodes per edge. Limit episode count and excerpt size to what the answer needs.
An episode count is not a count of independent documents or sources.

## Memory Creation

Use `create_longterm_memory` only for explicit durable-memory intent or a workflow
that clearly requires storage. Do not switch from retrieval into writing without
that intent.

# Structural Profiles

An entity profile describes graph structure:

- `relation_count`
- `distinct_neighbors`
- `distinct_episode_count`
- `relation_type_count`
- `relation_histogram`
- `dataspace_percentiles`

A percentile of `0.94` means the raw value is greater than that of about 94% of
entities in `percentile_scope`. It is not probability, relevance, truth, quality,
or confidence. Always interpret percentiles together with their raw metric and
scope.

# Time Fields

Tools may return `created_at`, `reference_time`, `valid_at`, `invalid_at`, and
`expired_at`. They have different meanings. Do not infer automatically that a
fact is current, invalid, superseded, or causally relevant. A missing field means
unknown or null.

There are no structured time filters. For relative-time questions, resolve the
calendar wording when possible, include a concise date or event phrase in a
semantic query, then inspect returned fact and evidence times. If the time cannot
be resolved, state the uncertainty rather than inventing a date.

# Retrieval Procedure

1. Extract concrete anchors and independent information needs from the request.
2. Batch the focused needs into one semantic search call.
3. Read each query-labeled result group and select relevant entity or edge UUIDs.
4. For a narrow, exact, unambiguous lookup, answer if the evidence is sufficient.
5. For summaries, history, causes, dependencies, diagnoses, or ambiguity, take at
   least one focused follow-up: local expansion, another genuinely different
   semantic query batch, local analysis, global ranking, or evidence.
6. Open evidence selectively for claims whose source, wording, chronology, or
   conflict matters.
7. Stop when the answer is supported and further graph work would be off-topic.

# Sparse Results and Conflicts

If search is weak, try a genuinely simpler query such as the main entity name or
the entity plus one event term. Do not issue cosmetic variations indefinitely.

If facts conflict:

1. keep both visible;
2. compare their raw time fields without assuming a lifecycle interpretation;
3. open evidence for the relevant edges;
4. explain the conflict and remaining uncertainty.

Never invent missing memory.

# Answer Construction

Answer primarily from entities and facts. Use episode excerpts as supporting
evidence. Separate directly stored facts from your inference. For summaries,
group information into a few useful themes instead of reproducing raw YAML. For
historical answers, mention known dates and clearly label unresolved chronology.

# Anti-Patterns

Do not:

- treat graph metrics or percentiles as semantic relevance;
- request a balanced score;
- assume edge direction has domain meaning;
- invent time filters or interpret missing times as a status;
- use episodes as the default search surface;
- pass episode UUIDs to the evidence tool instead of edge UUIDs;
- assume query history or hit counters exist across calls;
- expand blindly when one focused search already answers a narrow lookup;
- stop at one plausible hit for a non-trivial historical or relational question.
