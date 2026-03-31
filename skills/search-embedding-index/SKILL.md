---
name: search-embedding-index
description: Search embedding collections semantically with clear queries and iterative refinement.
tools: [search_in_embedding_collections]
tags: [search, embeddings, knowledge, documents, vector]
---

# Goal
Use semantic search over embedding collections to find relevant content when exact text matching is not enough.

## Use when
- The user asks about information that may exist in indexed collections.
- You need conceptually related matches, not just literal string matches.
- The request may span one or more knowledge collections.

## Workflow
1. Identify the information need and turn it into a specific natural-language query.
2. Choose collections if the relevant scope is known. Otherwise search broadly first.
3. Call `search_in_embedding_collections` with `action="search"`.
4. Review the results and refine the query if the first search is too broad or too weak.

## Query Rules
- Use a non-empty, specific query.
- Use semantic natural-language queries, not structured boolean query syntax such as `AND` or `OR`.
- The model searches by similarity between terms, concepts, and search phrases.
- Prefer focused concepts over vague requests.
- Add collection filters when you know where the information should live.
- Refine iteratively instead of asking one overly broad question.

## Limits
- This is semantic search, not long-term memory lookup.
- Results are ranked by relevance, not by recency.
- Not all information is guaranteed to be indexed.

## Examples
```json
{
  "query": "API authentication methods and requirements",
  "collections": ["api-docs"],
  "action": "search"
}
```

```json
{
  "query": "company Q4 2024 financial results and earnings",
  "collections": ["financial-reports"],
  "action": "search"
}
```
