---
name: document-reader
description: Retrieve answers from indexed documents with sample, search, and browse steps.
tags: [dokumente, lesen, analyse, text-extraktion, dateien]
tools: [get_document_content_sample, search_for_document_content, browse_document_content]
---

# Goal
Answer document questions by retrieving evidence from indexed content. If evidence is weak, retrieve more instead of guessing.

## Use when
- The user asks about uploaded or indexed documents.
- The user refers to specific files, sections, or details inside documents.
- You need to summarize or inspect document content.

## Workflow
1. If the question is broad or coverage is unknown, start with `get_document_content_sample`.
2. Extract keywords, section labels, or document structure hints from the sample.
3. Use `search_for_document_content` to find the best starting position for reading.
4. After every useful search hit, continue with `browse_document_content` at the returned `chunk_index` and `element_offset`.
5. If the hit is too early in the document or mostly structural content, call `search_for_document_content` again with `from_chunk_index` or with `from_chunk_index` and `from_element_offset`.
6. For large documents, resample from another area before giving up.

## Search Tool
`search_for_document_content` is for locating the best place to start reading, not for fully reading the answer.

Parameters:
- `file_content_query`: focused search request for the document content.
- `document_ids`: document keys to search in.
- `from_chunk_index`: optional lower bound to ignore earlier chunks.
- `from_element_offset`: optional lower bound inside `from_chunk_index`; only use it together with `from_chunk_index`.

Search procedure:
- Start without filters unless you already know the relevant part is later in the document.
- Treat the search result as a starting point, not as full evidence.
- If the search result looks relevant, immediately continue with `browse_document_content`.
- If the search result is too early, repeat the same search with `from_chunk_index` or `from_chunk_index` plus `from_element_offset`.

## Browse Tool
`browse_document_content` reads one chunk and a bounded window of elements inside that chunk.
This matters because some non-PDF documents can have few chunks, or even one chunk, but many elements.

Parameters:
- `document_id`: document key to inspect.
- `chunk_index`: zero-based chunk index.
- `element_offset`: zero-based element offset inside the selected chunk.
- `element_limit`: number of elements to render from that chunk. Keep this small; default is one element.
- `element_content_offset`: character offset inside each rendered element's content.
- `element_content_char_limit`: maximum characters to render per element. The tool may cap this to keep the response bounded.

Sequential browse procedure:
- If search returned a specific location, start browsing from that `chunk_index` and `element_offset`.
- Otherwise start with `chunk_index=0`, `element_offset=0`, and `element_limit=1`.
- Read the tool metadata: `Total Elements In Chunk`, `Rendered Elements`, and `Next Element Offset`.
- If `Next Element Offset` is not `N/A`, continue the same chunk with that `element_offset`.
- If an element is truncated, continue that same element using the tool's explicit continuation instruction with the same `document_id`, same `chunk_index`, same `element_offset`, `element_limit=1`, and the returned `element_content_offset`.
- Only advance to the next `chunk_index` after the current chunk's elements are exhausted or the answer has enough evidence.
- Do not assume a whole document was read just because a chunk was read. For one-chunk documents, iterate through the elements.

## Rules
- Treat unknown coverage as `0`.
- If coverage is `0` or the task is broad, sample first.
- After two failed searches in a row, reset orientation with another sample or browse sequentially.
- Do not stop at a search hit if you still need source content. Use browse for the actual reading.
- Only say you could not find the answer after at least one sample call, unless coverage is already high, and after the full search sequence has failed.
- When citing evidence, include chunk indices and element offsets/indices when useful for traceability.

## Search Tips
- Search with 2-6 focused keywords.
- Consider English and German structure terms.
- Use `from_chunk_index` or `from_element_offset` to skip earlier sections when searches keep returning content from the start of the document.
- For multi-document tasks, retrieve per document first and synthesize afterward.

## Examples
- Broad question: sample first, then summarize the dominant topics.
- Exact section question: search with literal section terms, then synonyms, then browse if needed.
- Full summary request: browse sequentially across chunks and element windows; synthesize only after enough elements have been inspected.
