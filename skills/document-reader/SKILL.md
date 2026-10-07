---
name: document-reader
description: Research exported documents directly through their Markdown, assets, and structured provenance.
tags: [dokumente, lesen, analyse, text-extraktion, dateien, bilder]
tools: [fs_read_command, exec_terminal_command]
---

# Goal

Answer document questions from the exported source artefact, not from a hidden
retrieval index. The runtime context supplies the absolute `Dokument-Markdown`
path and, where available, `Asset-Verzeichnis` path for each attached document.

## Workflow

1. Start with the supplied absolute Markdown path; never derive a path from a
   file ID or filename.
2. Read the heading structure with `rg '^#{1,6} ' <document.md>`.
3. Search literal terms with `rg -n -i '<terms>' <document.md>`.
4. Read the surrounding range with `sed -n '<start>,<end>p' <document.md>`.
5. For broad or full-coverage work, progress through `document.md` in bounded
   line windows and keep the covered ranges in your reasoning.
6. Follow `assets/...` references for tables or pictures whose visual content
   can change the answer. Resolve them relative to the export directory.

## Artefact layout

Refined Docling artefacts are structured as:

```text
<source-file-stem>__<file-key>/docling-<source-hash>/
├── document.md
└── assets/
```

An AnyDoc fast artefact is instead typically
`.../fast-<source-hash>/document.md`. It can be read immediately but may have
no assets and no page/BBox provenance. A refined artefact preserves Docling's
reading order and can include visual assets. Never infer page or bounding-box
provenance from Markdown markup. UI renderers obtain that information from the
document-element graph data.

## Rules

- Do not look for former document retrieval tools, Chroma collections,
  embedding IDs, or `FileChunk` nodes. They no longer exist; the exported
  Markdown file is the document index.
- Prefer the supplied absolute paths; do not guess storage paths or use file
  IDs as retrieval keys.
- Quote or cite Markdown line ranges and asset names when this helps a user
  verify the source.
- Do not claim visual facts from an asset filename or a placeholder alone.
- If a document artefact is missing, say so instead of inventing content from
  metadata.
