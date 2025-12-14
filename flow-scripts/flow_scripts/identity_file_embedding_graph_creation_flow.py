# LICENSED under custome Ariadne Industries GmbH License Terms
# see https://www.ariadneanyverse.de/Annex_On-premise_License_Terms.pdf

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from aaa_types.dtos import (AAAChatCompletionChunk,
                            GeneralConversationChatRequest, MessageDTO,
                            ProcessedDocumentChunk)
from flow_kit.engine_utils import EngineUtils, identity_key_var
from flow_kit.registry import flow

logger = logging.getLogger("flows.identity_file_embedding_graph_creation_flow")

"""
Identity-scoped embedding and LTM graph creation flow.

- Derives the files directory from the caller's identity key and the
  `AAA_STORAGE_BASE_DIR` environment variable. Files live under:
    <AAA_STORAGE_BASE_DIR>/<sanitized-identity>/files

- Scans that directory for PDFs, performs extract → chunk → embed, and
  ingests reconstructed episodic markdown into the LTM graph.

- Stores all flow-related controller and index JSON files inside the same
  per-identity files directory (e.g., cancel flag and embedding index).

Function signature

- dto: GeneralConversationChatRequest
- collection_name: str
- dataspace_id: str | None = None
- use_cpu: bool = True
- max_parallel: int = 1
- ingest_to_graph: bool = True
"""


INTRO_PROMPT = (
    "You are performing an embedded document processing run (CPU extraction).\n"
    "For each PDF: extract → chunk → create embeddings → register LTM/graph entries.\n"
    "Answer concisely, keep a clear structure, and emit per‑file status updates."
)


SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9._-]")


def user_dir(base: str, identity: str) -> str:
    identity = SAFE_NAME_RE.sub("", identity)
    return os.path.join(base, identity, "files")


def get_storage_base_dir() -> str:
    base = os.environ.get("AAA_STORAGE_BASE_DIR", None)
    if base is None:
        raise ValueError("No AAA_STORAGE_BASE_DIR given by environment variable")
    os.makedirs(base, exist_ok=True)
    return base


def safe_json_path(raw: str) -> Path:
    """
    Convert a potentially JSON‑escaped path string to a pathlib.Path.
    If not JSON, use the raw string.
    """
    try:
        cleaned = json.loads(raw)
        if not isinstance(cleaned, str):
            cleaned = str(cleaned).strip()
    except json.JSONDecodeError:
        cleaned = str(raw).strip()
    return Path(cleaned)


def _basename(value: str | os.PathLike[str] | None) -> str:
    if value is None:
        return ""
    try:
        return os.path.basename(str(value))
    except Exception:
        return str(value)


def _sanitize_error_message(msg: str, *possible_paths: str) -> str:
    """
    Remove directory information from any path-like substrings in error messages.
    Replaces detected paths with their basenames.
    """
    if not isinstance(msg, str):
        return str(msg)
    # Replace any explicitly provided paths first
    for p in possible_paths:
        if not p:
            continue
        try:
            msg = msg.replace(p, _basename(p))
        except Exception:
            pass
    # Generic path pattern (both POSIX and Windows)
    path_like = re.compile(r"((?:[A-Za-z]:)?[\\/][^\s'\"]+(?:[\\/][^\s'\"]+)*)")
    def _repl(m: re.Match[str]) -> str:
        return _basename(m.group(1))
    return path_like.sub(_repl, msg)


# ---- DTOs: Embedding Index ----

IndexStatus = Literal["unprocessed", "embedded", "embedded_without_ltm", "failed"]


# ---- Local extraction helpers (EngineUtils-backed) ----


async def iter_sse_chunks(
    engine_utils: EngineUtils,
    file_path: str,
    mime_type: str,
    dto: GeneralConversationChatRequest,
    use_cpu: bool = True,
):

    privacy_level = dto.completion_request.aaa_body.privacy_level
    model_name = dto.completion_request.aaa_body.model_name

    stream = (
        engine_utils.process_file_cpu(
            file_path, mime_type, privacy_level=privacy_level, model_name=model_name
        )
        if use_cpu
        else engine_utils.process_file(
            file_path, mime_type, privacy_level=privacy_level, model_name=model_name
        )
    )

    try:
        async for item in stream:
            if isinstance(item, (bytes, str)):
                line = item.decode() if isinstance(item, bytes) else item
                if not line.startswith("data:"):
                    continue
                data = line[6:]
                yield ProcessedDocumentChunk.model_validate_json(data)
            elif isinstance(item, dict):
                yield ProcessedDocumentChunk.model_validate(item)
            else:
                try:
                    yield ProcessedDocumentChunk.model_validate(item)
                except Exception:
                    # Ignore malformed items but continue streaming
                    continue
    except Exception as e:
        # Propagate extraction/processing errors so caller can mark file as failed
        # Do not include file paths in the error message.
        raise RuntimeError(f"Extraction failed: {e}")


async def _embedding_exists(
    engine_utils: EngineUtils, collection_name: str, embedding_id: str
) -> bool:
    try:
        resp = await engine_utils.get_embedding_by_id(
            collection_name=collection_name, embedding_id=embedding_id
        )
    except Exception:
        return False
    try:
        # Contract: nothing found -> empty lists
        if not resp or not resp.embedding_ids:
            return False
        first = resp.embedding_ids[0] if len(resp.embedding_ids) > 0 else []
        return bool(first and (embedding_id in first))
    except Exception:
        return False


async def _episode_exists(engine_utils: EngineUtils, episode_uuid: str) -> bool:
    try:
        res = await engine_utils.get_episode_with_uuid(episode_uuid)
    except Exception:
        return True
    try:
        # Contract: nothing found -> empty lists
        if res is None:
            return False
        else:
            if res.uuid == episode_uuid:
                return True
            else:
                return False
    except Exception:
        return True


async def _sync_index_ids_with_backend(
    engine_utils: EngineUtils,
    collection_name: str,
    index_path: Path,
    index: dict,
    index_lock: asyncio.Lock,
    pdf_files: list[Path],
) -> None:
    """
    For each present file's index entry, verify that stored embedding_ids and
    episodic_ids still exist in the backend. Remove stale IDs from the index.
    """
    for p in pdf_files:
        key = str(p)
        entry = index.get(key)
        if not entry:
            continue
        # For unprocessed/failed: attempt deletion of episodic IDs, then verify; only remove those confirmed gone
        try:
            status = entry.get("status")
            epi_ids_for_reset = list(entry.get("episodic_ids") or [])
            if epi_ids_for_reset and status in ("unprocessed", "failed"):
                for episode_uuid in epi_ids_for_reset:
                    try:
                        await engine_utils.delete_episode_from_ltm_graph(episode_uuid)
                    except Exception:
                        pass
                remaining_epi: list[str] = []
                for episode_uuid in epi_ids_for_reset:
                    try:
                        if await _episode_exists(engine_utils, episode_uuid):
                            remaining_epi.append(episode_uuid)
                    except Exception:
                        # Conservative: keep ID on verification error
                        remaining_epi.append(episode_uuid)
                if remaining_epi != epi_ids_for_reset:
                    async with index_lock:
                        cur = (index.get(key) or {}).copy()
                        cur["episodic_ids"] = remaining_epi
                        cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                        index[key] = cur
                        _save_index(index_path, index)
        except Exception:
            # Non-fatal; continue with remaining integrity checks
            pass
        changed = False
        # Validate embeddings
        kept_emb_ids: list[str] = []
        for emb_id in list(entry.get("embedding_ids") or []):
            if await _embedding_exists(engine_utils, collection_name, emb_id):
                kept_emb_ids.append(emb_id)
            else:
                changed = True
        # Validate episodic ids
        kept_epi_ids: list[str] = []
        for epi_id in list(entry.get("episodic_ids") or []):
            if await _episode_exists(engine_utils, epi_id):
                kept_epi_ids.append(epi_id)
            else:
                changed = True
        if changed:
            try:
                async with index_lock:
                    cur = entry.copy()
                    cur["embedding_ids"] = kept_emb_ids
                    cur["episodic_ids"] = kept_epi_ids
                    cur["updated_at"] = (
                        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    )
                    index[key] = cur
                    _save_index(index_path, index)
            except Exception:
                # Non-fatal; continue with best-effort sync
                pass


# Switch to agentic flow to stream status updates and a final response
@flow(is_agentic_flow=True)
async def identity_file_embedding_graph_creation_flow(
    dto: GeneralConversationChatRequest,
    use_cpu: bool = True,
    ingest_to_graph: bool = True,
):
    """
    Identity-scoped flow: derive the files directory from the identity key
    and process PDFs found under it. Index and control files live inside the
    same per-identity directory.
    """ 
    dataspace_id = "default_ltm_dataspace"
    collection_name = "files"


    identity_key = identity_key_var.get()
    engine_utils = EngineUtils(identity_key)

    # Prepare context (kept minimal; no agent call in static flow)
    engine_utils.reset_dto_tool_context(dto)
    engine_utils.set_initial_system_prompt(dto, system_prompt=INTRO_PROMPT, replace=True)

    # Resolve per-identity files directory from environment + identity key
    base_dir = get_storage_base_dir()
    user_files_dir = Path(user_dir(base_dir, identity_key))
    logger.info("Resolved user files dir: identity=%s path=%s", identity_key, str(user_files_dir))
    os.makedirs(user_files_dir, exist_ok=True)

    path = user_files_dir
    if not path.is_dir():
        raise ValueError(
            f"The resolved user files directory '{str(path)}' does not exist or is not a valid directory."
        )
    
    document_files = sorted([
    p for p in path.rglob("*") 
    if p.is_file() and p.suffix.lower() in {".csv", ".docx", ".pptx", ".png", ".md", ".xlsx", ".pdf"}
    ])
    logger.info("Found %d PDF files on disk", len(document_files))
    # Keep a stable snapshot of all on-disk PDF paths for cleanup logic
    all_pdf_files = list(document_files)

    # Per-collection index file stored inside the per-identity files dir
    index_path = path / f".embedding_index_{collection_name}.json"
    index = _load_index(index_path)
    logger.info("Loaded index at %s with %d entries", str(index_path), len(index))
    # Serialize index writes across workers to avoid race conditions
    index_lock = asyncio.Lock()

    # Stage 0: ensure IDs in the local index actually exist in the backend
    await _sync_index_ids_with_backend(
        engine_utils,
        collection_name,
        index_path,
        index,
        index_lock,
        document_files,
    )

    scheduled_steps = [
        "extract_cpu" if use_cpu else "extract_gpu",
        "chunk_content",
        "compute_embeddings",
        "create_ltm_graph_entries",
        "enqueue_ingest_pipeline",
        "ingest_ltm_graph",
    ]

    results: list[dict] = []
    failures: list[dict] = []
    skipped: list[dict] = []
    updates: dict[str, dict] = {}

    # Pre-check: query storage for already embedded files and sync index
    # Assumption: file names are unique in this flow's scope
    remote_files = await engine_utils.get_all_files_with_all_elements(identity_key)

    remote_by_name: dict[str, dict] = {}
    for f in remote_files:
        base_name = os.path.basename(f.file.stored_name)
        emb_ids: list[str] = []
        file_key = f.file.key
        stored_name = f.file.filename
        # Build content parts from remote chunks/elements so we can ingest
        # without re-extracting when needed
        content_parts_remote: list[str] = []
        for ch in f.chunks:
            if ch.chunk.embedding_id:
                emb_ids.append(ch.chunk.embedding_id)
            # Create a similar content summary as in fresh processing
            content_parts_remote.append(
                f"Summary: {ch.chunk.summary}\nClassification: {ch.chunk.classification}"
            )
            for el in ch.elements:
                if el.embedding_id:
                    emb_ids.append(el.embedding_id)
                # Elements from remote contain `content`; include it directly
                if getattr(el, "content", None):
                    content_parts_remote.append(el.content)
        remote_by_name[base_name] = {
            "file_key": file_key,
            "stored_name": stored_name,
            "embedding_ids": emb_ids,
            "content_parts": content_parts_remote,
        }
    logger.info("Discovered %d remote files with embedded entities", len(remote_by_name))

    # Filter local files: if remote embeddings already exist, update index and
    # skip only when the previous run succeeded; otherwise process to create LTM graph
    to_process_files: list[Path] = []
    to_ingest_from_remote: list[Path] = []
    for p in document_files:
        info = remote_by_name.get(p.name)
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        file_hash = _hash_file(p)
        key = str(p)
        prev = index.get(key) or {}
        prev_ids_set = set(prev.get("embedding_ids") or [])
        prev_epi_ids = list(prev.get("episodic_ids") or [])
        prev_status = prev.get("status")

        # 1) Ensure every on-disk file is present in the index with current hash
        # Also sync embedding ids from remote if available
        new_ids_list = list(dict.fromkeys((info or {}).get("embedding_ids", []) ))
        merged_entry = {
            "name": p.name,
            "path": key,
            "collection": collection_name,
            "hash": file_hash,
            "embedding_ids": new_ids_list if info is not None else (prev.get("embedding_ids", []) or []),
            "episodic_ids": prev_epi_ids,
            # Keep prior status if set; otherwise start as unprocessed
            "status": prev_status or "unprocessed",
            "updated_at": timestamp,
        }
        # Only write if something changed or entry is new
        if (key not in index) or (set(merged_entry.get("embedding_ids", [])) != prev_ids_set) or (merged_entry.get("hash") != prev.get("hash")) or (merged_entry.get("status") != prev_status):
            async with index_lock:
                index[key] = merged_entry
                _save_index(index_path, index)
            logger.debug(
                "Indexed/synced file: name=%s status=%s emb_ids=%d epi_ids=%d",
                p.name,
                merged_entry.get("status"),
                len(merged_entry.get("embedding_ids") or []),
                len(merged_entry.get("episodic_ids") or []),
            )

        # 2) Decide action per rules
        if prev_status in ("embedded", "embedded_without_ltm"):
            # Do not reprocess or re-ingest episodic memory for embedded files.
            skipped.append(
                {
                    "name": _basename((info or {}).get("stored_name") or p.name),
                    "path": _basename(p.name),
                    "collection": collection_name,
                    "status": "embedded",
                    "embedding_id_count": len(new_ids_list) if info is not None else len(prev.get("embedding_ids", []) or []),
                    "timestamp": timestamp,
                }
            )
            logger.debug("Skip embedded file (no action): %s", p.name)
            continue

        # 3) Inconsistent state: unprocessed but has embedding ids -> reconcile
        if (prev_status == "unprocessed") and ((prev.get("embedding_ids") or [])):
            if info is not None:
                if not prev_epi_ids:
                    to_ingest_from_remote.append(p)
                else:
                    skipped.append(
                        {
                            "name": _basename((info or {}).get("stored_name") or p.name),
                            "path": _basename(p.name),
                            "collection": collection_name,
                            "status": "existing_embeddings",
                            "embedding_id_count": len((info or {}).get("embedding_ids") or []),
                            "timestamp": timestamp,
                        }
                    )
                continue
            else:
                # No backend record: prune stale embedding IDs before full processing
                try:
                    stale_ids = list(prev.get("embedding_ids") or [])
                    if stale_ids:
                        try:
                            await engine_utils.delete_embeddings_by_ids(collection_name, stale_ids)
                        except Exception:
                            pass
                        # Clear from local index so the run starts clean
                        async with index_lock:
                            cur = (index.get(key) or {}).copy()
                            cur["embedding_ids"] = []
                            cur["updated_at"] = timestamp
                            index[key] = cur
                            _save_index(index_path, index)
                        logger.info(
                            "Pruned %d stale embedding ids for %s before re-embedding",
                            len(stale_ids),
                            p.name,
                        )
                except Exception:
                    pass
                logger.info(
                    "Unprocessed with embedding IDs but no backend file found; will process fully: %s",
                    p.name,
                )

        # 4) If remote exists (has embeddings in DB), do not re-extract; just ingest if episodic missing
        if info is not None:
            if not prev_epi_ids:
                to_ingest_from_remote.append(p)
            else:
                skipped.append(
                    {
                        "name": _basename((info or {}).get("stored_name") or p.name),
                        "path": _basename(p.name),
                        "collection": collection_name,
                        "status": "existing_embeddings",
                        "embedding_id_count": len(new_ids_list),
                        "timestamp": timestamp,
                    }
                )
                logger.debug("Skip file with existing embeddings and episodic IDs: %s", p.name)
            continue

        # 5) No remote embeddings; proceed with full processing
        to_process_files.append(p)

    # Streaming run, sequentially to simplify status updates
    await asyncio.sleep(0)
    yield engine_utils.yield_status_update("Suche PDF-Dateien")

    # First, handle ingestion-only cases using remote content (no re-extraction)
    logger.info("Starting ingestion-only phase for %d files", len(to_ingest_from_remote))
    for p in to_ingest_from_remote:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        key = str(p)
        await asyncio.sleep(0)
        info = remote_by_name.get(p.name) or {}
        display_name = _basename(info.get("stored_name") or p.name)
        yield engine_utils.yield_status_update(f"Prüfe LTM-Status für: {display_name}")
        try:
            content_parts = list(info.get("content_parts") or [])
            if content_parts and ingest_to_graph:
                await asyncio.sleep(0)
                yield engine_utils.yield_status_update("Analysiere Fakten und ingestiere Episoden aus vorhandenen Embeddings")
                logger.info("Ingesting to LTM from remote content: %s (parts=%d)", p.name, len(content_parts))
                document_content = "\n\n".join(content_parts)
                subagent_stream = engine_utils.send_subagent_request(
                    subagent_name="ingest_into_longterm_memory",
                    dto=dto,
                    payload={
                        "text": document_content,
                        "dataspace_id": dataspace_id,
                        "reference_time_iso": timestamp,
                    },
                )
                final_payload: Optional[dict] = None
                episodic_ids: list[str] = []
                async for c in subagent_stream:
                    if c.final_ai_message_dto is None:
                        # Stream incremental updates and capture newly added episode uuids
                        try:
                            if c.chunk:
                                created_episode_content = c.chunk.choices[0].delta.content
                                if created_episode_content and created_episode_content.startswith("added_episode: "):
                                    test = created_episode_content[15:]
                                    created_episode_data = json.loads(test)
                                    uuid = (
                                        (created_episode_data or {})
                                        .get("added_data", {})
                                        .get("episode", {})
                                        .get("uuid")
                                    )
                                    if uuid and uuid not in episodic_ids:
                                        episodic_ids.append(uuid)
                                        # Persist incremental episodic id into the index
                                        try:
                                            async with index_lock:
                                                cur = index.get(key, {}).copy() if key in index else {
                                                    "name": p.name,
                                                    "path": key,
                                                    "collection": collection_name,
                                                    "hash": _hash_file(p),
                                                    "embedding_ids": list((info or {}).get("embedding_ids") or []),
                                                    "episodic_ids": [],
                                                    "status": "unprocessed",
                                                    "updated_at": timestamp,
                                                }
                                                cur_epi = list(cur.get("episodic_ids") or [])
                                                if uuid not in cur_epi:
                                                    cur_epi.append(uuid)
                                                cur["episodic_ids"] = cur_epi
                                                cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                                                index[key] = cur
                                                _save_index(index_path, index)
                                        except Exception:
                                            pass
                        except Exception as e:
                            logger.warning(str(e))
                        yield c
                        continue
                    try:
                        final_payload = json.loads(c.final_ai_message_dto.content)
                    except Exception:
                        final_payload = None
                if final_payload:
                    for chunk_res in final_payload.get("factual_for_chunk", []):
                        for added in chunk_res.get("added_episodes", []):
                            uuid = added.get("episode", {}).get("uuid")
                            if uuid and uuid not in episodic_ids:
                                episodic_ids.append(uuid)
                # Persist index with episodic ids and mark embedded (no reprocessing)
                async with index_lock:
                    cur = index.get(key, {}).copy() if key in index else {}
                    cur.update(
                        {
                            "name": p.name,
                            "path": key,
                            "collection": collection_name,
                            "hash": _hash_file(p),
                            "embedding_ids": list((info or {}).get("embedding_ids") or []),
                            "episodic_ids": episodic_ids,
                            "status": "embedded_without_ltm" if len(episodic_ids) == 0 else "embedded",
                            "updated_at": timestamp,
                        }
                    )
                    index[key] = cur
                    _save_index(index_path, index)
                logger.info(
                    "Ingested episodic IDs for %s: count=%d",
                    p.name,
                    len(episodic_ids),
                )
            # Report as result
            results.append(
                {
                    "name": display_name,
                    "path": _basename(p.name),
                    "collection": collection_name,
                    "status": "embedded",
                    "scheduled_steps": [
                        "ingest_ltm_graph",
                    ],
                    "pages": 0,
                    "elements": 0,
                    "embedding_id_count": len((info or {}).get("embedding_ids") or []),
                    "deleted_before_recreate": 0,
                    "deleted_episodes_before_recreate": 0,
                    "episodic_id_count": len((index.get(key) or {}).get("episodic_ids") or []),
                    "timestamp": timestamp,
                }
            )
        except (asyncio.CancelledError, GeneratorExit):
            # Client-side cancellation: remove any episodes created in this run and revert index
            try:
                epis_to_delete = list(episodic_ids if 'episodic_ids' in locals() else [])
                if epis_to_delete:
                    for u in list(epis_to_delete):
                        asyncio.create_task(engine_utils.delete_episode_from_ltm_graph(u))
                # Update index immediately (non-awaited, best-effort)
                cur = (index.get(key) or {}).copy()
                existing_epi = list(cur.get("episodic_ids") or [])
                cur["episodic_ids"] = [e for e in existing_epi if e not in (episodic_ids or [])]
                cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                index[key] = cur
                _save_index(index_path, index)
            except Exception as e:
                logger.warning(str(e))
            # Propagate cancellation so upstream can close stream
            raise
        except Exception as e:
            safe_err = _sanitize_error_message(str(e), str(p), str(path))
            logger.error("Ingestion-only failed for %s: %s", p.name, safe_err)
            failures.append(
                {
                    "name": display_name,
                    "path": _basename(p.name),
                    "collection": collection_name,
                    "status": "error",
                    "error": safe_err,
                    "timestamp": timestamp,
                }
            )
            async with index_lock:
                cur = index.get(key, {})
                cur.update(
                    {
                        "name": p.name,
                        "path": key,
                        "collection": collection_name,
                        "hash": _hash_file(p),
                        "embedding_ids": list((info or {}).get("embedding_ids") or []),
                        "episodic_ids": cur.get("episodic_ids", []) or [],
                        "status": "failed",
                        "updated_at": timestamp,
                    }
                )
                index[key] = cur
                _save_index(index_path, index)

    # Process each file sequentially for predictable streaming
    logger.info("Starting full processing phase for %d files", len(to_process_files))
    for p in to_process_files:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        created_embedding_ids: list[str] = []
        episodic_ids: list[str] = []
        page_count = 0
        element_count = 0

        file_hash = _hash_file(p)
        key = str(p)
        prev = index.get(key)

        await asyncio.sleep(0)
        # Prefer DB stored filename if available for user-facing messages
        db_info = remote_by_name.get(p.name) or {}
        display_name = _basename(db_info.get("stored_name") or p.name)
        yield engine_utils.yield_status_update(f"Verarbeite Datei: {display_name}")
        logger.info("Processing file: %s", p.name)

        try:
            # Immediately mark as unprocessed
            unprocessed_entry = {
                "name": p.name,
                "path": key,
                "collection": collection_name,
                "hash": file_hash,
                "embedding_ids": (prev or {}).get("embedding_ids", []) or [],
                "episodic_ids": (prev or {}).get("episodic_ids", []) or [],
                "status": "unprocessed",
                "updated_at": timestamp,
            }
            async with index_lock:
                index[key] = unprocessed_entry
                _save_index(index_path, index)

            # Delete previous episodic nodes if present
            deleted_episodes = 0
            if prev and prev.get("episodic_ids"):
                for episode_uuid in prev["episodic_ids"]:
                    try:
                        await engine_utils.delete_episode_from_ltm_graph(episode_uuid)
                        deleted_episodes += 1
                    except Exception as e:
                        logger.warning(str(e))
                if deleted_episodes:
                    async with index_lock:
                        cur = index.get(key, {})
                        cur["episodic_ids"] = []
                        cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                        index[key] = cur
                        _save_index(index_path, index)

            # Extract → chunk → embed
            await asyncio.sleep(0)
            yield engine_utils.yield_status_update("Extrahiere den Inhalt der Datei")
            logger.debug("Begin extraction/chunk/embedding: %s", p.name)

            content_parts: list[str] = []
            file_info = remote_by_name.get(p.name) or {}
            file_key = file_info.get("file_key")
            existing_ids_for_file = list(file_info.get("embedding_ids") or [])
            embeddings_already_exist = bool(existing_ids_for_file)
            chunk_meta_key = None

            async for chunk in iter_sse_chunks(
                engine_utils,
                str(p),
                "application/pdf",
                dto,
                use_cpu=use_cpu,
            ):
                page_count += 1
                await asyncio.sleep(0)
                yield engine_utils.yield_status_update(
                    f"Inhalt aus Seite/Abschnitt {chunk.index} extrahiert: {chunk.summary}",
                    with_icon=True,
                )
                classification = chunk.classification
                content_parts.append(
                    f"Summary: {chunk.summary}\nClassification: {classification}"
                )

                if not embeddings_already_exist:
                    created = await engine_utils.add_embedding_chunk(
                        collection_name=collection_name,
                        chunk_embedding=chunk.embedding,
                        chunk_document_text=chunk.summary,
                        meta_data={
                            "file_name": os.path.basename(str(p)),
                            "file_path": str(p),
                            "page": chunk.index,
                        },
                    )
                    emb_id = created.embedding_id
                    if emb_id:
                        created_embedding_ids.append(emb_id)
                        try:
                            if file_key:
                                await asyncio.sleep(0)
                                yield engine_utils.yield_status_update(
                                    f"Verknüpfe Teil {chunk.index} der Datei in meinem Gehirn"
                                )
                                chunk_meta = await engine_utils.create_file_chunk_meta_in_graph_db(
                                    identity_key,
                                    file_key,
                                    index=chunk.index,
                                    summary=chunk.summary,
                                    classification=chunk.classification,
                                    embedding_id=emb_id,
                                    embedding_model="bge-m3",
                                )
                                chunk_meta_key = chunk_meta.key
                        except Exception:
                            chunk_meta_key = None
                        # Persist incremental progress
                        try:
                            async with index_lock:
                                cur = index.get(key, {
                                    "name": p.name,
                                    "path": key,
                                    "collection": collection_name,
                                    "hash": file_hash,
                                    "embedding_ids": created_embedding_ids,
                                    "episodic_ids": [],
                                    "status": "unprocessed",
                                    "updated_at": timestamp,
                                }).copy()
                                cur["embedding_ids"] = list(created_embedding_ids)
                                cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                                index[key] = cur
                                _save_index(index_path, index)
                        except Exception:
                            pass

                # Optional: embed extracted page elements
                if not embeddings_already_exist:
                    logger.debug("Creating element embeddings in DB for: %s", p.name)
                    for i, el in enumerate(chunk.elements or []):
                        element_count += 1
                        if el.markdown_formatted_element:
                            content_parts.append(el.content)
                        created_el = await engine_utils.add_embedding_chunk(
                            collection_name=collection_name,
                            chunk_embedding=el.embedding,
                            chunk_document_text=el.text or "",
                            meta_data={
                                "file_name": os.path.basename(str(p)),
                                "file_path": str(p),
                                "page": chunk.index,
                                "element_id": el.id,
                                "element_type": el.type or "element",
                            },
                        )
                        el_emb_id = created_el.embedding_id
                        if el_emb_id:
                            await asyncio.sleep(0)
                            yield engine_utils.yield_status_update(
                                f"Verknüpfe Element {i} von Teil {chunk.index} der Datei in meinem Gehirn"
                            )
                            created_embedding_ids.append(el_emb_id)
                            try:
                                if chunk_meta_key:
                                    await engine_utils.create_file_chunk_element_meta_in_graph_db(
                                        identity_key,
                                        chunk_meta_key,
                                        index=el.index,
                                        content=el.content,
                                        category=el.category,
                                        embedding_id=el_emb_id,
                                        embedding_model=None,
                                    )
                            except Exception:
                                pass
                            try:
                                async with index_lock:
                                    cur = index.get(key, {}).copy() or {
                                        "name": p.name,
                                        "path": key,
                                        "collection": collection_name,
                                        "hash": file_hash,
                                        "embedding_ids": [],
                                        "episodic_ids": [],
                                        "status": "unprocessed",
                                        "updated_at": timestamp,
                                    }
                                    cur_embs = list(cur.get("embedding_ids") or [])
                                    cur_embs.append(el_emb_id)
                                    cur["embedding_ids"] = cur_embs
                                    cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                                    index[key] = cur
                                    _save_index(index_path, index)
                            except Exception:
                                pass

            # Ingest episodic memory
            if content_parts and ingest_to_graph:
                logger.info("Ingesting processed content into LTM: %s (pages=%d, elements=%d)", p.name, page_count, element_count)
                await asyncio.sleep(0)
                yield engine_utils.yield_status_update(
                    "Analysiere Fakten und ingestiere Episoden")
                document_content = "\n\n".join(content_parts)
                subagent_stream = engine_utils.send_subagent_request(
                    subagent_name="ingest_into_longterm_memory",
                    dto=dto,
                    payload={
                        "text": document_content,
                        "dataspace_id": dataspace_id,
                        "reference_time_iso": timestamp,
                    }
                )
                final_payload: Optional[dict] = None
                async for c in subagent_stream:
                    if c.final_ai_message_dto is None:
                        if c.chunk:
                            try:
                                created_episode_content = c.chunk.choices[0].delta.content
                                if created_episode_content and created_episode_content.startswith("added_episode: "):
                                    created_episode_data = json.loads(created_episode_content[16:])
                                    uuid = (
                                        (created_episode_data or {})
                                        .get("added_data", {})
                                        .get("episode", {})
                                        .get("uuid")
                                    )
                                    if uuid:
                                        # Track in memory
                                        if uuid not in episodic_ids:
                                            episodic_ids.append(uuid)
                                        # Persist incrementally to the local index
                                        try:
                                            async with index_lock:
                                                cur = index.get(
                                                    str(p),
                                                    {
                                                        "name": p.name,
                                                        "path": str(p),
                                                        "collection": collection_name,
                                                        "hash": file_hash,
                                                        "embedding_ids": created_embedding_ids,
                                                        "episodic_ids": [],
                                                        "status": "unprocessed",
                                                        "updated_at": timestamp,
                                                    },
                                                ).copy()
                                                cur_epi = list(cur.get("episodic_ids") or [])
                                                if uuid not in cur_epi:
                                                    cur_epi.append(uuid)
                                                cur["episodic_ids"] = cur_epi
                                                cur["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                                                index[str(p)] = cur
                                                _save_index(index_path, index)
                                        except Exception:
                                            pass
                            except Exception:
                                pass
                        yield c
                        continue
                    try:
                        final_payload = json.loads(c.final_ai_message_dto.content)
                    except Exception:
                        final_payload = None

                if final_payload:
                    for chunk_res in final_payload.get("factual_for_chunk", []):
                        for added in chunk_res.get("added_episodes", []):
                            uuid = added.get("episode", {}).get("uuid")
                            if not uuid:
                                continue
                            if uuid not in episodic_ids:
                                episodic_ids.append(uuid)
                            try:
                                async with index_lock:
                                    cur = index.get(
                                        str(p),
                                        {
                                            "name": p.name,
                                            "path": str(p),
                                            "collection": collection_name,
                                            "hash": file_hash,
                                            "embedding_ids": created_embedding_ids,
                                            "episodic_ids": [],
                                            "status": "unprocessed",
                                            "updated_at": timestamp,
                                        },
                                    ).copy()
                                    epi_ids = list(cur.get("episodic_ids") or [])
                                    if uuid not in epi_ids:
                                        epi_ids.append(uuid)
                                    cur["episodic_ids"] = epi_ids
                                    cur["updated_at"] = (
                                        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                                    )
                                    index[str(p)] = cur
                                    _save_index(index_path, index)
                            except Exception:
                                pass

            # Persist final index entry for this file
            embedding_ids_to_store = (
                created_embedding_ids if not embeddings_already_exist else existing_ids_for_file
            )
            index_entry = {
                "name": p.name,
                "path": str(p),
                "collection": collection_name,
                "hash": file_hash,
                "embedding_ids": embedding_ids_to_store,
                "episodic_ids": episodic_ids,
                "status": "embedded_without_ltm" if len(episodic_ids) == 0 else "embedded",
                "updated_at": timestamp,
            }
            # Frontend payload must not leak server paths: prefer DB filename
            results.append(
                {
                    "name": _basename((remote_by_name.get(p.name) or {}).get("stored_name") or p.name),
                    "path": _basename(p.name),
                    "collection": collection_name,
                    "status": "embedded",
                    "scheduled_steps": scheduled_steps,
                    "pages": page_count,
                    "elements": element_count,
                    "embedding_id_count": len(embedding_ids_to_store),
                    "deleted_before_recreate": 0,
                    "deleted_episodes_before_recreate": 0,
                    "episodic_id_count": len(episodic_ids),
                    "timestamp": timestamp,
                }
            )
            async with index_lock:
                index[str(p)] = index_entry
                _save_index(index_path, index)
            logger.info(
                "Completed file: %s emb_ids=%d epi_ids=%d",
                p.name,
                len(embedding_ids_to_store),
                len(episodic_ids),
            )

        except (asyncio.CancelledError, GeneratorExit):
            # Client-side cancellation: delete only the data created in this run
            try:
                # Remove episodes created so far (batch, fire-and-forget)
                if episodic_ids:
                    for u in list(episodic_ids):
                        asyncio.create_task(engine_utils.delete_episode_from_ltm_graph(u))
                # Remove embeddings created so far (pages and elements)
                if created_embedding_ids:
                    asyncio.create_task(engine_utils.delete_embeddings_by_ids(
                        collection_name, list(created_embedding_ids)
                    ))
                # Revert index to its pre-run state immediately
                prev_embs = (prev or {}).get("embedding_ids", []) or []
                prev_epis = (prev or {}).get("episodic_ids", []) or []
                cur = {
                    "name": p.name,
                    "path": str(p),
                    "collection": collection_name,
                    "hash": file_hash,
                    "embedding_ids": list(prev_embs),
                    "episodic_ids": list(prev_epis),
                    "status": "unprocessed",
                    "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
                index[str(p)] = cur
                _save_index(index_path, index)
            except Exception:
                pass
            # Propagate cancellation so upstream can close stream
            raise
        except Exception as e:
            # Sanitize error to avoid path leakage
            safe_err = _sanitize_error_message(str(e), str(p), str(path))
            logger.error("Processing failed for %s: %s", p.name, safe_err)
            failures.append(
                {
                    "name": _basename((remote_by_name.get(p.name) or {}).get("stored_name") or p.name),
                    "path": _basename(p.name),
                    "collection": collection_name,
                    "status": "error",
                    "error": safe_err,
                    "timestamp": timestamp,
                }
            )
            async with index_lock:
                index[str(p)] = {
                    "name": p.name,
                    "path": str(p),
                    "collection": collection_name,
                    "hash": file_hash,
                    "embedding_ids": [],
                    "episodic_ids": [],
                    "status": "failed",
                    "updated_at": timestamp,
                }
                _save_index(index_path, index)

    # Cleanup: remove index entries for files no longer present on disk
    # Important: Use the full on-disk set to decide removals, not the filtered list
    current_paths = set(str(p) for p in all_pdf_files)
    removed_files: list[dict] = []
    to_delete_keys = [k for k in list(index.keys()) if k not in current_paths]
    logger.info("Cleanup phase: considering %d index entries not on disk", len(to_delete_keys))
    for k in to_delete_keys:
        entry = index.get(k) or {}
        display_name = _basename(entry.get("name") or k)
        logger.debug("Attempting to remove backend records for missing file: %s", display_name)
        # Ensure embeddings and episodic nodes are actually removed before dropping index entry
        emb_ids = list(entry.get("embedding_ids") or [])
        epi_ids = list(entry.get("episodic_ids") or [])
        # If a remote file exists for this basename, prefer deleting the remote file (which also removes embeddings)
        remote_deleted = False
        try:
            info = remote_by_name.get(display_name) if 'remote_by_name' in locals() else None
            file_key = (info or {}).get("file_key")
            if file_key:
                try:
                    ok = await engine_utils.delete_file(file_key)
                    remote_deleted = bool(ok)
                except Exception:
                    # Fallback: try deleting only the embedded metadata for the file
                    try:
                        ok2 = await engine_utils.delete_embedded_file_meta_endpoint(file_key)
                        remote_deleted = bool(ok2)
                    except Exception:
                        remote_deleted = False
        except Exception:
            remote_deleted = False
        # Attempt per-id deletion only if remote deletion path wasn't taken or failed
        if emb_ids and not remote_deleted:
            try:
                await engine_utils.delete_embeddings_by_ids(collection_name, emb_ids)
            except Exception:
                pass
        remaining_emb_ids: list[str] = []
        for emb_id in emb_ids:
            try:
                if await _embedding_exists(engine_utils, collection_name, emb_id):
                    remaining_emb_ids.append(emb_id)
            except Exception:
                # On verification error, conservatively keep the id
                remaining_emb_ids.append(emb_id)
        # Episodic deletions
        for episode_uuid in epi_ids:
            try:
                await engine_utils.delete_episode_from_ltm_graph(episode_uuid)
            except Exception:
                pass
        remaining_epi_ids: list[str] = []
        for episode_uuid in epi_ids:
            try:
                if await _episode_exists(engine_utils, episode_uuid):
                    remaining_epi_ids.append(episode_uuid)
            except Exception:
                remaining_epi_ids.append(episode_uuid)

        if not remaining_emb_ids and not remaining_epi_ids:
            # All related records gone; remove index entry and record removal
            removed_files.append(
                {
                    "name": _basename(entry.get("name") or k),
                    "path": _basename(entry.get("name") or k),
                    "collection": entry.get("collection", collection_name),
                    "removed_embedding_count": len(emb_ids),
                    "removed_episodic_count": len(epi_ids),
                }
            )
            async with index_lock:
                index.pop(k, None)
                _save_index(index_path, index)
            logger.info("Removed index entry for missing file: %s", _basename(entry.get("name") or k))
        else:
            # Could not remove everything; retain entry with remaining ids and mark as failed
            async with index_lock:
                cur = (index.get(k) or {}).copy()
                cur.update(
                    {
                        "embedding_ids": remaining_emb_ids,
                        "episodic_ids": remaining_epi_ids,
                        "status": "failed",
                        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    }
                )
                index[k] = cur
                _save_index(index_path, index)
            logger.warning(
                "Partial cleanup for missing file %s: remaining_emb=%d remaining_epi=%d",
                _basename(entry.get("name") or k),
                len(remaining_emb_ids),
                len(remaining_epi_ids),
            )

    # Apply index updates and persist (locked)
    async with index_lock:
        index.update(updates)
        _save_index(index_path, index)

    final_result = {
        "collection": collection_name,
        # Do not include server folder path in frontend payload
        "pdf_count": len(document_files),
        "embedded_files": results,
        "failed_files": failures,
        "skipped_files": skipped,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "removed_files": removed_files,
    }

    logger.info(
        "Finished run: pdf=%d embedded=%d failed=%d skipped=%d removed=%d",
        len(document_files),
        len(results),
        len(failures),
        len(skipped),
        len(removed_files),
    )
    # Stream final AI response as one chunk
    yield AAAChatCompletionChunk(
        final_ai_message_dto=MessageDTO(
            key="",
            content=json.dumps(final_result, indent=0),
            tool_calls=[],
            chat_key=dto.chat_key,
            is_ai_speaking=True,
            role="assistant",
            actor="flow",
            flow_id="identity_file_embedding_graph_creation_flow",
            created_at=datetime.now(),
        )
    )


def _hash_file(p: Path) -> str:
    sha = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _load_index(path: Path) -> dict:
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        # Corrupt or unreadable index; start fresh
        pass
    return {}


def _save_index(path: Path, data: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        # Ignore write errors silently to avoid breaking the flow
        pass


# No per-ID HTTP deletion; handled via EngineUtils.delete_embeddings_by_ids
