import json
import os
import re
from pathlib import Path
from typing import Dict, List, Literal, Optional

from aaa_types.dtos import GeneralConversationChatRequest
from flow_kit.engine_utils import identity_key_var
from flow_kit.registry import flow
from pydantic import BaseModel, Field

"""
Returns the per-identity embedding index JSON used by
identity_file_embedding_graph_creation_flow.

Index location (per identity):
  <AAA_STORAGE_BASE_DIR>/<sanitized-identity>/files/.embedding_index_files.json

- If the index does not exist, returns an empty object and exists=False.
"""


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


@flow()
async def identity_file_embedding_graph_index_getter_flow(
    dto: GeneralConversationChatRequest,
    collection_name: str = "files",
):
    """
    Load and return the per-identity embedding index JSON for the given
    collection (defaults to "files").
    """
    identity_key = identity_key_var.get()
    base_dir = get_storage_base_dir()
    user_files_dir = Path(user_dir(base_dir, identity_key))
    os.makedirs(user_files_dir, exist_ok=True)

    index_path = user_files_dir / f".embedding_index_{collection_name}.json"

    if not index_path.exists():
        return {
            "collection": collection_name,
            # "folder": str(user_files_dir),
            # "index_path": str(index_path),
            "exists": False,
            "index": {},
        }

    try:
        with open(index_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        # If the index is unreadable/corrupt, surface minimal diagnostics
        return {
            "collection": collection_name,
            # "folder": str(user_files_dir),
            # "index_path": str(index_path),
            "exists": True,
            "error": str(e),
            "index": {},
        }

    # Build DTO for type safety, then return as plain dict
    try:
        index_dto = build_embedding_index_dto(data)
        raw_payload = index_dto.model_dump()
    except Exception:
        raw_payload = data if isinstance(data, dict) else {}

    # Sanitize: do not return server paths. Re-key entries by filename and
    # replace any 'path' fields with the basename only.
    def _basename(v: str | None) -> str:
        if not v:
            return ""
        try:
            return os.path.basename(v)
        except Exception:
            return v

    entries = {}
    for k, v in (raw_payload.get("entries") or {}).items():
        try:
            name = v.get("name") or _basename(v.get("path") or k)
            safe_name = _basename(name)
            safe_entry = dict(v)
            safe_entry["name"] = safe_name
            safe_entry["path"] = safe_name
            entries[safe_name] = safe_entry
        except Exception:
            continue
    index_payload = {"entries": entries}

    return {
        "collection": collection_name,
        # "folder": str(user_files_dir),
        # "index_path": str(index_path),
        "exists": True,
        "index": index_payload,
    }


# ---- DTOs: Embedding Index (mirrors creation flow) ----

IndexStatus = Literal["unprocessed", "embedded", "failed", "embedded_without_ltm"]


class EmbeddingIndexEntryDTO(BaseModel):
    name: str
    path: str
    collection: str
    hash: Optional[str] = None
    embedding_ids: List[str] = Field(default_factory=list)
    episodic_ids: List[str] = Field(default_factory=list)
    status: Optional[IndexStatus] = None
    updated_at: Optional[str] = None


class EmbeddingIndexDTO(BaseModel):
    entries: Dict[str, EmbeddingIndexEntryDTO] = Field(default_factory=dict)


def build_embedding_index_dto(raw: dict) -> EmbeddingIndexDTO:
    entries: Dict[str, EmbeddingIndexEntryDTO] = {}
    for k, v in (raw or {}).items():
        try:
            if isinstance(v, dict):
                entries[k] = EmbeddingIndexEntryDTO.model_validate(v)
        except Exception:
            continue
    return EmbeddingIndexDTO(entries=entries)
