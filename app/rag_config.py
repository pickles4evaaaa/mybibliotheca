"""Utilities for loading and saving Retrieval-Augmented Generation (RAG) settings.

RAG configuration mirrors the AI settings flow:
- Base values live in the project `.env`
- Runtime overrides persist inside `data/rag_config.json`
- Access is cached briefly to avoid rereading the filesystem per request
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Mapping
import json
import os
import time

from flask import current_app

# Cache keys stored on the Flask app object
_CACHE_KEY = "_cached_rag_config"
_CACHE_TS_KEY = "_cached_rag_config_ts"
_CACHE_TTL_SECONDS = 5

# All managed environment keys for the RAG system
RAG_ENV_KEYS = [
    "RAG_ENABLED",
    "RAG_DB_PATH",
    "RAG_COLLECTION_NAME",
    "RAG_DISTANCE_METRIC",
    "RAG_CHUNK_SIZE",
    "RAG_CHUNK_OVERLAP",
    "RAG_CHUNK_UNIT",
    "RAG_LLM_PROVIDER",
    "RAG_LLM_BASE_URL",
    "RAG_LLM_MODEL",
    "RAG_LLM_API_KEY",
    "RAG_EMBEDDING_PROVIDER",
    "RAG_EMBEDDING_BASE_URL",
    "RAG_EMBEDDING_MODEL",
    "RAG_EMBEDDING_API_KEY",
    "RAG_EMBEDDING_DIMENSIONS",
]

DEFAULT_RAG_CONFIG: Dict[str, str] = {
    "RAG_ENABLED": "false",
    # Store the vector store under ./data/chroma so it rides the main volume
    "RAG_DB_PATH": "data/chroma",
    "RAG_COLLECTION_NAME": "book-snippets",
    "RAG_DISTANCE_METRIC": "cosine",
    # Chunk size/overlap expressed in whole words – 800/120 matches ~3-4 paragraphs,
    # enough context for book passages without diluting relevance.
    "RAG_CHUNK_SIZE": "800",
    "RAG_CHUNK_OVERLAP": "120",
    "RAG_CHUNK_UNIT": "words",
    # Default to a local Ollama endpoint for both LLM + embeddings to keep self-host friendly.
    "RAG_LLM_PROVIDER": "ollama",
    "RAG_LLM_BASE_URL": "http://localhost:11434",
    "RAG_LLM_MODEL": "llama3.1:8b-instruct",
    "RAG_LLM_API_KEY": "",
    "RAG_EMBEDDING_PROVIDER": "ollama",
    "RAG_EMBEDDING_BASE_URL": "http://localhost:11434",
    "RAG_EMBEDDING_MODEL": "nomic-embed-text",
    "RAG_EMBEDDING_API_KEY": "",
    "RAG_EMBEDDING_DIMENSIONS": "768",
}


@dataclass(frozen=True)
class RAGConfig:
    """Lightweight typed view over the raw settings."""

    values: Dict[str, str]

    def get(self, key: str, default: str | None = None) -> str:
        return self.values.get(key, DEFAULT_RAG_CONFIG.get(key, default or ""))

    @property
    def enabled(self) -> bool:
        return self.values.get("RAG_ENABLED", "false").lower() == "true"

    @property
    def chunk_size(self) -> int:
        return _coerce_positive_int(self.values.get("RAG_CHUNK_SIZE"), fallback=800)

    @property
    def chunk_overlap(self) -> int:
        return _coerce_positive_int(self.values.get("RAG_CHUNK_OVERLAP"), fallback=120)

    @property
    def db_path(self) -> Path:
        return Path(self.values.get("RAG_DB_PATH", DEFAULT_RAG_CONFIG["RAG_DB_PATH"]))


def _project_root_env_path() -> Path:
    try:
        root_dir = Path(current_app.root_path).parent
    except Exception:
        root_dir = Path(__file__).resolve().parents[1]
    return root_dir / ".env"


def _data_dir() -> Path:
    try:
        cfg_dir = current_app.config.get("DATA_DIR", "data")
    except Exception:
        cfg_dir = "data"
    return Path(cfg_dir)


def _coerce_positive_int(raw_val: str | None, fallback: int) -> int:
    try:
        value = int(str(raw_val or "").strip())
        return value if value > 0 else fallback
    except Exception:
        return fallback


def load_rag_config(force_refresh: bool = False) -> Dict[str, str]:
    """Load RAG configuration (with caching) from .env + JSON overlay."""
    try:
        if not force_refresh:
            cached = current_app.config.get(_CACHE_KEY)
            cached_ts = current_app.config.get(_CACHE_TS_KEY)
            if cached and cached_ts and (time.time() - cached_ts) < _CACHE_TTL_SECONDS:
                return cached
    except Exception:
        pass

    config: Dict[str, str] = {}
    env_path = _project_root_env_path()

    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key in RAG_ENV_KEYS:
                config[key] = value.strip()

    rag_json_path = _data_dir() / "rag_config.json"
    if rag_json_path.exists():
        try:
            with rag_json_path.open("r", encoding="utf-8") as fh:
                json_data = json.load(fh)
            if isinstance(json_data, dict):
                for key, value in json_data.items():
                    if key in RAG_ENV_KEYS:
                        config[key] = str(value)
        except Exception as exc:
            try:
                current_app.logger.warning("Failed reading rag_config.json: %s", exc)
            except Exception:
                pass

    for key, default in DEFAULT_RAG_CONFIG.items():
        config.setdefault(key, default)

    try:
        current_app.config[_CACHE_KEY] = config
        current_app.config[_CACHE_TS_KEY] = time.time()
    except Exception:
        pass
    return config


def save_rag_config(config: Dict[str, Any]) -> bool:
    """Persist RAG settings back to .env and the JSON overlay."""
    env_path = _project_root_env_path()
    try:
        env_path.parent.mkdir(parents=True, exist_ok=True)
        existing_lines = env_path.read_text().splitlines(keepends=True) if env_path.exists() else []
        updated = {key: False for key in RAG_ENV_KEYS}
        new_lines: list[str] = []

        for line in existing_lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                new_lines.append(line)
                continue
            key, _sep, _val = stripped.partition("=")
            key = key.strip()
            if key in updated:
                value = str(config.get(key, DEFAULT_RAG_CONFIG.get(key, ""))).strip()
                new_lines.append(f"{key}={value}\n")
                updated[key] = True
            else:
                new_lines.append(line)

        missing_keys = [key for key, done in updated.items() if not done]
        if missing_keys:
            new_lines.append("\n# RAG / Vector Search Configuration (managed by Admin UI)\n")
            for key in missing_keys:
                value = str(config.get(key, DEFAULT_RAG_CONFIG.get(key, ""))).strip()
                new_lines.append(f"{key}={value}\n")

        env_path.write_text("".join(new_lines))

        rag_json_path = _data_dir() / "rag_config.json"
        rag_json_path.parent.mkdir(parents=True, exist_ok=True)
        subset = {key: str(config.get(key, DEFAULT_RAG_CONFIG.get(key, ""))) for key in RAG_ENV_KEYS}
        with rag_json_path.open("w", encoding="utf-8") as fh:
            json.dump(subset, fh, indent=2)

        try:
            current_app.config.pop(_CACHE_KEY, None)
            current_app.config.pop(_CACHE_TS_KEY, None)
        except Exception:
            pass
        return True
    except Exception as exc:
        try:
            current_app.logger.error("Error saving RAG config: %s", exc)
        except Exception:
            print(f"Error saving RAG config: {exc}")
        return False


def resolve_rag_db_path(raw_path: str | None) -> Path:
    """Resolve the configured RAG DB path to an absolute location on disk."""
    candidate = Path(raw_path or DEFAULT_RAG_CONFIG["RAG_DB_PATH"])
    if candidate.is_absolute():
        return candidate
    try:
        data_dir = current_app.config.get("DATA_DIR", None)
    except Exception:
        data_dir = None
    if data_dir:
        base = Path(data_dir)
        if not base.is_absolute():
            base = _project_root_env_path().parent / base
    else:
        base = _project_root_env_path().parent
    return (base / candidate).resolve()


def rag_config_dataclass(force_refresh: bool = False) -> RAGConfig:
    return RAGConfig(load_rag_config(force_refresh=force_refresh))


def build_rag_config_payload(data: Mapping[str, Any]) -> Dict[str, str]:
    """Normalize arbitrary mapping input into a rag_config.json payload."""
    existing = load_rag_config()
    payload = existing.copy()

    def _text(name: str, fallback_key: str, default_value: str) -> str:
        raw = str(data.get(name, existing.get(fallback_key, default_value))).strip()
        return raw or existing.get(fallback_key, default_value)

    payload['RAG_ENABLED'] = 'true' if data.get('rag_enabled') else 'false'
    payload['RAG_DB_PATH'] = _text('rag_db_path', 'RAG_DB_PATH', 'data/chroma')
    payload['RAG_COLLECTION_NAME'] = _text('rag_collection_name', 'RAG_COLLECTION_NAME', 'book-snippets')
    payload['RAG_DISTANCE_METRIC'] = _text('rag_distance_metric', 'RAG_DISTANCE_METRIC', 'cosine')

    chunk_size = _coerce_positive_int(str(data.get('rag_chunk_size', existing.get('RAG_CHUNK_SIZE'))), _coerce_positive_int(existing.get('RAG_CHUNK_SIZE'), 800))
    chunk_overlap = _coerce_positive_int(str(data.get('rag_chunk_overlap', existing.get('RAG_CHUNK_OVERLAP'))), _coerce_positive_int(existing.get('RAG_CHUNK_OVERLAP'), 120))
    if chunk_overlap >= chunk_size:
        chunk_overlap = max(chunk_size // 4, 50)
    payload['RAG_CHUNK_SIZE'] = str(chunk_size)
    payload['RAG_CHUNK_OVERLAP'] = str(chunk_overlap)
    payload['RAG_CHUNK_UNIT'] = 'words'

    payload['RAG_LLM_PROVIDER'] = _text('rag_llm_provider', 'RAG_LLM_PROVIDER', 'ollama')
    payload['RAG_LLM_BASE_URL'] = _text('rag_llm_base_url', 'RAG_LLM_BASE_URL', 'http://localhost:11434')
    payload['RAG_LLM_MODEL'] = _text('rag_llm_model', 'RAG_LLM_MODEL', 'llama3.1:8b-instruct')
    payload['RAG_LLM_API_KEY'] = str(data.get('rag_llm_api_key', existing.get('RAG_LLM_API_KEY', ''))).strip()

    payload['RAG_EMBEDDING_PROVIDER'] = _text('rag_embedding_provider', 'RAG_EMBEDDING_PROVIDER', 'ollama')
    payload['RAG_EMBEDDING_BASE_URL'] = _text('rag_embedding_base_url', 'RAG_EMBEDDING_BASE_URL', 'http://localhost:11434')
    payload['RAG_EMBEDDING_MODEL'] = _text('rag_embedding_model', 'RAG_EMBEDDING_MODEL', 'nomic-embed-text')
    payload['RAG_EMBEDDING_API_KEY'] = str(data.get('rag_embedding_api_key', existing.get('RAG_EMBEDDING_API_KEY', ''))).strip()
    payload['RAG_EMBEDDING_DIMENSIONS'] = str(
        _coerce_positive_int(
            str(data.get('rag_embedding_dimensions', existing.get('RAG_EMBEDDING_DIMENSIONS'))),
            _coerce_positive_int(existing.get('RAG_EMBEDDING_DIMENSIONS'), 768)
        )
    )
    return payload
