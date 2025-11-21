"""ChromaDB-backed vector store service for Retrieval-Augmented experiences."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
import os
import threading

import requests

try:  # chromadb is an optional dependency everywhere else, so guard imports
    import chromadb
    from chromadb.config import Settings
except ImportError:  # pragma: no cover - handled at runtime if dependency missing
    chromadb = None  # type: ignore
    Settings = None  # type: ignore

from flask import current_app

from app.rag_config import rag_config_dataclass, RAGConfig


@dataclass
class Chunk:
    """Simple representation of a chunk of text destined for vector storage."""

    text: str
    metadata: Dict[str, Any]


class EmbeddingProvider:
    """Thin wrapper that knows how to call the configured embedding endpoint."""

    def __init__(self, config: RAGConfig):
        self.config = config

    def embed_documents(self, texts: Sequence[str]) -> List[List[float]]:
        return self._embed(texts)

    def embed_query(self, text: str) -> List[float]:
        return self._embed([text])[0]

    def _embed(self, texts: Sequence[str]) -> List[List[float]]:
        provider = (self.config.values.get("RAG_EMBEDDING_PROVIDER") or "ollama").lower()
        current_app.logger.info(f"RAG: Embedding {len(texts)} texts using provider '{provider}'")
        current_app.logger.debug(f"RAG: Embedding texts: {[t[:50] + '...' for t in texts]}")
        if provider == "openai":
            return self._embed_openai(texts)
        if provider == "ollama":
            return self._embed_ollama(texts)
        raise RuntimeError(f"Unsupported embedding provider: {provider}")

    def _embed_openai(self, texts: Sequence[str]) -> List[List[float]]:
        api_key = (self.config.values.get("RAG_EMBEDDING_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("OpenAI embedding provider selected but no API key configured.")
        base_url = (self.config.values.get("RAG_EMBEDDING_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        model = (self.config.values.get("RAG_EMBEDDING_MODEL") or "text-embedding-3-large").strip()
        url = f"{base_url}/embeddings"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        current_app.logger.info(f"RAG: Calling OpenAI embeddings at {url} model={model}")
        response = requests.post(url, headers=headers, json={"model": model, "input": list(texts)}, timeout=45)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or []
        if len(data) != len(texts):
            raise RuntimeError("Embedding response size mismatch from OpenAI.")
        return [entry.get("embedding", []) for entry in data]

    def _embed_ollama(self, texts: Sequence[str]) -> List[List[float]]:
        base_url = (self.config.values.get("RAG_EMBEDDING_BASE_URL") or "http://localhost:11434").rstrip("/")
        if base_url.endswith("/v1"):
            base_url = base_url[:-3]
        model = (self.config.values.get("RAG_EMBEDDING_MODEL") or "nomic-embed-text").strip()
        url = f"{base_url}/api/embeddings"
        current_app.logger.info(f"RAG: Calling Ollama embeddings at {url} model={model} for {len(texts)} texts")
        embeddings: List[List[float]] = []
        for i, text in enumerate(texts):
            if i % 10 == 0:
                current_app.logger.debug(f"RAG: Ollama embedding progress {i}/{len(texts)}")
            try:
                response = requests.post(url, json={"model": model, "prompt": text}, timeout=45)
                response.raise_for_status()
                payload = response.json()
                emb = payload.get("embedding")
                if not emb:
                    current_app.logger.error(f"RAG: Ollama returned empty embedding for text chunk {i}")
                    raise RuntimeError("Ollama returned an empty embedding vector.")
                embeddings.append(emb)
            except Exception as e:
                current_app.logger.error(f"RAG: Ollama embedding failed for chunk {i}: {e}")
                raise
        return embeddings


class RAGVectorService:
    """High-level helper that wraps Chroma persistent storage for RAG."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._config = rag_config_dataclass()
        self._client = None
        self._collection = None
        self._embedding_provider = EmbeddingProvider(self._config)
        # Disable telemetry globally to honor admin requirements.
        os.environ.setdefault("CHROMA_TELEMETRY_ENABLED", "0")
        os.environ.setdefault("ANONYMIZED_TELEMETRY", "false")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def reload_configuration(self) -> None:
        with self._lock:
            self._config = rag_config_dataclass(force_refresh=True)
            self._embedding_provider = EmbeddingProvider(self._config)
            self._client = None
            self._collection = None

    def chunk_text(self, text: str, chunk_size: int | None = None, chunk_overlap: int | None = None) -> List[Chunk]:
        """Split text into overlapping word chunks suited for long-form books."""
        if not text:
            return []
        chunk_size = chunk_size or self._config.chunk_size
        chunk_overlap = chunk_overlap or self._config.chunk_overlap
        if chunk_size <= 0:
            chunk_size = 800
        if chunk_overlap >= chunk_size:
            chunk_overlap = max(chunk_size // 4, 50)
            
        words = text.split()
        
        chunks: List[Chunk] = []
        start = 0
        index = 0
        while start < len(words):
            end = min(len(words), start + chunk_size)
            chunk_words = words[start:end]
            chunk_text_value = " ".join(chunk_words).strip()
            if chunk_text_value:
                chunks.append(Chunk(text=chunk_text_value, metadata={"chunk_index": index}))
                index += 1
            
            if end == len(words):
                break
                
            start = end - chunk_overlap
            if start <= 0: # Should not happen if logic is correct but safety check
                 start = end
                 
        return chunks

    def upsert_book(self, *, book_id: str, text: str, metadata: Dict[str, Any] | None = None) -> int:
        """Chunk + embed + index a book's content."""
        # current_app.logger.info(f"RAG: upsert_book called for {book_id}. Text length: {len(text)}")
        
        if not self._config.enabled:
            current_app.logger.warning(f"RAG: upsert_book skipped for {book_id} - RAG disabled")
            raise RuntimeError("RAG is disabled. Enable it in Admin Settings > Vector Search.")
            
        if chromadb is None:
            current_app.logger.error(f"RAG: upsert_book failed for {book_id} - chromadb missing")
            raise RuntimeError("chromadb is not installed. Add it to your environment to enable RAG.")
            
        metadata = metadata or {}
        
        chunks = self.chunk_text(text)
        if not chunks:
            current_app.logger.warning(f"RAG: No chunks generated for {book_id}")
            return 0
            
        current_app.logger.info(f"RAG: Generated {len(chunks)} chunks for {book_id}. Starting embedding...")
        
        try:
            embeddings = self._embedding_provider.embed_documents([chunk.text for chunk in chunks])
        except Exception as e:
            current_app.logger.error(f"RAG: Embedding generation failed for {book_id}: {e}")
            raise
            
        current_app.logger.info(f"RAG: Generated {len(embeddings)} embeddings for {book_id}. Upserting to Chroma...")
        try:
            collection = self._get_collection()
            # Remove previous copies for this book to avoid duplicates
            collection.delete(where={"book_id": book_id})
            
            ids = [f"{book_id}:{chunk.metadata['chunk_index']}" for chunk in chunks]
            documents = [chunk.text for chunk in chunks]
            metadatas = [
                {
                    **metadata,
                    **chunk.metadata,
                    "book_id": book_id,
                }
                for chunk in chunks
            ]
            collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings)
            current_app.logger.info(f"RAG: Upsert successful for {book_id}")
            return len(chunks)
        except Exception as e:
            current_app.logger.error(f"RAG: Chroma upsert failed for {book_id}: {e}")
            raise

    def delete_book(self, book_id: str) -> None:
        if not self._config.enabled or chromadb is None:
            return
        try:
            self._get_collection().delete(where={"book_id": book_id})
        except Exception as exc:
            current_app.logger.warning("Failed to delete RAG chunks for %s: %s", book_id, exc)

    def query(self, *, text: str, n_results: int = 5, where: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not self._config.enabled:
            raise RuntimeError("RAG is disabled.")
        if chromadb is None:
            raise RuntimeError("chromadb is not installed")
        embedding = self._embedding_provider.embed_query(text)
        collection = self._get_collection()
        return collection.query(
            query_embeddings=[embedding],
            n_results=n_results,
            where=where,
            include=["metadatas", "documents", "distances"],
        )

    def list_indexed_books(self) -> List[Dict[str, Any]]:
        """List all books currently indexed in the vector store."""
        if not self._config.enabled or chromadb is None:
            return []
        try:
            # Fetch all metadata to find unique books
            # Note: This is not efficient for massive datasets but fine for personal use
            result = self._get_collection().get(include=["metadatas"])
            metadatas = result.get("metadatas", [])
            
            books = {}
            for meta in metadatas:
                if not meta: continue
                book_id = meta.get("book_id")
                if book_id and book_id not in books:
                    books[book_id] = {
                        "book_id": book_id,
                        "title": meta.get("title", "Unknown Title"),
                        "chunk_count": 0
                    }
                if book_id:
                    books[book_id]["chunk_count"] += 1
            
            return sorted(list(books.values()), key=lambda x: x['title'])
        except Exception as exc:
            current_app.logger.error("Failed to list indexed books: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _get_collection(self):
        with self._lock:
            if self._collection is not None:
                return self._collection
            client = self._get_client()
            collection_name = self._config.values.get("RAG_COLLECTION_NAME") or "book-snippets"
            metadata = {"hnsw:space": self._config.values.get("RAG_DISTANCE_METRIC", "cosine")}
            self._collection = client.get_or_create_collection(name=collection_name, metadata=metadata)
            return self._collection

    def _get_client(self):
        if chromadb is None:
            raise RuntimeError("chromadb package missing")
        with self._lock:
            if self._client is not None:
                return self._client
            path = self._get_storage_path()
            path.mkdir(parents=True, exist_ok=True)
            if Settings is None:
                raise RuntimeError("chromadb Settings unavailable; check installation")
            settings = Settings(anonymized_telemetry=False)
            self._client = chromadb.PersistentClient(path=str(path), settings=settings)
            return self._client

    def _get_storage_path(self) -> Path:
        raw_path = self._config.values.get("RAG_DB_PATH") or "data/chroma"
        storage_path = Path(raw_path)
        if storage_path.is_absolute():
            return storage_path
        base_dir: Path
        data_dir: str | None = None
        try:
            data_dir = current_app.config.get("DATA_DIR")  # type: ignore[attr-defined]
        except Exception:
            data_dir = None
        if data_dir:
            base_dir = Path(data_dir)
            if not base_dir.is_absolute():
                base_dir = self._resolve_project_root() / base_dir
        else:
            base_dir = self._resolve_project_root()
        return (base_dir / storage_path).resolve()

    def _resolve_project_root(self) -> Path:
        try:
            return Path(current_app.root_path).parent  # type: ignore[attr-defined]
        except Exception:
            return Path(__file__).resolve().parents[2]


def get_rag_vector_service() -> RAGVectorService:
    return RAGVectorService()
