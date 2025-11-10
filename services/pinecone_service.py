from typing import List, Dict, Any
from pinecone import Pinecone
from core.config import Config, Logger
import requests
import uuid

class PineconeService:
    """Service to interact with Pinecone index for document chunks."""

    def __init__(self):
        self.logger = Logger

        self.api_key = Config.PINECONE_API_KEY
        self.index_name = Config.PINECONE_INDEX_NAME

        if not self.api_key:
            raise ValueError("Missing PINECONE_API_KEY environment variable.")
        if not self.index_name:
            raise ValueError("Missing PINECONE_INDEX_NAME environment variable.")

        self.pc = Pinecone(api_key=self.api_key)
        self.index = self.pc.Index(self.index_name)

        # API request, filter by metadata feature
        self.api_version = "2025-10"
        self.headers = {
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
            "X-Pinecone-API-Version": self.api_version,
        }
        index_info = self.pc.describe_index(self.index_name)
        self.index_host = index_info.get('host', None)

        self.logger.success(f"✅ Connected to Pinecone: {self.index_host} | Index: {self.index_name}")

    def sync(
        self, 
        embeddings: Dict[str, List[Dict[str, Any]]]
    ) -> int:
        """
        Sync embeddings to Pinecone.
        
        Parameters:
            embeddings: Dict mapping document_id → list of chunk dicts,
                each containing: id (optional), chunk_index, content, embedding, token_count.
        """
        total_upserted = 0

        for document_id, chunks in embeddings.items():
            if not chunks:
                self.logger.warning(f"[Pinecone] No chunks for document {document_id}")
                continue

            # Fetch existing chunks from Pinecone
            existing_chunks = self.get_chunks_by_document(document_id)
            existing_keys = list(existing_chunks.keys()) if isinstance(existing_chunks, dict) else []
            existing_count = len(existing_keys)
            new_count = len(chunks)

            self.logger.info(f"[Pinecone] Syncing document {document_id}: {existing_count} → {new_count} chunks")

            # Map existing IDs to new chunks if counts match or fewer
            for i in range(min(existing_count, new_count)):
                chunks[i]["id"] = existing_keys[i]

            # Delete extra chunks if new_count < existing_count
            if new_count < existing_count:
                extra_ids = existing_keys[new_count:]
                if extra_ids:
                    try:
                        self.index.delete(ids=extra_ids)
                        self.logger.warning(f"[Pinecone] Deleted {len(extra_ids)} extra chunks for document {document_id}")
                    except Exception as e:
                        self.logger.error(f"[Pinecone] Failed to delete extra chunks for {document_id}: {e}")
                        raise

            # Upsert all chunks for this document
            upserted = self.upsert_chunks(chunks, document_id)
            total_upserted += upserted
        return total_upserted
    
    def upsert_chunks(
        self, 
        chunks: List[Dict[str, Any]], 
        document_id: str
    ) -> int:
        """
            Upsert document chunks into Pinecone.
            If a chunk has no 'id', a UUID will be generated automatically.
        """
        vectors = []

        for chunk in chunks:
            try:
                # Use existing ID or generate a new UUID
                chunk_id = str(chunk.get("id") or uuid.uuid4())
                embedding = chunk["embedding"]
                metadata = {
                    "document_id": str(document_id),
                    "chunk_index": int(chunk["chunk_index"]),
                    "token_count": int(chunk.get("token_count", 0)),
                }

                vectors.append({
                    "id": chunk_id,
                    "values": embedding,
                    "metadata": metadata,
                })

            except KeyError as e:
                self.logger.error(f"[Pinecone] Missing key in chunk data: {e}")
                continue

        try:
            self.index.upsert(vectors)
            self.logger.success(f"[Pinecone] Upserted {len(vectors)} chunk(s).")
            return len(vectors)
        except Exception as e:
            self.logger.error(f"[Pinecone] Failed to upsert chunks: {e}")
            raise

    # Get existing chunks embedding
    def get_chunks_by_document(
        self, 
        document_id: str, 
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Fetch chunks from Pinecone by document_id metadata.
        Returns a dict where keys are chunk IDs.
        """
        url = f"https://{self.index_host}/vectors/fetch_by_metadata"
        payload = {
            "namespace": "__default__",
            "filter": {"document_id": {"$eq": document_id}},
            "limit": limit
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json()
            vectors = data.get("vectors", {})
            if not isinstance(vectors, dict):
                self.logger.warning("[Pinecone] Unexpected response format — expected dict of vectors.")
                return {}
            self.logger.info(f"[Pinecone] Retrieved {len(vectors)} existing chunk(s) for document {document_id}")
            return vectors
        except Exception as e:
            self.logger.error(f"[Pinecone] Error fetching chunks for {document_id}: {e}")
            return {}