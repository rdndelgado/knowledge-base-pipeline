from typing import List, Dict, Any
from pinecone import Pinecone
from utils.logger import logger
import requests
import uuid
import os

class PineconeService:
    """Service to interact with Pinecone index for document chunks."""

    def __init__(self):

        self.api_key = os.getenv("PINECONE_API_KEY", "")
        self.index_name = os.getenv("PINECONE_INDEX_NAME", "")

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

        logger.info(f"✅ Connected to Pinecone: {self.index_host} | Index: {self.index_name}")

    def sync(
        self, 
        embeddings: Dict[str, List[Dict[str, Any]]]
    ) -> int:
        """Sync embeddings to Pinecone."""
        total_upserted = 0

        for document_id, chunks in embeddings.items():
            logger.debug(f"Document ID: {document_id} | Chunks - {len(chunks)}")
            upserted = self.upsert_chunks(chunks, document_id)
            total_upserted += upserted
        return total_upserted
    
    def upsert_chunks(
        self,
        chunks: List[Dict[str, Any]],
        document_id: str
    ) -> int:
        vectors = []

        for chunk in chunks:
            try:
                if not chunk.get("id"):
                    raise ValueError("Missing chunk ID from database")

                embedding = chunk.get("embedding")
                if not embedding:
                    raise ValueError("Missing embedding")

                vectors.append({
                    "id": str(chunk["id"]),  # MySQL ID
                    "values": embedding,
                    "metadata": {
                        "document_id": str(document_id),
                        "chunk_index": int(chunk["chunk_index"]),
                        "token_count": int(chunk.get("token_count", 0)),
                    }
                })

            except Exception as e:
                logger.error(
                    f"[Pinecone] Skipping chunk "
                    f"[doc={document_id} | chunk={chunk.get('id')}]: {e}"
                )

        if not vectors:
            return 0

        self.index.upsert(vectors=vectors)
        logger.info(f"[Pinecone] Upserted {len(vectors)} vectors")

        return len(vectors)


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
                logger.warning("[Pinecone] Unexpected response format — expected dict of vectors.")
                return {}
            logger.info(f"[Pinecone] Retrieved {len(vectors)} existing chunk(s) for document {document_id}")
            return vectors
        except Exception as e:
            logger.error(f"[Pinecone] Error fetching chunks for {document_id}: {e}")
            return {}

    def clear_index(self, namespace: str = "__default__") -> bool:
        """
        Clear all vectors from the Pinecone index.

        Parameters:
            namespace: Namespace to clear (default: "__default__").

        Returns:
            True if the index was cleared successfully, False otherwise.
        """
        try:
            try:
                self.index.delete(delete_all=True, namespace=namespace)
            except TypeError:
                self.index.delete(deleteAll=True, namespace=namespace)
            logger.info(f"[Pinecone] Cleared index '{self.index_name}' namespace '{namespace}'.")
            return True
        except Exception as e:
            logger.warning(f"[Pinecone] Failed to clear index: {e}")