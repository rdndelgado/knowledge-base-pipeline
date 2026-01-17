from utils.logger import logger
from openai import OpenAI
from typing import List, Dict, Any
from collections import defaultdict
import os

class EmbeddingService:
    def __init__(self):
        self.embedding_model = os.getenv("EMBEDDING_MODEL", "")
        self.api_key = os.getenv("OPENAI_API_KEY", "")

        self.client = OpenAI(api_key=self.api_key)

    def generate_embeddings(self, document_chunks: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate embeddings for a list of document chunks, grouped by document_id.
        
        Returns:
            dict: {document_id: [chunk_dicts_with_embeddings]}
        """
        grouped_embeddings = defaultdict(list)

        for chunk in document_chunks:
            doc_id = str(chunk.get("document_id"))
            try:
                response = self.client.embeddings.create(
                    model=self.embedding_model,
                    input=chunk["content"]
                )
                embedding = response.data[0].embedding
                chunk['embedding'] = embedding
                grouped_embeddings[doc_id].append(chunk)
            except Exception as e:
                logger.error(f"[Embeddings] Failed to generate embedding for [doc: {doc_id} | chunk: {chunk.get('id')}]: {e}")
                continue

        logger.info(f"[Embeddings] Generated embeddings for {len(grouped_embeddings)} documents.")
        return dict(grouped_embeddings)