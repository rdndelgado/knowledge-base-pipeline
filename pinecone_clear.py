from pinecone import Pinecone
from utils.logger import logger
import os
from dotenv import load_dotenv

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


    def clear_index(self, namespace: str = "__default__") -> bool:
        """
        Clear all vectors from the Pinecone index.
        """
        try:
            try:
                self.index.delete(delete_all=True, namespace=namespace)
            except TypeError:
                # Backward compatibility
                self.index.delete(deleteAll=True, namespace=namespace)

            logger.info(
                f"[Pinecone] Cleared index '{self.index_name}' namespace '{namespace}'."
            )
            return True

        except Exception as e:
            logger.warning(f"[Pinecone] Failed to clear index: {e}")
            return False

if __name__ == "__main__":

    load_dotenv()
    
    pinecone_service = PineconeService()
    success = pinecone_service.clear_index(namespace="__default__")

    if success:
        logger.info("✅ Pinecone index cleared successfully.")
    else:
        logger.error("❌ Failed to clear Pinecone index.")