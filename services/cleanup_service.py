from typing import List
from utils.logger import logger
import os

class CleanupService:
    
    def __init__(self, supabase_client, pinecone_index, requested_files: List[str]):
        self.supabase = supabase_client
        self.index = pinecone_index
        self.requested_files = requested_files

    def get_document_ids(self) -> List[str]:
        """Fetch existing document IDs from Supabase for the given filenames."""
        all_ids = []
        try:
            for file in self.requested_files:
                title = os.path.splitext(file)[0]
                response = (
                    self.supabase.table("documents")
                    .select("id, title")
                    .eq("title", title)  # exact match by title
                    .execute()
                )
                if not response.data:
                    logger.warning(f"[Cleanup] No matching documents found for '{title}'.")
                    continue  # skip to next file

                ids = [doc["id"] for doc in response.data]
                all_ids.extend(ids)
                logger.info(f"[Cleanup] Found {len(ids)} document(s) for '{title}'.")

            return all_ids
        except Exception as e:
            logger.error(f"[Cleanup] Error fetching document IDs: {e}")
            return []

    def delete_documents_in_supabase(self, document_ids: List[str]) -> None:
        """Delete documents from the Supabase 'documents' table."""
        for doc_id in document_ids:
            try:
                self.supabase.table("documents").delete().eq("id", doc_id).execute()
                logger.info(f"[Cleanup] Deleted document {doc_id} from Supabase.")
            except Exception as e:
                logger.error(f"[Cleanup] Failed to delete document {doc_id}: {e}")

    def delete_chunks_in_pinecone(self, document_ids: List[str]) -> None:
        """Delete all Pinecone chunks with matching document_id metadata."""
        for doc_id in document_ids:
            try:
                self.index.delete(filter={"document_id": {"$eq": doc_id}})
                logger.info(f"[Cleanup] Deleted Pinecone chunks for document {doc_id}.")
            except Exception as e:
                logger.error(f"[Cleanup] Failed to delete Pinecone chunks for {doc_id}: {e}")

    def run(self):
        """Run cleanup in modular order — Supabase first, then Pinecone."""
        document_ids = self.get_document_ids()
        if not document_ids:
            logger.warning("No documents to delete.")
            return
        self.delete_documents_in_supabase(document_ids)
        self.delete_chunks_in_pinecone(document_ids)
        logger.info("[Cleanup] Document removal completed.")