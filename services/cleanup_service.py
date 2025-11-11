from typing import List
from utils.logger import Logger

class CleanupService:
    def __init__(self, supabase_client, pinecone_index, requested_files: List[str]):
        self.supabase = supabase_client
        self.index = pinecone_index
        self.requested_files = requested_files
        self.logger = Logger

    def get_document_ids(self) -> List[str]:
        """Fetch existing document IDs from Supabase for the given filenames."""
        try:
            response = (
                self.supabase.table("documents")
                .select("id, title")
                .in_("title", self.requested_files)
                .execute()
            )
            if not response.data:
                self.logger.warning("[Cleanup] No matching documents found.")
                return []
            ids = [doc["id"] for doc in response.data]
            self.logger.info(f"[Cleanup] Found {len(ids)} document(s) to delete.")
            return ids
        except Exception as e:
            self.logger.error(f"[Cleanup] Failed to fetch documents: {e}")
            return []

    def delete_documents_in_supabase(self, document_ids: List[str]) -> None:
        """Delete documents from the Supabase 'documents' table."""
        for doc_id in document_ids:
            try:
                self.supabase.table("documents").delete().eq("id", doc_id).execute()
                self.logger.success(f"[Cleanup] Deleted document {doc_id} from Supabase.")
            except Exception as e:
                self.logger.error(f"[Cleanup] Failed to delete document {doc_id}: {e}")

    def delete_chunks_in_pinecone(self, document_ids: List[str]) -> None:
        """Delete all Pinecone chunks with matching document_id metadata."""
        for doc_id in document_ids:
            try:
                self.index.delete(filter={"document_id": {"$eq": doc_id}})
                self.logger.success(f"[Cleanup] Deleted Pinecone chunks for document {doc_id}.")
            except Exception as e:
                self.logger.error(f"[Cleanup] Failed to delete Pinecone chunks for {doc_id}: {e}")

    def run(self):
        """Run cleanup in modular order — Supabase first, then Pinecone."""
        document_ids = self.get_document_ids()
        if not document_ids:
            return
        self.delete_documents_in_supabase(document_ids)
        self.delete_chunks_in_pinecone(document_ids)
        self.logger.info("[Cleanup] Document removal completed.")