from typing import List
from utils.logger import logger
import os

class CleanupService:
    
    def __init__(self, mysql_service, pinecone_index, requested_files: List[str]):
        self.mysql_service = mysql_service
        self.index = pinecone_index
        self.requested_files = requested_files

    def get_document_ids(self) -> List[str]:
        """Fetch existing document IDs from MySQL for the given filenames."""
        try:
            if not self.mysql_service:
                logger.error("[Cleanup] MySQL service is not initialized")
                return []
            
            # Extract titles from filenames (remove extension)
            titles = [os.path.splitext(file)[0] for file in self.requested_files]
            
            if not titles:
                logger.warning("[Cleanup] No files provided for cleanup")
                return []
            
            # Fetch document IDs for all titles at once
            document_ids = self.mysql_service.get_document_ids_by_titles(titles)
            
            if document_ids:
                logger.info(f"[Cleanup] Found {len(document_ids)} document(s) to delete.")
            else:
                logger.warning(f"[Cleanup] No matching documents found for the provided titles.")
            
            return document_ids
        except Exception as e:
            import traceback
            logger.error(f"[Cleanup] Error fetching document IDs: {e}")
            logger.error(f"[Cleanup] Traceback: {traceback.format_exc()}")
            return []

    def delete_documents_in_mysql(self, document_ids: List[str]) -> None:
        """Delete documents from MySQL. Chunks will be deleted automatically due to CASCADE."""
        if not document_ids:
            return
        
        try:
            self.mysql_service.delete_documents_by_ids(document_ids)
            logger.info(f"[Cleanup] Deleted {len(document_ids)} document(s) from MySQL.")
        except Exception as e:
            logger.error(f"[Cleanup] Failed to delete documents from MySQL: {e}")
            raise

    def delete_chunks_in_pinecone(self, document_ids: List[str]) -> None:
        """Delete all Pinecone chunks with matching document_id metadata."""
        if not document_ids:
            return
        
        for doc_id in document_ids:
            try:
                self.index.delete(filter={"document_id": {"$eq": doc_id}})
                logger.info(f"[Cleanup] Deleted Pinecone chunks for document {doc_id}.")
            except Exception as e:
                logger.error(f"[Cleanup] Failed to delete Pinecone chunks for {doc_id}: {e}")

    def run(self):
        """Run cleanup in modular order — MySQL first, then Pinecone."""
        document_ids = self.get_document_ids()
        if not document_ids:
            logger.warning("No documents to delete.")
            return
        
        self.delete_documents_in_mysql(document_ids)
        self.delete_chunks_in_pinecone(document_ids)
        logger.info("[Cleanup] Document removal completed.")