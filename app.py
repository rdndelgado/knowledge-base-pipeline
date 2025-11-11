from services.google_drive_service import GoogleDriveService
from services.document_service import DocumentService
from services.supabase_service import SupabaseService
from services.embedding_service import EmbeddingService
from services.pinecone_service import PineconeService
from services.cleanup_service import CleanupService
from utils.logger import Logger

class App:
    def __init__(self):
        
        self.logger = Logger
        self.google_drive_service = GoogleDriveService(
            download_dir="documents"
        )
        self.document_service = DocumentService()
        self.supabase_service = SupabaseService()
        self.embedding_service = EmbeddingService()
        self.pinecone_service = PineconeService()

        self.requested_files = [
            "Test"
        ]

        self.files_to_delete = []

    def run(self):
        self.cleanup() # delete specific records from supabase and pinecone
    
    def sync(self):
        # Pull documents, mapped their IDs in the DB, update and chunk them
        filenames = self.google_drive_service.fetch_files(all=False, titles=self.requested_files) # Specific documents
        # filenames = self.google_drive_service.fetch_files(all=True) # All doc files in gdrive folder

        if not filenames:
            self.logger.warning('No files downloaded. Skipping process.')
            return
        mapped_docs, chunked_docs = self.document_service.process(documents=filenames)

        # Update to database
        synced_documents = self.supabase_service.sync_documents(mapped_docs) # Sync documents to Supabase
        synced_chunks = self.supabase_service.sync_document_chunks(chunked_docs) # Sync document chunks to Supabase

        # Generate embeddings
        embedded_chunks = self.embedding_service.generate_embeddings(synced_chunks)
        total_upserted = self.pinecone_service.sync(embedded_chunks)
        self.logger.info(f"Sync Complete. {total_upserted} chunks updated.")

    def cleanup(self):
        cleanup = CleanupService(
            supabase_client=self.supabase_service.client,
            pinecone_index=self.pinecone_service.index,
            requested_files=self.requested_files
        )
        cleanup.run()

if __name__ == "__main__":
    app = App()
    app.run()