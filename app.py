from services.google_drive_service import GoogleDriveService
from services.document_service import DocumentService
from services.supabase_service import SupabaseService
from services.mysql_service import MySQLService
from services.embedding_service import EmbeddingService
from services.pinecone_service import PineconeService
from services.cleanup_service import CleanupService
from models.database import KBDocumentChunk
from utils.logger import logger
from dotenv import load_dotenv

class App:
    def __init__(self):
        self.google_drive_service = GoogleDriveService(
            download_dir="documents"
        )
        self.mysql_service = MySQLService()
        self.document_service = DocumentService(mysql_service=self.mysql_service)
        self.supabase_service = SupabaseService()
        self.embedding_service = EmbeddingService()
        self.pinecone_service = PineconeService()

        self.requested_files = []

    def run(self):
        try:
            # ----------------ONLY CHOOSE ONE OPERATION----------------

            # --------------------------------CLEANUP----------------------------------------
            # Set all to True to remove all records stored in the DB and Pinecone that is in the Google Drive folder
            # self.requested_files = self.google_drive_service.fetch_files(all=True)
            # self.cleanup()

            # --------------------------------SYNC----------------------------------------
            # Sync
            self.sync(all=True)  # Sync all -> change to False to sync only specific files you set in the self.requested_files list
        except Exception as e:
            logger.exception(f"App run failed: {e}")

    def sync(self, all=False):
        # Pull documents, mapped their IDs in the DB, update and chunk them
        if not all:
            filenames = self.google_drive_service.fetch_files(titles=self.requested_files)
        else:
            filenames = self.google_drive_service.fetch_files(all=all)

        if not filenames:
            logger.warning('No files downloaded. Skipping process.')
            return
        
        mapped_docs, chunked_docs = self.document_service.process(documents=filenames)
        inserted_chunks = self.mysql_service.bulk_insert_chunks(chunked_docs)

        # # Generate embeddings
        embedded_chunks = self.embedding_service.generate_embeddings(inserted_chunks)
        total_upserted = self.pinecone_service.sync(embedded_chunks)
        logger.info(f"Sync Complete.")

    def cleanup(self):
        cleanup = CleanupService(
            supabase_client=self.supabase_service.client,
            pinecone_index=self.pinecone_service.index,
            requested_files=self.requested_files
        )
        cleanup.run()

if __name__ == "__main__":
    load_dotenv()
    app = App()
    app.run()