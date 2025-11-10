import os
from supabase import create_client, Client
from core.config import Logger
from typing import List, Dict

class SupabaseService:
    
    def __init__(self):

        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.logger = Logger
        self.client: Client = create_client(self.url, self.key)
        self.logger.success("Supabase initialized successfully.")

        self.max_retries = 3

    def sync_documents(self, documents: List[Dict]) -> List[Dict]:
        """
        Upsert multiple document records to the 'documents' table.
        """
        synced_documents = []

        for document in documents:
            data = self._upsert_document(document)
            if data:
                synced_documents.append(data)
        
        return synced_documents
       
    def _upsert_document(self, document: dict):
        """
        Insert or update a document record in the 'documents' table.
        If a document with the same title exists, it will be updated.
        If 'id' is not provided, a new record will be inserted with an auto-generated ID.
        """
        for attempt in range(self.max_retries):
            try:
                # Only include 'id' if it exists
                data = {
                    "title": document["title"],
                    "content": document["content"]
                }
                if document.get("id") is not None:
                    data["id"] = document["id"]

                response = (
                    self.client.table("documents")
                    .upsert(data, on_conflict="id")
                    .execute()
                )

                if response.data and response.data[0]:
                    self.logger.success(f"[Supabase] Upserted document: {document['title']}")
                    return response.data[0]

            except Exception as e:
                self.logger.error(f"[Supabase] Retrying {attempt + 1}/{self.max_retries} - Error upserting document '{document['title']}': {e}")
                continue
    
    def sync_document_chunks(self, document_chunks: List[Dict]) -> List[Dict]:
        """
        Sync document chunks with Supabase.
        Updates, inserts, or deletes chunks to match the latest document chunk list.
        """
        synced_chunks = []

        for doc in document_chunks:
            doc_id = doc.get("document_id")
            new_chunks = doc.get("chunks", [])

            existing_chunks = self._fetch_document_chunks(doc_id)
            existing_count = len(existing_chunks)
            new_count = len(new_chunks)

            self.logger.info(f"[Supabase] Syncing document ID {doc_id}: {existing_count} → {new_count} chunks")

            # Map old IDs to new chunks where possible
            for i in range(min(existing_count, new_count)):
                new_chunks[i]["id"] = existing_chunks[i].get("id")

            # Delete extra chunks if new list is shorter
            if new_count < existing_count:
                extra_ids = [chunk["id"] for chunk in existing_chunks[new_count:] if "id" in chunk]
                if extra_ids:
                    try:
                        self.client.table("document_chunks").delete().in_("id", extra_ids).execute()
                        self.logger.warning(f"Deleted {len(extra_ids)} outdated chunks for document ID: {doc_id}")
                    except Exception as e:
                        self.logger.error(f"Failed to delete old chunks for {doc_id}: {e}")

            # Upsert all new/updated chunks
            doc_chunks = self._upsert_document_chunks(doc)
            synced_chunks.extend(doc_chunks)

        return synced_chunks

    def _upsert_document_chunks(self, document_chunks: Dict) -> List[Dict]:
        """
        Upsert all chunks for a given document.
        If a chunk has no 'id', it will be inserted with an auto-generated ID.
        """
        doc_id = document_chunks.get("document_id")
        chunks = document_chunks.get("chunks", [])
        doc_chunks = []

        for chunk in chunks:
            try:
                # Build payload
                data = {
                    "document_id": doc_id,
                    "chunk_index": chunk["chunk_index"],
                    "content": chunk["content"],
                    "token_count": chunk.get("token_count", 0)
                }
                # Include 'id' only if it exists
                if chunk.get("id") is not None:
                    data["id"] = chunk["id"]

                response = (
                    self.client.table("document_chunks")
                    .upsert(data, on_conflict="id")
                    .execute()
                )

                if response.data and response.data[0]:
                    doc_chunks.append(response.data[0])
                    self.logger.info(f"[Supabase] Upserted chunk {chunk['chunk_index']}.")

            except Exception as e:
                self.logger.warning(
                    f"[Supabase] Failed to upsert chunk {chunk.get('chunk_index')} → {e}"
                )
                continue

        return doc_chunks

    def get_document_by_title(self, filename: str):
        """
            Query the 'documents' table by filename (title column).
            Returns the full record if found, else None.
        """
        try:
            response = (
                self.client.table("documents")
                .select("*")
                .eq("title", filename)
                .execute()
            )

            if response.data:
                return response.data[0]
            else:
                return None
        except Exception as e:
            self.logger.error(f"[Supabase] Error querying document '{filename}': {e}")
            return None

    def _fetch_document_chunks(self, document_id: int):
        """
        Fetch all chunks associated with a given document ID.
        Returns a list of chunk records.
        """
        for attempt in range(self.max_retries):
            try:
                response = (
                    self.client.table("document_chunks")
                    .select("*")
                    .eq("document_id", document_id)
                    .execute()
                )
                return response.data if response.data else []
            except Exception as e:
                self.logger.error(f"(Retrying {attempt}/{self.max_retries}) Error fetching chunks for document ID {document_id}: {e}")
                continue