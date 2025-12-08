import os
import pandas as pd
from datetime import datetime
from docx import Document
from utils.logger import Logger
from services.supabase_service import SupabaseService
from utils.tokens import count_tokens
from typing import List, Dict, Tuple
from nltk.tokenize import sent_tokenize

class DocumentService:
    
    def __init__(self, docs_dir="documents", error_log_path="logs/errors.csv"):
        self.docs_dir = docs_dir
        self.error_log_path = error_log_path
        self.logger = Logger
        self.supabase_service = SupabaseService()
        os.makedirs(os.path.dirname(error_log_path), exist_ok=True)

        # Chunking parameters
        self.CHUNK_MIN_SIZE = 500
        self.CHUNK_MAX_SIZE = 800
        self.CHUNK_OVERLAP = 80
    
    def process(self, documents: List[str] = None) -> Tuple[List[Dict], List[Dict]]:
        """
            Map .docx files to existing database records and update their content.
            Generate chunks for the documents.
            Returns a list of records that were successfully mapped with chunks.
        """
        mapped_docs = self._map_documents(documents=documents)
        chunked_docs = self._generate_chunks(mapped_docs)
        
        return mapped_docs, chunked_docs

    def _map_documents(self, documents: List[str] = None) -> List[Dict]:
        """
        Map .docx files to existing database records and update their content.
        Returns a list of records that were successfully updated.
        
        Parameters:
            documents (List[str], optional): List of filenames to process.
                If None, all .docx files in the directory are considered.
        """
        
        total_docs = 0
        mapped_documents = []

        # List all .docx files in the directory
        all_files = [f for f in os.listdir(self.docs_dir) if f.endswith(".docx")]

        # Filter files if documents parameter is provided
        files_to_process = [f for f in all_files if not documents or f in documents]

        if not files_to_process:
            self.logger.warning("[Document Service] No documents to process.")
            return []

        for filename in files_to_process:
            total_docs += 1
            title = os.path.splitext(filename)[0]
            file_path = os.path.join(self.docs_dir, filename)

            try:
                # Fetch existing document from the database
                document = self.supabase_service.get_document_by_title(title)

                # Read .docx content
                content = self._read_docx(file_path)
                if not content.strip():
                    self.logger.warning(f"[Document Service] Empty content for: {title}")
                    continue
                
                if not document:
                    self.logger.warning(f"[Document Service] Document not found in the database: {title}")
                    payload = {
                        "title": title,
                        "content": content
                    }
                    # if document is new, insert one.
                    data = self.supabase_service._upsert_document(payload)
                    if data:
                        mapped_documents.append(data)
                    continue

                # Update the content in the document
                document["content"] = content
                mapped_documents.append(document)
            except Exception as e:
                self._log_error(title, str(e))
                self.logger.error(f"[Document Service] Error while mapping document {title}: {e}")
        
        self.logger.info(f"[Document Service] Mapped {len(mapped_documents)}/{total_docs} documents.")
        return mapped_documents

    def _chunk_text(self, text: str):
        """
        Split text into chunks with overlap and sentence boundaries.
        """
        sentences = sent_tokenize(text)
        chunks = []
        current_chunk = []
        current_tokens = 0

        for sentence in sentences:
            sentence_tokens = count_tokens(sentence)

            # If adding this sentence would exceed max tokens → start new chunk
            if current_tokens + sentence_tokens > self.CHUNK_MAX_SIZE:
                if current_chunk:
                    chunks.append(" ".join(current_chunk))
                
                # Start overlap
                overlap_tokens = 0
                overlap_chunk = []
                while current_chunk and overlap_tokens < self.CHUNK_OVERLAP:
                    sent = current_chunk.pop()
                    overlap_chunk.insert(0, sent)
                    overlap_tokens += count_tokens(sent)

                current_chunk = overlap_chunk + [sentence]
                current_tokens = overlap_tokens + sentence_tokens
            else:
                current_chunk.append(sentence)
                current_tokens += sentence_tokens

        # Add last chunk
        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

    def _generate_chunks(self, documents: List[Dict]):
        """Generate chunks for each document based on chunk size."""
        chunked_documents = []

        for _, doc in enumerate(documents):
            doc_id = doc["id"]
            content = str(doc["content"])
            title = doc.get("title", "Untitled")

            document_chunks = []

            chunks = self._chunk_text(content)
            for idx, chunk in enumerate(chunks):
                token_count = count_tokens(chunk)
                document_chunks.append({
                    "chunk_index": idx,
                    "content": chunk,
                    "token_count": token_count
                })
            chunked_documents.append({"document_id": doc_id, "chunks": document_chunks})
            self.logger.info(f"Generated {len(document_chunks)} chunks for document: {title}")
        return chunked_documents
    
    def _read_docx(self, file_path):
        """Read .docx file and return plain text."""
        try:
            doc = Document(file_path)
            return "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        except Exception as e:
            self.logger.error(f"Failed to read {file_path}.")
            self._log_error(file_path, str(e))
            raise

    def _log_error(self, filename, error_message):
        """Append error details to error log CSV with timestamp."""
        try:
            timestamp = datetime.now().isoformat()
            df = pd.DataFrame([
                {
                    "timestamp": timestamp,
                    "filename": filename,
                    "error": error_message
                }
            ])

            if os.path.exists(self.error_log_path):
                df.to_csv(self.error_log_path, mode="a", index=False, header=False)
            else:
                df.to_csv(self.error_log_path, index=False)
        except Exception as e:
            self.logger.error(f"[Document Service] Failed to write to error log: {e}")
            raise