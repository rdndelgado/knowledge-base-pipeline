import os
from google.cloud.sql.connector import Connector
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError, InterfaceError
from utils.logger import logger
from models.database import KBDocument, KBDocumentChunk
from typing import Optional, Dict
import uuid

class MySQLService:
    
    def __init__(self):
        try:
            # Initialize Cloud SQL Connector
            self.connector = Connector()
            
            # Cloud SQL instance connection name
            self.instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
            self.db_name = os.getenv("MYSQL_DATABASE")
            self.mysql_pwd = os.getenv("MYSQL_PASSWORD")
            self.mysql_user = os.getenv("MYSQL_USER")
            self.document_table = os.getenv("DOCUMENT_TABLE")
            self.chunk_table = os.getenv("CHUNK_TABLE")
            self.max_retries = 3
            
            # Create connection pool with Cloud SQL Connector
            self.pool = PooledDB(
                creator=self._get_connection,
                mincached=2,        # Start with 2 open connections
                maxcached=10,       # Max idle connections allowed
                maxconnections=20,  # Max total connections allowed
                blocking=True,      # Wait if pool is exhausted
            )

            logger.info("MySQL connection pool started.")

            # Create tables once at startup
            self.create_tables()

        except Exception as e:
            logger.error(f"Critical Failure: Could not initialize DB pool: {e}")
            raise

    def _get_connection(self):
        """
        Creates a connection using Cloud SQL Connector.
        This method is called by the connection pool.
        """
        conn = self.connector.connect(
            self.instance_connection_name,
            "pymysql",
            user=self.mysql_user,
            password=self.mysql_pwd,
            db=self.db_name,
            cursorclass=DictCursor,
            autocommit=True
        )
        return conn

    def execute_query(self, query, params=None, fetch=False):
        """
        Executes SQL safely using the pool.
        Automatically returns connection to pool.
        """
        conn = self.pool.connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                return cursor.rowcount

        except (OperationalError, InterfaceError) as e:
            logger.error(f"Database error during query execution: {e}")
            raise

        finally:
            conn.close()

    # TODO: To remove
    def create_tables(self):
        queries = [
            f"""
            CREATE TABLE IF NOT EXISTS {self.document_table} (
            id CHAR(36) PRIMARY KEY DEFAULT (UUID()),
            title TEXT NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.chunk_table} (
            id CHAR(36) PRIMARY KEY DEFAULT (UUID()),
            document_id CHAR(36),
            chunk_index INT NOT NULL,
            content TEXT NOT NULL,
            token_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES {self.document_table}(id) ON DELETE CASCADE
            )
            """,
            # f"""
            # CREATE INDEX idx_kb_document_chunks_document_id
            # ON {self.chunk_table} (document_id, chunk_index)
            # """
        ]

        for q in queries:
            self.execute_query(q)

        logger.info("Knowledge base tables created.")

    def get_document_by_title(self, title: str):
        """Fetch a document by its title."""
        query = f"SELECT * FROM {self.document_table} WHERE title = %s"
        results = self.execute_query(query, (title,), fetch=True)
        return results[0] if results else None
    
    def get_chunks_by_document_id(self, document_id: str):
        """Fetch chunks by document ID."""
        query = f"SELECT * FROM {self.chunk_table} WHERE document_id = %s ORDER BY chunk_index ASC"
        results = self.execute_query(query, (document_id,), fetch=True)
        return results
    
    def insert_document(self, document: KBDocument) -> Optional[Dict]:
        """
        Insert a document with a UUID primary key and return the inserted record.
        
        Returns:
            dict: The inserted document record including its generated UUID
        """
        conn = self.pool.connection()
        try:
            conn.begin()
            with conn.cursor() as cursor:
                # Generate a UUID for the document
                document_id = str(uuid.uuid4())

                # Insert with the UUID
                cursor.execute(
                    f"INSERT INTO {self.document_table} (id, title, content) VALUES (%s, %s, %s)",
                    (document_id, document.title, document.content)
                )

                # Fetch the inserted document
                cursor.execute(
                    f"SELECT * FROM {self.document_table} WHERE id = %s",
                    (document_id,)
                )
                inserted_doc = cursor.fetchone()

            conn.commit()
            logger.info(f"[MySQL] Inserted document '{document.title}' with UUID {document_id}")
            return inserted_doc

        except Exception as e:
            conn.rollback()
            logger.error(f"[MySQL] Failed to insert document '{document.title}': {e}")
            raise

        finally:
            conn.close()

    def bulk_insert_chunks(self, chunks: list) -> list:

        conn = self.pool.connection()
        try:
            conn.begin()
            with conn.cursor() as cursor:
                insert_query = f"""
                    INSERT INTO {self.chunk_table}
                    (id, document_id, chunk_index, content, token_count)
                    VALUES (%s, %s, %s, %s, %s)
                """

                for chunk in chunks:
                    chunk_id = str(uuid.uuid4())
                    chunk["id"] = chunk_id

                    cursor.execute(
                        insert_query,
                        (
                            chunk_id,
                            chunk["document_id"],
                            chunk["chunk_index"],
                            chunk["content"],
                            chunk.get("token_count", 0),
                        )
                    )

                    logger.debug(f"Chunk UUID generated: {chunk_id}")

            conn.commit()
            logger.info(f"[MySQL] Bulk inserted {len(chunks)} chunks with UUIDs")
            return chunks

        except Exception as e:
            conn.rollback()
            logger.error(f"[MySQL] Bulk insert failed: {e}")
            raise

        finally:
            conn.close()
            
    def close(self):
        """
        Cleanup method to close the connector.
        Should be called when shutting down the application.
        """
        if hasattr(self, 'connector'):
            self.connector.close()
            logger.info("Cloud SQL Connector closed.")