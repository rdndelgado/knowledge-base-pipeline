import os
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from utils.logger import Logger
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

class GoogleDriveService:
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    def __init__(self, download_dir="documents"):
        self.logger = Logger
        self.credentials_json = os.getenv("CREDENTIALS_JSON_FILE")
        self.folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        self.download_dir = download_dir
        self.drive = None

        if not self.credentials_json or not os.path.exists(self.credentials_json):
            raise ValueError("Missing or invalid credentials JSON file.")
        if not self.folder_id:
            raise ValueError("Missing folder ID.")

        os.makedirs(self.download_dir, exist_ok=True)

    def authenticate(self):
        """Authenticate using a service account (non-interactive)."""
        creds = service_account.Credentials.from_service_account_file(
            self.credentials_json,
            scopes=self.SCOPES
        )
        self.drive = build("drive", "v3", credentials=creds)
        self.logger.success("✅ Authenticated using service account.")

    # List all files in the google drive folder
    def list_drive_folder_doc_files(self):
        """List all Google Docs and .docx files inside the specified Drive folder."""
        if not self.drive:
            raise RuntimeError("Drive client not initialized. Call authenticate() first.")

        # Include both Google Docs and Word files
        query = (
            f"'{self.folder_id}' in parents and "
            f"(mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document' "
            f"or mimeType='application/vnd.google-apps.document')"
        )

        results = self.drive.files().list(
            q=query,
            fields="files(id, name, mimeType)"
        ).execute()

        return results.get("files", [])

    # Select files to process
    def filter_files(self, files, titles=None, all=True):
        """Return all files if all=True, else filter by a list of titles."""
        if all:
            return files
        if not titles:
            self.logger.warning("No file titles provided; skipping download.")
            return []
        return [f for f in files if f["name"] in titles]

    # Download files from Google Drive
    def download_files(self, files, requested_titles=None, all_files=True) -> List[str]:
        """Download all Google Docs and .docx files to the local directory."""
        if not self.drive:
            raise RuntimeError("Drive client not initialized. Call authenticate() first.")

        if not files:
            self.logger.warning("No matching files to download.")
            return []

        os.makedirs(self.download_dir, exist_ok=True)
        downloaded_files = []
        downloaded_count = 0

        for f in files:
            file_id = f["id"]
            file_name = f["name"]
            mime_type = f.get("mimeType", "")

            # Always save as .docx for consistency
            safe_name = f"{os.path.splitext(file_name)[0]}.docx"
            file_path = os.path.join(self.download_dir, safe_name)

            try:
                # --- Handle Google Docs export ---
                if mime_type == "application/vnd.google-apps.document":
                    request = self.drive.files().export(
                        fileId=file_id,
                        mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )
                else:
                    # --- Handle already uploaded .docx files ---
                    request = self.drive.files().get_media(fileId=file_id)

                # Download to local file
                with io.FileIO(file_path, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            self.logger.info(f"Downloading {file_name}: {int(status.progress() * 100)}%")

                downloaded_count += 1
                downloaded_files.append(safe_name)
                self.logger.success(f"Downloaded: {safe_name}")

            except Exception as e:
                self.logger.error(f"Error downloading {file_name}: {e}")
                continue

        # --- Summary logging ---
        if all_files:
            self.logger.info(f"Total downloaded: {downloaded_count}")
        else:
            total_requested = len(requested_titles) if requested_titles else 0
            self.logger.success(f"Total downloaded: {downloaded_count}/{total_requested}")

        return downloaded_files


    # Fetch files from Google Drive
    def fetch_files(self, all=True, titles=None) -> List[str]:
        """Authenticate and fetch files — all or filtered by name. Returns list of downloaded filenames."""
        self.authenticate()
        files = self.list_drive_folder_doc_files()
        selected_files = self.filter_files(files, titles=titles, all=all)
        downloaded_files = self.download_files(selected_files, requested_titles=titles, all_files=all)
        return downloaded_files