from app.config.loader import OAUTH_AUTH_URI, OAUTH_CERT_URL, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_PROJECT_ID, OAUTH_REDIRECT_URI, OAUTH_TOKEN_URI, WB_DRIVE, WB_DAILY, WB_WEEKLY, WB_MONTHLY
import os
import pickle
from datetime import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from app.debug.line import debug_line, debug_title
from app.automations.log.state import log  # ✅ import from new file


class googledrive:
    def __init__(self, drive_url, daily, weekly, monthly, startDate, endDate, time_grain, brand):
        self.drive_url = drive_url
        self.daily = daily
        self.weekly = weekly
        self.monthly = monthly
        self.startDate = startDate
        self.endDate = endDate
        self.time_grain = time_grain
        self.brand = brand
        self.SCOPES = ["https://www.googleapis.com/auth/drive"]

        self.client_config = {
            "installed": {
                "client_id": OAUTH_CLIENT_ID,
                "project_id": OAUTH_PROJECT_ID,
                "auth_uri": OAUTH_AUTH_URI,
                "token_uri": OAUTH_TOKEN_URI,
                "auth_provider_x509_cert_url": OAUTH_CERT_URL,
                "client_secret": OAUTH_CLIENT_SECRET,
                "redirect_uris": [OAUTH_REDIRECT_URI],
            }
        }

        # ✅ Fixed folder path for credentials & token
        self.cred_dir = os.path.join("app", "api", "business_process")
        self.token_path = os.path.join(self.cred_dir, "token.pickle")

        # ✅ Ensure folder exists
        os.makedirs(self.cred_dir, exist_ok=True)

        # ✅ Load OAuth2 credentials (user account, not service account)
        self.creds = None
        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                self.creds = pickle.load(token)

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(
                    self.client_config, self.SCOPES
                )
                self.creds = flow.run_local_server(port=0)
            # Save credentials for next run
            with open(self.token_path, "wb") as token:
                pickle.dump(self.creds, token)

        self.service = build("drive", "v3", credentials=self.creds)
    
    def get_subfolder_id(self, job_id, parent_id, folder_name):
        """Find subfolder ID by name under parent (no creation)."""
        query = (
            f"'{parent_id}' in parents and "
            f'name="{folder_name}" and '
            "mimeType='application/vnd.google-apps.folder' and trashed=false"
        )

        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get("files", [])
        if items:
            return items[0]["id"]
            
        # ✅ Create folder if not found
        log(job_id, f"No Folder Found Creating New Folder for {folder_name}")
        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        new_folder = self.service.files().create(body=folder_metadata, fields="id, name").execute()

        log(None, f"📁 Created new folder: {new_folder['name']} ({new_folder['id']})")
        return new_folder["id"]


    def get_destination_folder(self, job_id, root_id, timegrain, date_start):
        """Navigate ROOT → timegrain → YYYY → MM-YYYY"""
        # Step 1: Daily/Weekly/Monthly
        log(job_id, f"Navigating on {timegrain} folder")
        grain_id = self.get_subfolder_id(job_id, root_id, timegrain)

        # Step 2: Year
        year_name = date_start.strftime("%Y")   # e.g. "2025"
        log(job_id, f"Navigating on {year_name} folder")
        year_id = self.get_subfolder_id(job_id, grain_id, year_name)

        # Step 3: Month-Year
        if self.time_grain.lower() in ["month", "monthly"]:
            return year_id
        
        month_year_name = date_start.strftime("%m'%y")  # e.g. "09-2025"
        log(job_id, f"Navigating on {month_year_name} folder")
        dest_id = self.get_subfolder_id(job_id, year_id, month_year_name)

        return dest_id

    def process(self, job_id):
        log(job_id, "Google Drive API's")
        date_start = datetime.strptime(self.startDate, "%d-%m-%Y")
        date_end = datetime.strptime(self.endDate, "%d-%m-%Y")
        startDate = date_start.strftime("%Y%m%d")
        endDate = date_end.strftime("%Y%m%d")
        monthDate = date_start.strftime("%m%y")
        # ROOT_FOLDER_ID = "1c9QnT9lRCqbn0D960E357Ypa3ezMxi0f"

        # ✅ Map self.time_grain → folder name
        if self.time_grain.lower() in ["day", "daily"]:
            folder_name = "Daily"
            SOURCE_FILE_ID = self.daily
            file_name = f"{self.brand} - {startDate} {folder_name} Business Performance Template"

        elif self.time_grain.lower() in ["week", "weekly"]:
            folder_name = "Weekly"
            SOURCE_FILE_ID = self.weekly
            file_name = f"{self.brand} - {startDate} {folder_name} Business Performance Template"

        elif self.time_grain.lower() in ["month", "monthly"]:
            folder_name = "Monthly"
            SOURCE_FILE_ID = self.monthly
            file_name = f"{self.brand} - {monthDate} {folder_name} Business Performance Template"

        else:
            folder_name = "Misc"

        log(job_id, "Getting the Destination Folder")
        DEST_FOLDER_ID = self.get_destination_folder(job_id, self.drive_url, folder_name, date_start)

        
        # DEST_FOLDER_ID = "1o0KajmupdO_BpKexgXoibn3OBXnRVT-V"

    
        copy_metadata = {
            "name": file_name,
            "parents": [DEST_FOLDER_ID],  # folder ID only
        }
        


        try:
            new_file = self.service.files().copy(
                fileId=SOURCE_FILE_ID,
                body=copy_metadata,
                fields="id, name, parents, owners"
            ).execute()

            log(job_id, f"✅ Google Drive Copied Sheet Successfully")
            log(job_id, f"📄 File: {new_file['name']} ({new_file['id']})")
            log(job_id, f"📂 Parents: {new_file.get('parents')}")
            log(job_id, f"👤 Owners: {[o['emailAddress'] for o in new_file.get('owners', [])]}")

            data = {
                "status": 200,
                "url": new_file['id']
            }
            return data

        except HttpError as error:
            log(job_id, f"❌ Drive API Error: {error}")
