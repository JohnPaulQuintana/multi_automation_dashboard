from app.config.loader import (
    TYPE, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_EMAIL, CLIENT_ID,
    AUTH_URI, TOKEN_URI, AUTH_PROVIDER_X509_CERT_URL, CLIENT_X509_CERT_URL, UNIVERSE_DOMAIN
)
from app.constant.winbdt import WINBDT_RANGE 
from app.config.loader import NSU_FTD_TRACKER_SHEET
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from app.automations.log.state import log
from typing import List, Any
from datetime import datetime, timedelta
import time
import os
import json
import re
from collections import defaultdict
from datetime import datetime

class transferData:
    def __init__(self, fetch_data, sheet_url, copy_url, startDate):
        self.fetch_data = fetch_data["data"]
        self.sheet_url = sheet_url
        self.copy_url = copy_url["url"]
        self.startDate = startDate
        
        self.scope = ["https://www.googleapis.com/auth/spreadsheets"]
        config_dict = {
            "type": TYPE,
            "project_id": PROJECT_ID,
            "private_key_id": PRIVATE_KEY_ID,
            "private_key": PRIVATE_KEY,
            "client_email": CLIENT_EMAIL,
            "client_id": CLIENT_ID,
            "auth_uri": AUTH_URI,
            "token_uri": TOKEN_URI,
            "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": CLIENT_X509_CERT_URL,
            "universe_domain": UNIVERSE_DOMAIN,
        }
        self.creds = Credentials.from_service_account_info(config_dict, scopes=self.scope)
        self.service = build("sheets", "v4", credentials=self.creds)

    def get_sheet_id(self, sheet_name: str) -> int:
        """Fetch the numeric sheetId given a sheet/tab name"""
        meta = self.service.spreadsheets().get(
            spreadsheetId=self.sheet_url
        ).execute()

        for s in meta["sheets"]:
            if s["properties"]["title"] == sheet_name:
                return s["properties"]["sheetId"]
        raise ValueError(f"Sheet '{sheet_name}' not found")

    def insert_column_header(self, sheet_id, date, weekday_name):
        """Insert a new column at E, copy format from D, and set date header (with optional hyperlink)"""
        requests = []
        full_url = f"https://docs.google.com/spreadsheets/d/{self.sheet_url}/edit"
        # Insert new column at index 4 (E column)
        requests.append({
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 4,
                    "endIndex": 5
                },
                "inheritFromBefore": False
            }
        })

        # Copy format from column D → column E
        requests.append({
            "copyPaste": {
                "source": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,
                    "startColumnIndex": 5,
                    "endColumnIndex": 6
                },
                "destination": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,
                    "startColumnIndex": 4,
                    "endColumnIndex": 5
                },
                "pasteType": "PASTE_FORMAT",
                "pasteOrientation": "NORMAL"
            }
        })

        # If url is provided, use HYPERLINK formula
        header_value = {
            "userEnteredValue": {
                "formulaValue": f'=HYPERLINK("{full_url}","{date}")'
            }
        }


        # Set header value in row 1, column E
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 4,
                    "endColumnIndex": 5
                },
                "rows": [{"values": [header_value]}],
                "fields": "userEnteredValue"
            }
        })

        # Row 2 → weekday name
        weekday_value = {
            "userEnteredValue": {
                "stringValue": weekday_name
            }
        }
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,   # row 2
                    "endRowIndex": 2,
                    "startColumnIndex": 4,
                    "endColumnIndex": 5
                },
                "rows": [{"values": [weekday_value]}],
                "fields": "userEnteredValue"
            }
        })

        # Execute requests
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_url,
            body={"requests": requests}
        ).execute()

    def insert_data(self, sheet_id, values):
        """
        Insert fetch_data starting from row 3 (index 2 in API),
        into the newly inserted column E.
        """
        requests = []
        clean_values = []
        for v in values:
            if isinstance(v, list):          # e.g. ['1']
                if v:                        # non-empty
                    clean_values.append(str(v[0]))
                else:
                    clean_values.append("")  # empty cell
            else:
                clean_values.append(str(v))
        # Convert fetch_data into row values
        # Assuming fetch_data is a list of values (one per row)
        rows = [{"values": [{"userEnteredValue": {"stringValue": str(v)}}]} for v in clean_values]

        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 2,   # Row 3 in Google Sheets
                    "startColumnIndex": 4, # Column E
                    "endColumnIndex": 5
                },
                "rows": rows,
                "fields": "userEnteredValue"
            }
        })

        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_url,
            body={"requests": requests}
        ).execute()


    def transfer_data(self, job_id):
        log(job_id, f"Transferring into Live Docs....")
        date = datetime.strptime(self.startDate, "%d-%m-%Y")
        format_date = date.strftime("%Y%m%d")
        weekday_name = date.strftime("%A")  

        # self.fetch_data

        sheet_id = self.get_sheet_id("Daily")


        self.insert_column_header(sheet_id, format_date, weekday_name)

        self.insert_data(sheet_id, self.fetch_data)


        log(job_id, f"Full URL: https://docs.google.com/spreadsheets/d/{self.sheet_url}/edit ")

