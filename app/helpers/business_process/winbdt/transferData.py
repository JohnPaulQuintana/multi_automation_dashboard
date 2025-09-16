from app.config.loader import (
    TYPE, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_EMAIL, CLIENT_ID,
    AUTH_URI, TOKEN_URI, AUTH_PROVIDER_X509_CERT_URL, CLIENT_X509_CERT_URL, UNIVERSE_DOMAIN
)

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
    def __init__(self, fetch_data, sheet_url, copy_url, startDate, endDate, time_grain):
        self.fetch_data = fetch_data["data"]
        self.sheet_url = sheet_url
        self.copy_url = copy_url["url"]
        self.startDate = startDate
        self.endDate = endDate
        self.time_grain = time_grain
        
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

    def insert_column_header(self, sheet_id, date, end, timegrain):
        """Insert a new column at E, copy format from D, and set date header (with optional hyperlink)"""
        requests = []
        full_url = f"https://docs.google.com/spreadsheets/d/{self.copy_url}/edit"
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
        if timegrain == "Daily":
            format_date = date.strftime("%Y%m%d")
            weekday_name = date.strftime("%A")

            header_value = {
                "userEnteredValue": {
                    "formulaValue": f'=HYPERLINK("{full_url}","{format_date}")'
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

        elif timegrain == "Weekly":
            format_startDate = date.strftime("%Y%m%d")
            format_endDate = (end - timedelta(days=1)).strftime("%Y%m%d")  # subtract 1 day
            header_value = {
                "userEnteredValue": {
                    "formulaValue": f'=HYPERLINK("{full_url}","{format_startDate}-{format_endDate}")'
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
        elif timegrain == "Monthly":
            format_startDate = date.strftime("%m/%y")
            header_value = {
                "userEnteredValue": {
                    "formulaValue": f'=HYPERLINK("{full_url}","{format_startDate}")'
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


        # Execute requests
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_url,
            body={"requests": requests}
        ).execute()

    def insert_data(self, sheet_name, values):
        """Insert data starting from row 3 in column E, preserving formulas."""
        clean_values = []
        for v in values:
            if isinstance(v, list) and v:   # already a list with value
                clean_values.append([str(v[0])])
            elif isinstance(v, list):      # empty list
                clean_values.append([""])
            else:                          # scalar value
                clean_values.append([str(v)])

        body = {"values": clean_values}

        self.service.spreadsheets().values().update(
            spreadsheetId=self.sheet_url,
            range=f"'{sheet_name}'!E3",  # sheet_name, not sheet_id
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()



    def transfer_data(self, job_id):
        log(job_id, f"Transferring into Live Docs....")
        date = datetime.strptime(self.startDate, "%d-%m-%Y")
        end = datetime.strptime(self.endDate, "%d-%m-%Y")
        
        if self.time_grain.lower() in ["day", "daily"]:
            timegrain = "Daily"
        elif self.time_grain.lower() in ["week", "weekly"]:
            timegrain = "Weekly"
        elif self.time_grain.lower() in ["month", "monthly"]:
            timegrain = "Monthly"
        else:
            timegrain = "Daily"  

        # self.fetch_data

        sheet_id = self.get_sheet_id(timegrain)


        self.insert_column_header(sheet_id, date, end, timegrain)

        self.insert_data(timegrain, self.fetch_data)


        log(job_id, f"Full URL: https://docs.google.com/spreadsheets/d/{self.sheet_url}/edit ")

