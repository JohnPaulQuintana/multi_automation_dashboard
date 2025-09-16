from app.config.loader import (
    TYPE, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_EMAIL, CLIENT_ID,
    AUTH_URI, TOKEN_URI, AUTH_PROVIDER_X509_CERT_URL, CLIENT_X509_CERT_URL, UNIVERSE_DOMAIN
)
from app.constant.businessProcess import BADSHA_RANGE 
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

class badshaSpreadsheet():
    def __init__(self, data, live_url, copy_url):
        self.data = data
        self.live_url = live_url
        self.copy_url = copy_url["url"]

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

    def get_sheet_id(self, spreadsheet_id, sheet_name):
        """Fetch the sheetId from spreadsheet by name."""
        metadata = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in metadata.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                return sheet["properties"]["sheetId"]
        raise Exception(f"Sheet {sheet_name} not found")

    def get_first_empty_row(self, copy_url, sheet_name, col="A", start_row=1):
        """Find the first empty row in a given column (default column A), starting from start_row."""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=copy_url,
                range=f"{sheet_name}!{col}{start_row}:{col}"  # read from row 4 downward
            ).execute()

            values = result.get("values", [])

            if sheet_name in ["Provider Performance (Data)", "Overall Performance (Data)"]:
                return len(values) + 2

            return len(values) + start_row  # offset by start_row
        except HttpError as err:
            raise Exception(f"Google Sheets API error (get_first_empty_row): {err}")

    def clean_entry(self, entry):
        cleaned = []
        for v in entry.values():
            if isinstance(v, str):
                # strip spaces and collapse newlines
                v = v.replace("\n", " ").strip()
            cleaned.append(v)
        return cleaned

    def batch_insert_values(self, copy_url, data_dict, chunk_size=1500):
        """
        Insert multiple datasets into different sheets in batched API calls.
        Splits large inserts into chunks to avoid API timeouts.
        Expands sheet rows if needed.
        """
        try:
            # Get sheet metadata once
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=copy_url).execute()
            sheets = {s["properties"]["title"]: s for s in sheet_metadata.get("sheets", [])}

            for sheet_name, values in data_dict.items():
                if not values:
                    continue

                # Find first empty row
                start_row = self.get_first_empty_row(copy_url, sheet_name, col="A", start_row=1)

                # Get sheet properties
                sheet_props = sheets[sheet_name]["properties"]
                sheet_id = sheet_props["sheetId"]
                row_count = sheet_props["gridProperties"]["rowCount"]

                # Expand rows if needed
                needed_rows = start_row + len(values) - row_count
                if needed_rows > 0:
                    requests = [{
                        "appendDimension": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "length": needed_rows
                        }
                    }]
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=copy_url,
                        body={"requests": requests}
                    ).execute()

                # Insert in chunks
                for i in range(0, len(values), chunk_size):
                    chunk = values[i:i+chunk_size]
                    body = {
                        "valueInputOption": "USER_ENTERED",
                        "data": [
                            {
                                "range": f"{sheet_name}!A{start_row + i}",
                                "values": chunk
                            }
                        ]
                    }
                    self.service.spreadsheets().values().batchUpdate(
                        spreadsheetId=copy_url,
                        body=body
                    ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (batch_insert_values): {err}")



    def get_row(self, copy_url, sheet_name, col, start_row, item=None):
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=copy_url,
                range=f"{sheet_name}!{col}{start_row}:{col}"  # read from row 1 downward
            ).execute()

            values = result.get("values", [])

            for i, row in enumerate(values, start=start_row):
                if row and row[0] == item:
                    return i
            return None  # not found
        except HttpError as err:
            raise Exception(f"Google Sheets API error (get_first_empty_row): {err}")
        
    def account_creation(self, copy_url, sheet_name, value, match):
        """
        Update deposit/withdrawal value into Google Sheet (single update).
        """
        try:
            # Find the row in col A matching the label
            row = self.get_row(copy_url, sheet_name, col="A", start_row=1, item=match)

            if row is None:
                raise Exception(f"Match '{match}' not found in column A of {sheet_name}")

            # Ensure value is a list of lists
            if not isinstance(value, list):
                value = [[value]]
            elif not isinstance(value[0], list):
                value = [value]

            body = {
                "range": f"{sheet_name}!B{row}",
                "majorDimension": "ROWS",
                "values": value
            }

            self.service.spreadsheets().values().update(
                spreadsheetId=copy_url,
                range=f"{sheet_name}!B{row}",
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (account_creation): {err}")

    def copy_deposit_withdrawal_columns(self, spreadsheet_id, sheet_name):
        try:
            sheet_id = self.get_sheet_id(spreadsheet_id, sheet_name)

            # --- Step 1: Read column J (index 9) ---
            range_j = f"{sheet_name}!J:J"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_j
            ).execute()

            values = result.get("values", [])

            # --- Step 2: Parse numbers (support -, .00, decimals) ---
            parsed_values = []
            for row in values:
                if row:  # not empty
                    raw = str(row[0]).strip()
                    try:
                        # allow negatives and decimals
                        num = float(raw.replace(",", ""))  
                        parsed_values.append([num])
                    except ValueError:
                        parsed_values.append([""])  # keep blank if invalid
                else:
                    parsed_values.append([""])

            # --- Step 3: Copy I -> K (still raw copyPaste) ---
            requests = [
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "startColumnIndex": 8,   # I
                            "endColumnIndex": 9
                        },
                        "destination": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "startColumnIndex": 10,  # K
                            "endColumnIndex": 11
                        },
                        "pasteType": "PASTE_VALUES"
                    }
                }
            ]

            body = {"requests": requests}
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

            # --- Step 4: Write parsed J -> L ---
            if parsed_values:
                range_l = f"{sheet_name}!L1"
                self.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_l,
                    valueInputOption="USER_ENTERED",
                    body={"values": parsed_values}
                ).execute()

            # --- Step 5: Format L as Number (handles negatives & decimals) ---
            format_request = {
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startColumnIndex": 11,  # L
                                "endColumnIndex": 12
                                # whole column
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "numberFormat": {
                                        "type": "NUMBER",
                                        "pattern": "#,##0.00"  # keeps 2 decimals, supports -123.45
                                    }
                                }
                            },
                            "fields": "userEnteredFormat.numberFormat"
                        }
                    }
                ]
            }

            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=format_request
            ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (copy_deposit_withdrawal_columns): {err}")

    def copy_summary_data(self, job_id, copy_url, sheet_name):
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=copy_url,
                range=f"{sheet_name}!B3:B"  # read from row B3 downward
            ).execute()

            values = result.get("values", [])

            cleaned_values = []
            for value in values:
                if value:
                    # Use regex to remove currency symbols but keep the negative sign and digits
                    cleaned_value = re.sub(r'[^0-9.%\-]', '', value[0])
                    cleaned_values.append([cleaned_value])
                else:
                    # If there is no data, keep it as is (empty)
                    cleaned_values.append([""])

            return cleaned_values
        except HttpError as err:
            raise Exception(f"Google Sheets API error (Reading Row): {err}")

    def transfer(self, job_id):
        try:
            log(job_id, "📤 Starting data transfer to spreadsheet...")

            # --- Step 1: Compare CODE sheet vs previous JSON ---

            log(job_id,"Preparing Data")
            account_creation = self.data.get("account_creation", [])
            deposit_withdrawal = self.data.get("deposit_withdrawal", [])
            # deposit_total = self.data.get("deposit_total", [])
            # withdrawal_total = self.data.get("withdrawal_total", [])
            overall_performance = self.data.get("overall_performance", [])
            provider_performance = self.data.get("provider_performance", [])

            # --- Convert dicts → lists of values (remove keys) ---
            # account_creation_value = [self.clean_entry(entry) for entry in account_creation]
            deposit_withdrawal_value = [self.clean_entry(entry) for entry in deposit_withdrawal]
            overall_performance_value = [self.clean_entry(entry) for entry in overall_performance]
            provider_performance_value = [self.clean_entry(entry) for entry in provider_performance]
            log(job_id, "Data is Fetching in Spreadsheet")

            self.batch_insert_values(self.copy_url, {
                BADSHA_RANGE["DepositWithdrawal"]: deposit_withdrawal_value,
                BADSHA_RANGE["OverallPerformance"]: overall_performance_value,
                BADSHA_RANGE["ProviderPerformance"]: provider_performance_value,
            })


            self.account_creation(self.copy_url, BADSHA_RANGE["SUMMARY"], account_creation, "New Player Accounts Created")

            # Copy DepositWithdrawal J -> K & L
            self.copy_deposit_withdrawal_columns(self.copy_url, BADSHA_RANGE["DepositWithdrawal"])

            
            
            data = self.copy_summary_data(job_id, self.copy_url, BADSHA_RANGE["SUMMARY"])

            if not data:
                return {
                    "status": 204,
                    "message": "Coudn't Copy the Data"
                }

            return {
                "status": 200,
                "data": data
            }
        except Exception as e:
            log(job_id, f"❌ Transfer failed due to error: {e}")
            return {"status": "Failed"}
