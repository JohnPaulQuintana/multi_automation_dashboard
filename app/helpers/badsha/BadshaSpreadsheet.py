from app.config.loader import (
    TYPE, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_EMAIL, CLIENT_ID,
    AUTH_URI, TOKEN_URI, AUTH_PROVIDER_X509_CERT_URL, CLIENT_X509_CERT_URL, UNIVERSE_DOMAIN
)
from app.constant.tracker import TRACKER_RANGE
from app.config.loader import NSU_FTD_TRACKER_SHEET
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from app.automations.log.state import log
from typing import List, Any
import re
from collections import defaultdict
from datetime import datetime

class spreadsheet():
    def __init__(self, data, sheet_url, sheet_range, yesterdayDate):
        self.data = data
        self.url = sheet_url
        self.sheetId = sheet_range
        self.yesterdayDate = yesterdayDate
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

    def get_first_empty_row(self, sheet_id, sheet_name, col="A", start_row=4):
        """Find the first empty row in a given column (default column A), starting from start_row."""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!{col}{start_row}:{col}"  # read from row 4 downward
            ).execute()

            values = result.get("values", [])
            return len(values) + start_row  # offset by start_row
        except HttpError as err:
            raise Exception(f"Google Sheets API error (get_first_empty_row): {err}")


    def insert_values(self, sheet_id, sheet_name, values, col="A", add_blank=False, add_date=False):
        """Insert rows into the first empty cell starting at row 4 in column A."""
        try:
            start_row = self.get_first_empty_row(sheet_id, sheet_name, col, start_row=4)
            
            # if add_blank=True, skip one row (leave blank)
            if add_blank:
                start_row += 1  

            if add_date:
                # Date goes in A, B & C blank, then rest in D+
                parsed_date = datetime.strptime(self.yesterdayDate, "%d-%m-%Y")
                date_value = parsed_date.strftime("%b %d %Y")

                values = [[date_value] + [None, None] + row for row in values]
                insert_col = "A"
            else:
                # Default case → insert starting at D
                insert_col = "D"
            body = {"values": values}
            self.service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!{insert_col}{start_row}",  # insert starting at col D
                valueInputOption="RAW",
                body=body
            ).execute()
        except HttpError as err:
            raise Exception(f"Google Sheets API error: {err}")


    def clean_entry(self, entry):
        cleaned = []
        for v in entry.values():
            if isinstance(v, str):
                # strip spaces and collapse newlines
                v = v.replace("\n", " ").strip()
            cleaned.append(v)
        return cleaned

    def transfer(self, job_id):
        try:
            log(job_id, "📤 Starting data transfer to spreadsheet...")
            nsu_data = self.data.get("NSU", [])
            ftd_data = self.data.get("FTD", [])
            withdrawal_data = self.data.get("WITHDRAWAL", [])
            deposit_data = self.data.get("DEPOSIT", [])
            vt_apl_tpl_data = self.data.get("VT/APL/TPL", [])

            # --- Convert dicts → lists of values (remove keys) ---
            nsu_values = [self.clean_entry(entry) for entry in nsu_data]
            ftd_values = [self.clean_entry(entry) for entry in ftd_data]
            withdrawal_values = [self.clean_entry(entry) for entry in withdrawal_data]
            deposit_values = [self.clean_entry(entry) for entry in deposit_data]
            vt_apl_tpl_values = [self.clean_entry(entry) for entry in vt_apl_tpl_data]

            if nsu_values:
                self.insert_values(self.url, "NSU DATA", nsu_values)
                log(job_id, "NSU Data Insertion Completed")
            if ftd_values:
                self.insert_values(self.url, "FTD DATA", ftd_values)
                log(job_id, "FTD Data Insertion Completed")
            if deposit_values:
                self.insert_values(self.url, "DEPOSIT", deposit_values)
                log(job_id, "DEPOSIT Data Insertion Completed")
            if withdrawal_values:
                self.insert_values(self.url, "WITHDRAWAL", withdrawal_values)
                log(job_id, "WITHDRAWAL Data Insertion Completed")
            if vt_apl_tpl_values:
                self.insert_values(self.url, "VT/APL/TPL", vt_apl_tpl_values, add_blank=True, add_date=True)
                log(job_id, "VT/APL/TPL Data Insertion Completed")

        except Exception as e:
            log(job_id, f"❌ Transfer failed due to error: {e}")
            return {"status": "Failed"}
