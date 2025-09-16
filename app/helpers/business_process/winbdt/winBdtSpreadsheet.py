from app.config.loader import (
    TYPE, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_EMAIL, CLIENT_ID,
    AUTH_URI, TOKEN_URI, AUTH_PROVIDER_X509_CERT_URL, CLIENT_X509_CERT_URL, UNIVERSE_DOMAIN
)
from app.constant.businessProcess import WINBDT_RANGE 
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

class winBdtSpreadsheet():
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

    
    def batch_insert_values(self, copy_url, data_dict):
        """
        Insert multiple datasets into different sheets in ONE API call.
        data_dict: { sheet_name: (values, vt_apl_tpl_flag) }
        """
        try:
            requests = []
            for sheet_name, values in data_dict.items():
                if not values:
                    continue

                # Find first empty row for this sheet
                start_row = self.get_first_empty_row(copy_url, sheet_name, col="A", start_row=1)


                requests.append({
                    "range": f"{sheet_name}!A{start_row}",
                    "values": values
                })

            if requests:
                body = {
                    "valueInputOption": "USER_ENTERED",
                    "data": requests
                }
                self.service.spreadsheets().values().batchUpdate(
                    spreadsheetId=copy_url,
                    body=body
                ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (batch_insert_values): {err}")
        
    def insert_account_creation(self, copy_url, values):
        """
        Insert AccountCreation data separately (always as plain text, preserving
        leading zeros and preventing scientific notation).
        """
        try:
            if not values:
                return {"status": 204, "message": "No AccountCreation data to insert"}

            def protect_value(val):
                val_str = str(val)
                return val_str  # always keep as plain string

            safe_values = [[protect_value(value) for value in row] for row in values]

            start_row = self.get_first_empty_row(
                copy_url, WINBDT_RANGE["AccountCreation"], col="A", start_row=1
            )

            body = {
                "valueInputOption": "RAW",  # <-- keep exact plain text, no formatting
                "data": [{
                    "range": f"{WINBDT_RANGE['AccountCreation']}!A{start_row}",
                    "values": safe_values
                }]
            }

            self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=copy_url,
                body=body
            ).execute()

            return {"status": 200, "message": "AccountCreation inserted successfully"}

        except HttpError as err:
            raise Exception(f"Google Sheets API error (insert_account_creation): {err}")



    def get_row(self, copy_url, sheet_name, col, start_row, item=None):
        """
        Find the row number in a Google Sheet where the given column matches `match`.

        Args:
            copy_url (str): Spreadsheet ID
            sheet_name (str): The sheet/tab name
            col (str): Column letter to search in (default "A")
            start_row (int): Row number to start searching from (default 1)
            match (str): The value to search for

        Returns:
            int: The row number (1-based, as in Sheets), or None if not found
        """
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
        
    def deposit_withdrawal_batch(self, copy_url, items):
        """
        Batch update deposit/withdrawal values into Google Sheet.

        Args:
            copy_url (str): Spreadsheet ID
            items (list): List of tuples in format (sheet_name, value, match_string)
        """
        try:
            requests = []

            for sheet_name, value, match in items:
                # Find the row in col A matching the label
                row = self.get_row(copy_url, sheet_name, col="A", start_row=1, item=match)

                if row is None:
                    raise Exception(f"Match '{match}' not found in column A of {sheet_name}")

                # Always ensure value is a list of lists
                if not isinstance(value, list):
                    value = [[value]]
                elif not isinstance(value[0], list):
                    value = [value]

                requests.append({
                    "range": f"{sheet_name}!B{row}",
                    "values": value
                })

            if requests:
                body = {
                    "valueInputOption": "USER_ENTERED",
                    "data": requests
                }
                self.service.spreadsheets().values().batchUpdate(
                    spreadsheetId=copy_url,
                    body=body
                ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (deposit_withdrawal_batch): {err}")
    
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
            deposit_withdrawal_results = self.data.get("deposit_withdrawal_results", [])
            deposit_total = self.data.get("deposit_total", [])
            withdrawal_total = self.data.get("withdrawal_total", [])
            overall_performance = self.data.get("overall_performance", [])
            provider_performance = self.data.get("provider_performance", [])

            # --- Convert dicts → lists of values (remove keys) ---
            account_creation_value = [self.clean_entry(entry) for entry in account_creation]
            deposit_withdrawal_value = [self.clean_entry(entry) for entry in deposit_withdrawal_results]
            # deposit_total_value = [self.clean_entry(entry) for entry in deposit_total]
            # withdrawal_results_value = [self.clean_entry(entry) for entry in withdrawal_results]
            overall_performance_value = [self.clean_entry(entry) for entry in overall_performance]
            provider_performance_value = [self.clean_entry(entry) for entry in provider_performance]
            log(job_id, "Data is Fetching in Spreadsheet")
            self.batch_insert_values(self.copy_url, {
                WINBDT_RANGE["DepositWithdrawal"]: deposit_withdrawal_value,
                WINBDT_RANGE["OverallPerformance"]: overall_performance_value,
                WINBDT_RANGE["ProviderPerformance"]: provider_performance_value,
            })

            if account_creation_value:
                self.insert_account_creation(self.copy_url, account_creation_value)

            self.deposit_withdrawal_batch(
                self.copy_url,
                [
                    (WINBDT_RANGE["SUMMARY"], deposit_total, "Total Top Ups"),
                    (WINBDT_RANGE["SUMMARY"], withdrawal_total, "Total Withdrawals"),
                ]
            )


            data = self.copy_summary_data(job_id, self.copy_url, WINBDT_RANGE["SUMMARY"])

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
