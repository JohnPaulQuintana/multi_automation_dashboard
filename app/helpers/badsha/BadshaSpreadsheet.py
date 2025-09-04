from app.config.loader import (
    TYPE, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_EMAIL, CLIENT_ID,
    AUTH_URI, TOKEN_URI, AUTH_PROVIDER_X509_CERT_URL, CLIENT_X509_CERT_URL, UNIVERSE_DOMAIN
)
from app.constant.badsha import DAILY_BO_BADSHA_RANGE
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

class spreadsheet():
    def __init__(self, data, sheet_url, sheet_range, yesterdayDate):
        self.data = data
        self.url = sheet_url
        self.sheetId = sheet_range
        self.startDate = yesterdayDate
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


    def batch_insert_values(self, sheet_id, data_dict):
        """
        Insert multiple datasets into different sheets in ONE API call.
        data_dict: { sheet_name: (values, vt_apl_tpl_flag) }
        """
        try:
            requests = []
            for sheet_name, (values, vt_apl_tpl) in data_dict.items():
                if not values:
                    continue

                # Find first empty row for this sheet
                start_row = self.get_first_empty_row(sheet_id, sheet_name, col="A", start_row=4)

                if vt_apl_tpl:
                    # transform values (date, None, None, rest)
                    new_values = []
                    for row in values:
                        date_value = row[0]
                        rest = row[1:]
                        new_values.append([date_value, None, None] + rest)
                    values = new_values
                    insert_col = "A"
                else:
                    insert_col = "D"

                requests.append({
                    "range": f"{sheet_name}!{insert_col}{start_row}",
                    "values": values
                })

            if requests:
                body = {
                    "valueInputOption": "USER_ENTERED",
                    "data": requests
                }
                self.service.spreadsheets().values().batchUpdate(
                    spreadsheetId=sheet_id,
                    body=body
                ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (batch_insert_values): {err}")

    def clean_entry(self, entry):
        cleaned = []
        for v in entry.values():
            if isinstance(v, str):
                # strip spaces and collapse newlines
                v = v.replace("\n", " ").strip()
            cleaned.append(v)
        return cleaned

    def update_code_header(self, sheet_id, sheet_name, date):
        """
        Update merged cell C1:K1 in the CODE sheet.
        Note: you only need to update the top-left cell (C1).
        """
        try:
            parsed_date = datetime.strptime(date, "%d-%m-%Y")
            date_value = parsed_date.strftime("%d/%m/%Y") 

            body = {"values": [[date_value]]}
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!C1",
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()

            return date_value
        except HttpError as err:
            raise Exception(f"Google Sheets API error (update_code_header): {err}")
        
    def bobadsha(self, sheet_id, sheet_name):
        try:
            # Step 1: Pull a broad range (e.g. A3:M1000, adjust as needed)
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!A3:M1000"
            ).execute()

            values = result.get("values", [])
            
            # Step 2: Find the row where "TOTAL" appears in column A
            end_row = None
            for idx, row in enumerate(values, start=3):  # start=3 because we began from A3
                if row and row[0].strip().upper() == "TOTAL":
                    end_row = idx - 1   # use the row before TOTAL
                    break
            
            sheet_range = f"A3:M{end_row}"

            final_result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!{sheet_range}"
            ).execute()

            return final_result.get("values", [])

        except HttpError as err:
            raise Exception(f"Google Sheets API error (bobadsha): {err}")
    
    def affibo(self, sheet_id, sheet_name, sheet_range="A:F"):
        """
        Read values from sheet dynamically.
        - Finds the row where 'TOTAL' is in column A.
        - Starts reading 2 rows after that.
        """
        try:
            # Fetch all rows in the given range
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!{sheet_range}"
            ).execute()
            rows = result.get("values", [])

            # Find where 'TOTAL' appears in column A
            total_row = None
            for idx, row in enumerate(rows, start=1):  # 1-based row index
                if row and str(row[0]).strip().upper() == "TOTAL":
                    total_row = idx
                    break

            if total_row:
                start_row = total_row + 2  # start 2 rows after TOTAL
                dynamic_range = f"{sheet_name}!A{start_row}:{sheet_range.split(':')[-1]}"
                result = self.service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=dynamic_range
                ).execute()
                return result.get("values", [])

            # If TOTAL not found, just return everything
            return rows

        except HttpError as err:
            raise Exception(f"Google Sheets API error (affibo): {err}")


    def normalize_date(self, date_str: str):
        """
        Try multiple formats for parsing sheet dates.
        Returns a normalized YYYY-MM-DD string for comparison.
        """
        for fmt in ["%b %d %Y", "%B %d %Y", "%d/%m/%Y"]:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    def ensure_rows(self, sheet_id, sheet_name, needed_rows):
        """
        Ensure the sheet has at least `needed_rows` rows.
        """
        sheet_metadata = self.service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get("sheets", [])
        
        for s in sheets:
            properties = s.get("properties", {})
            if properties.get("title") == sheet_name:
                current_rows = properties.get("gridProperties", {}).get("rowCount", 0)

                if needed_rows > current_rows:
                    requests = [{
                        "appendDimension": {
                            "sheetId": properties["sheetId"],
                            "dimension": "ROWS",
                            "length": needed_rows - current_rows
                        }
                    }]
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={"requests": requests}
                    ).execute()
                break

    def write_values(self, job_id, sheet_id, sheet_name, values, targetDate, add_blank=False):
        """
        Write values for a given date.
        - If date exists: overwrite existing rows and expand if more rows are needed.
        - Always leave exactly ONE blank row after that date block.
        """
        try:
            target_date = self.normalize_date(targetDate)

            # Fetch all existing rows in sheet
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!A3:Z"  # adjust column range as needed
            ).execute()
            rows = result.get("values", [])

            # Find start/end row for this date
            start_row = None
            end_row = None
            for idx, row in enumerate(rows, start=3):
                if row and self.normalize_date(row[0]) == target_date:
                    if start_row is None:
                        start_row = idx
                    end_row = idx

            if start_row:  
                # ✅ Date exists → update
                existing_count = end_row - start_row + 1
                new_count = len(values)

                log(job_id, f"Updating {targetDate} ({existing_count} → {new_count} rows)")

                # Ensure enough rows exist
                self.ensure_rows(sheet_id, sheet_name, start_row + new_count)

                # Overwrite from start_row
                update_range = f"{sheet_name}!A{start_row}"
                self.service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=update_range,
                    valueInputOption="USER_ENTERED",
                    body={"values": values}
                ).execute()

                # Ensure one blank row after
                if add_blank:
                    blank_row = [[""] * len(values[0])]
                    self.service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=f"{sheet_name}!A{start_row+new_count}",
                        valueInputOption="USER_ENTERED",
                        body={"values": blank_row}
                    ).execute()

            else:
                # ✅ Date not found → append new block at bottom
                last_row = len(rows) + 2  # because we started at A3
                start_row = last_row + (2 if add_blank else 1)

                log(job_id, f"Appending new {targetDate} ({len(values)} rows) at row {start_row}")

                # Ensure enough rows exist
                self.ensure_rows(sheet_id, sheet_name, start_row + len(values))

                # Write values
                start_cell = f"A{start_row}"
                self.service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"{sheet_name}!{start_cell}",
                    valueInputOption="USER_ENTERED",
                    body={"values": values}
                ).execute()

                # Always leave one blank row after
                blank_row = [[""] * len(values[0])]
                self.service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"{sheet_name}!A{start_row+len(values)}",
                    valueInputOption="USER_ENTERED",
                    body={"values": blank_row}
                ).execute()

        except HttpError as err:
            raise Exception(f"Google Sheets API error (write_values): {err}")

    
    def load_previous_code_data(self, filepath="app/helpers/badsha/code_snapshot.json"):
        """Load previous Code sheet snapshot if it exists."""
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return json.load(f)
        return None
    
    def save_current_code_data(self, data, filepath="app/helpers/badsha/code_snapshot.json"):
        """Save current Code sheet snapshot for future reference."""
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
    
    def _column_index_to_letter(self, index: int) -> str:
        """Convert column index (1-based) to Excel/Sheets-style column letter."""
        letters = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

    def get_code_data(self, sheet_id, sheet_name, sheet_range="A5"):
        """Fetch Code sheet values with dynamic last column range."""
        try:
            # Get sheet metadata to find last column
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=sheet_id, includeGridData=False
            ).execute()

            # Find the target sheet info
            sheets = sheet_metadata.get("sheets", [])
            last_col_letter = "R"  # fallback

            for s in sheets:
                if s["properties"]["title"] == sheet_name:
                    last_col_index = s["properties"]["gridProperties"]["columnCount"]
                    # Convert index (1-based) to column letter
                    last_col_letter = self._column_index_to_letter(last_col_index)
                    break

            # Build dynamic range: from A5 to last_col
            final_range = f"{sheet_name}!A5:{last_col_letter}"

            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=final_range
            ).execute()

            return result.get("values", [])

        except HttpError as err:
            raise Exception(f"Google Sheets API error (get_code_data): {err}")
    
    def compare_snapshots(self, old_data, new_data):
        """Compare two snapshots and return only the differences (value-level)."""
        changes = []
        max_len = max(len(old_data), len(new_data))

        for i in range(max_len):
            old_row = old_data[i] if i < len(old_data) else None
            new_row = new_data[i] if i < len(new_data) else None

            # 🔄 Case 1: Row existed in both → check cell by cell
            if old_row and new_row:
                for col, (old_val, new_val) in enumerate(zip(old_row, new_row), start=1):
                    if str(old_val).strip() != str(new_val).strip():
                        changes.append(f"Change: old={old_val} → new={new_val}")

                # If new_row has extra cells
                if len(new_row) > len(old_row):
                    for extra in new_row[len(old_row):]:
                        changes.append(f"Added Value: {extra}")

            # ➕ Case 2: New row added
            elif not old_row and new_row:
                changes.append(f"Added Row: {new_row}")

            # ❌ Case 3: Row removed
            elif old_row and not new_row:
                changes.append(f"Removed Row: {old_row}")

        return changes

    def transfer(self, job_id):
        try:
            log(job_id, "📤 Starting data transfer to spreadsheet...")

            # --- Step 1: Compare CODE sheet vs previous JSON ---
            log(job_id, "🔎 Checking CODE sheet against previous snapshot...")
            previous_snapshot = self.load_previous_code_data()
        
            log(job_id,"Preparing Data")
            nsu_data = self.data.get("NSU", [])
            ftd_data = self.data.get("FTD", [])
            withdrawal_data = self.data.get("WITHDRAWAL", [])
            deposit_data = self.data.get("DEPOSIT", [])
            vt_apl_tpl_data = self.data.get("VT/APL/TPL", [])

            # --- Convert dicts → lists of values (remove keys) ---
            log(job_id,"Adjusting Data")
            nsu_values = [self.clean_entry(entry) for entry in nsu_data]
            ftd_values = [self.clean_entry(entry) for entry in ftd_data]
            withdrawal_values = [self.clean_entry(entry) for entry in withdrawal_data]
            deposit_values = [self.clean_entry(entry) for entry in deposit_data]
            vt_apl_tpl_values = [self.clean_entry(entry) for entry in vt_apl_tpl_data]
            log(job_id, "Data is Fetching in Spreadsheet")
            self.batch_insert_values(self.url, {
                DAILY_BO_BADSHA_RANGE["NSU"]: (nsu_values, False),
                DAILY_BO_BADSHA_RANGE["FTD"]: (ftd_values, False),
                DAILY_BO_BADSHA_RANGE["DEPOSIT"]: (deposit_values, False),
                DAILY_BO_BADSHA_RANGE["WITHDRAWAL"]: (withdrawal_values, False),
                DAILY_BO_BADSHA_RANGE["VT/APL/TPL"]: (vt_apl_tpl_values, True),
            })

            # Process for sheetNames (CODE, AFFILIATE, BOBADSHA AFFIBO)
            time.sleep(2.5) 
            log(job_id, "Processing on the Date of Sheet(CODE)")
            # codeDate = self.update_code_header(self.url, DAILY_BO_BADSHA_RANGE["CODE"])
            # if not codeHeader:
            #     log(job_id, "The updating Date in Sheet(CODE Failed)")

            # Example: backlog from 15-08-2025 to yesterday
            try:
                yesterdayDate = datetime.now().date() - timedelta(days=1)
                start_date = datetime.strptime(self.startDate, "%d-%m-%Y").date()

                while start_date <= yesterdayDate:
                    formatted_date = start_date.strftime("%d-%m-%Y")
                    codeDate = self.update_code_header(self.url, DAILY_BO_BADSHA_RANGE["CODE"], formatted_date)

                    log(job_id, f"The Date in the Sheet(CODE) has been Changed to {formatted_date}")
                    time.sleep(4)
                    log(job_id, "Processing on the Result for Sheets(CODE, AFFILIATE, BOBADSHA, AFFIBO)")

                    BOBADSHA = self.bobadsha(self.url, DAILY_BO_BADSHA_RANGE["AFFILIATE"])
                    time.sleep(1.5)
                    AFFIBO = self.affibo(self.url, DAILY_BO_BADSHA_RANGE["AFFILIATE"])
                    time.sleep(1.5)
                    log(job_id, f"Successfuly Copied The Data for {formatted_date}")

                    if BOBADSHA:
                        self.write_values(job_id, self.url, DAILY_BO_BADSHA_RANGE["BOBADSHA"], BOBADSHA, codeDate, add_blank=True)
                        log(job_id, "BOBADSHA Data Insertion Completed")
                        time.sleep(1.5)
                    if AFFIBO:
                        self.write_values(job_id, self.url, DAILY_BO_BADSHA_RANGE["AFFIBO"], AFFIBO, codeDate )
                        log(job_id, "AFFIBO Data Insertion Completed")
                        time.sleep(1.5)

                    start_date += timedelta(days=1)
            except Exception as e:
                log(job_id, f"❌ Error in process_bobadsha_affibo: {e}")
                raise


            # Fetch Latest Snapshot after Automation
            latest_code = self.get_code_data(self.url, DAILY_BO_BADSHA_RANGE["CODE"])

            if previous_snapshot:
                log(job_id, "🔎 Comparing with previous snapshot...")
                diffs = self.compare_snapshots(previous_snapshot, latest_code)

                if diffs:
                    for change in diffs:
                        log(job_id, change)
                else:
                    log(job_id, "✅ No changes detected.")
            else:
                log(job_id, "ℹ️ No previous snapshot found (first run). Skipping diff report.")

            log(job_id, "✅ Transfer completed successfully.")

            # Save current CODE sheet snapshot ---
            log(job_id, "💾 Saving current CODE sheet snapshot for next automation...")
            self.save_current_code_data(latest_code)

            log(job_id, "✅ Transfer completed successfully.")
        except Exception as e:
            log(job_id, f"❌ Transfer failed due to error: {e}")
            return {"status": "Failed"}
