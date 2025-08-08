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
    def __init__(self, data, sheet_url, sheet_name):
        self.data = data
        self.url = sheet_url
        self.sheetId = sheet_name
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

    def insert_data_into_sheet(self, job_id, brand, rows):
        sheet_range = TRACKER_RANGE.get(brand)
        if sheet_range:
            try:
                # Fetch existing data
                result = self.service.spreadsheets().values().get(
                    spreadsheetId=self.url,
                    range=sheet_range
                ).execute()

                existing_data = result.get('values', [])
                last_row = len(existing_data)

                # Extract just the sheet name
                sheet_name = sheet_range.split("!")[0] if "!" in sheet_range else sheet_range

                # Build append range
                append_range = f"{sheet_name}!A{last_row + 1}"

                body = {
                    "values": rows
                }

                self.service.spreadsheets().values().append(
                    spreadsheetId=self.url,
                    range=append_range,
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body=body
                ).execute()

                log(job_id, f"✅ Data for {brand} appended successfully starting from row {last_row + 2}.")
            except HttpError as err:
                log(job_id, f"❌ Error appending data for {brand}: {err}")
        else:
            log(job_id, f"⚠️ No defined range for {brand}, skipping...")

    def transfer(self, job_id):
        try:
            log(job_id, "📤 Starting data transfer to spreadsheet...")

            timestamp = datetime.today().strftime('%b - %d - %y %H:%M:%S.%f')

            # ✅ Organize data by brand/sheet (keep full rows)
            sheet_data = defaultdict(list)
            for row in self.data:
                if not row or not row[1]:
                    continue

                sheet_name = row[1]  # brand
                sheet_data[sheet_name].append(list(row))  # keep entire row

            log(job_id, f"📌 Brands with data: {list(sheet_data.keys())}")

            processing_order = ["6S", "BAJI", "JB"]
            for brand in processing_order:
                if brand in sheet_data:
                    rows_to_insert = []
                    for row in sheet_data[brand]:
                        if len(row) != 8:
                            log(job_id, f"⚠️ Skipping row for {brand} due to unexpected format: {row}")
                            continue

                        # (date, brand, username, currency, platform, keyword, nsu, ftd)
                        date = row[0]
                        brand = row[1]
                        rest = row[2:]  # username, currency, platform, keyword, nsu, ftd

                        # ✅ Final format: [date, brand, username, currency, platform, keyword, nsu, ftd, timestamp]
                        new_row = [date, brand] + rest + [timestamp]
                        rows_to_insert.append(new_row)

                    if rows_to_insert:
                        log(job_id, f"➡️ Inserting {len(rows_to_insert)} rows for brand {brand}")
                        self.insert_data_into_sheet(job_id, brand, rows_to_insert)
                    else:
                        log(job_id, f"⚠️ No valid rows to insert for brand {brand}")
                else:
                    log(job_id, f"🚫 No data for brand {brand}, skipping...")

            log(job_id, "✅ Finished transferring data to spreadsheet.")
        except Exception as e:
            log(job_id, f"❌ Transfer failed due to error: {e}")
            return {
                "status": "Failed"
            }
