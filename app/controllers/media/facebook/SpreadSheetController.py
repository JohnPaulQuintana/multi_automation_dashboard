from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN

import requests
import os
import time
import re
import random
# from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
# from config.config import Config
from datetime import datetime, timedelta, timezone
from typing import Optional


class SpreadSheetController:
    def __init__(self, spreadsheet, range=None):
        self.spreadsheet = spreadsheet
        self.range = range if range else "ACCOUNTS!A3:I"

        self.config_dict = {
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

    def get_facebook_accounts(self):
        print("Fetching accounts from spreadsheet...")
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(self.config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range=self.range).execute()
            values = result.get('values', [])
            if not values:
                print("No data found.")
            else:
                # for row in values:
                #     print(row)  # Process each row as needed
                return values
        except HttpError as err:
            print(f"An error occurred: {err}")

    def get_facebook_pages(self):
        print("Fetching pages from spreadsheet...")
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(self.config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range="PAGES!A2:K").execute()
            values = result.get('values', [])
            if not values:
                print("No data found.")
            else:
                # for row in values:
                #     print(row)  # Process each row as needed
                return values
        except HttpError as err:
            print(f"An error occurred: {err}")
    
    def get_spreadsheet_column(self, spreadsheet_id: str, tab_name: str, currency: str, insights:list, total_followers: int = 0 ,page_type: str = "page"):
        try:
            # Initialize service
            service = self._initialize_google_sheets_service()
            
            # Get sheet metadata and ID
            sheet_id = self._get_sheet_id(service, spreadsheet_id, tab_name)
            if sheet_id is None:
                return None

            #ON DEVELOPMENT
            # Get today's date string
            # today_str = datetime.now().strftime('%d/%m/%Y') # Current date

            #ON DEPLOYED
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)
            today_str = yesterday.strftime('%d/%m/%Y') #Yesterday date

            # Check or create date column
            date_col_index = self._handle_date_column(
                service, spreadsheet_id, sheet_id, tab_name, today_str
            )
            
            # Get all values from sheet
            values = self._get_sheet_values(service, spreadsheet_id, tab_name)
            if not values:
                return None

            # Find currency row
            currency_row_index = self._find_currency_row(values, currency, page_type)
            if currency_row_index is None:
                return None

            # Get value from column E for difference calculation
            value_in_column_e = self._get_value_from_column_e(values, currency_row_index, currency)
            if value_in_column_e is None:
                return None

            # Update cells with today's date, total followers, and difference
            self._update_sheet_values(
                service, spreadsheet_id, sheet_id, tab_name,
                today_str, currency_row_index, insights,total_followers, value_in_column_e
            )
            
            return values

        except HttpError as err:
            print(f"An error occurred: {err}")
            return None