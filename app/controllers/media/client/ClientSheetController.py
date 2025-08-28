from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, List, TypedDict
from app.automations.log.state import log
import requests
import os
import time
import re
import random
import ssl
import concurrent.futures

class PlatformCell(TypedDict):
    row: int
    value: str

class ClientSheetController:
    def __init__(self):
        self._service = None
        self._session = None

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
    
    def _initialize_google_sheets_service(self, job_id, retries: int = 3) -> bool:
        """Initialize Google Sheets service with SSL context and retry logic"""
        log(job_id, "Initializing Google Sheets service with secure connection...")
        # config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        
        try:
            # Create custom SSL context
            ssl_context = ssl.create_default_context()
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            
            # Initialize credentials
            creds = Credentials.from_service_account_info(
                self.config_dict,
                scopes=scope
            )
            
            # Create authorized session with SSL context
            self._session = AuthorizedSession(creds)
            self._session.verify = ssl_context
            
            # Build service with custom session
            self._service = build(
                'sheets',
                'v4',
                credentials=creds,
                static_discovery=False
            )
            
            log(job_id, "Google Sheets service initialized successfully")
            return True
            
        except ssl.SSLError as e:
            log(job_id, f"SSL Error: {e}")
            if retries > 0:
                log(job_id, f"Retrying... ({retries} attempts remaining)")
                return self._initialize_google_sheets_service(job_id, retries - 1)
            raise ConnectionError("Failed to establish secure connection after retries")
        except Exception as e:
            log(job_id, f"Initialization Error: {e}")
            raise

    def _safe_find_targets(
        self, job_id,
        spreadsheet_id: str,
        tab_name: str,
        config: Dict[str, List[str]]
    ) -> Dict[str, List[int]]:
        """Thread-safe target finding with retry logic"""
        try:
            start_row = config.get('start_row', 8)
            column = config.get('column', 'B')
            targets = config['targets']

            result = self._service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab_name}'!{column}{start_row}:{column}",
                majorDimension="COLUMNS"
            ).execute()
            
            values = result.get('values', [[]])
            column_data = values[0] if values else []
            
            return {
                target: [
                    start_row + idx
                    for idx, val in enumerate(column_data)
                    if target.lower() in str(val).lower().strip()
                ]
                for target in targets
            }

        except HttpError as e:
            if e.resp.status == 401:  # Unauthorized
                log(job_id, "Reinitializing service after auth error...")
                self._initialize_google_sheets_service(job_id)
                return self._safe_find_targets(job_id, spreadsheet_id, tab_name, config)
            log(job_id, f"API Error in {tab_name}: {e}")
            return {target: [] for target in config['targets']}
        except Exception as e:
            log(job_id, f"Unexpected error in {tab_name}: {e}")
            return {target: [] for target in config['targets']}

    def batch_find_targets(
        self, job_id,
        spreadsheet_id: str,
        tab_configs: Dict[str, Dict[str, List[str]]],
        max_workers: int = 3
    ) -> Dict[str, Dict[str, List[int]]]:
        """Batch search with connection recovery"""
        if not self._service and not self._initialize_google_sheets_service(job_id):
            return {}
        
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    job_id,
                    self._safe_find_targets,
                    spreadsheet_id,
                    tab_name,
                    config
                ): tab_name
                for tab_name, config in tab_configs.items()
            }

            for future in concurrent.futures.as_completed(futures):
                tab_name = futures[future]
                try:
                    results[tab_name] = future.result()
                except Exception as e:
                    log(job_id, f"Error processing {tab_name}: {e}")
                    results[tab_name] = {"error": str(e)}
        
        return results
    
    def convert_to_object_format(
        self,
        target_rows: Dict[str, List[int]],
        values: Dict[str, List[str]]
    ) -> Dict[str, List[PlatformCell]]:
        """
        Convert old format to new object format
        Args:
            target_rows: {'FACEBOOK PAGE': [9,23,34], ...}
            values: {'FACEBOOK PAGE': ['100','200','300'], ...}
        Returns:
            {'FACEBOOK PAGE': [{row:9, value:'100'}, ...]}
        """

        return {
            platform: [
                {"row": row, "value": value}
                for row, value in zip(rows, values.get(platform, []))
            ]
            for platform, rows in target_rows.items()
            if len(rows) == len(values.get(platform, []))
        }

    def _column_number_to_letter(self, col_num: int) -> str:
        """Convert column number to letter (1->A, 2->B, etc.)"""
        letters = []
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            letters.insert(0, chr(65 + remainder))
        return ''.join(letters)    

    def get_current_month_column(self, job_id, tab_name: str, spreadsheet_id: str) -> Optional[Tuple[int, str]]:
        """Find current month column with error handling"""
        if not self._service and not self._initialize_google_sheets_service(job_id):
            return None

        current_month = datetime.now().strftime('%b').upper()
        
        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab_name}'!2:2",
                majorDimension="ROWS"
            ).execute()
            
            headers = result.get('values', [[]])[0]
            
            for col_idx, header in enumerate(headers, start=1):
                if header.strip().upper() == current_month:
                    return (col_idx, self._column_number_to_letter(col_idx))
            
            log(job_id, f"Current month ({current_month}) column not found")
            return None
        except HttpError as error:
            log(job_id, f"API Error: {error}")
            return None

    def update_platform_cells(
        self, job_id,
        spreadsheet_id: str,
        tab_name: str,
        platform_cells: Dict[str, List[PlatformCell]],  # {'FACEBOOK PAGE': [{row:9, value:'100'}, ...]}
        search_column: str = "B"
    ) -> Dict[str, bool]:
        """
        Updates platforms using object format
        Args:
            platform_cells: {'FACEBOOK PAGE': [{row:9, value:'100'}, ...]}
            search_column: Column where platform names are found
        Returns:
            Dictionary of {platform: success_status}
        """
        if not self._service and not self._initialize_google_sheets_service(job_id):
            return {platform: False for platform in platform_cells}

        try:
            # 1. Get current month column
            month_col = self.get_current_month_column(job_id, tab_name, spreadsheet_id)
            if not month_col:
                print("⚠️ Failed to detect current month column")
                return {platform: False for platform in platform_cells}
            month_letter = month_col[1]

            # 2. Prepare batch update
            requests = []
            results = {}

            for platform, cells in platform_cells.items():
                for cell in cells:
                    requests.append({
                        'range': f"'{tab_name}'!{month_letter}{cell['row']}",
                        'values': [[cell['value']]]
                    })
                results[platform] = True

            # 3. Execute update
            if requests:
                self._service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'valueInputOption': 'USER_ENTERED', 'data': requests}
                ).execute()
                log(job_id, f"✅ Updated {len(requests)} cells for {len(platform_cells)} platforms")
            else:
                log(job_id, "⚠️ No valid cells to update")

            return results
        except Exception as e:
            log(job_id, f"❌ Update failed: {e}")
            return {platform: False for platform in platform_cells}
        
