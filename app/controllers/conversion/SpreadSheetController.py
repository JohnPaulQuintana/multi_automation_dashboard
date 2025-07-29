from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN


class SpreadsheetController:
    def __init__(self, spreadsheet, range=None):
        self.spreadsheet = spreadsheet
        self.range = range if range else "BAJI!A3:A"

    def get_keywords(self):
        print("Fetching accounts from spreadsheet...")
        
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
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range=self.range).execute()
            rows = result.get("values", [])# list[list[str]]
            # ────────── flatten and strip blanks ──────────
            keywords = [row[0].strip() for row in rows if row and row[0].strip()]
            print(f"Found {len(keywords)} keywords in range {self.range}.")
            return keywords    
            
        except HttpError as err:
            print(f"An error occurred: {err}")
            return []


