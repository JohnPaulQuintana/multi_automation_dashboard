from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN


class SpreadsheetController:
    def __init__(self, spreadsheet, range):
        self.spreadsheet = spreadsheet
        self.range = range

    def get_account(self):
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
            result = sheet.values().get(
                spreadsheetId=self.spreadsheet, 
                range=self.range
            ).execute()
            rows = result.get("values", [])

            running_accounts = []
            # skip_ids = {"cxs6shabbdt"}          
            for row in rows:
                if len(row) >= 7 and row[6].strip().lower() == "running":
                    # account_id = row[1].strip()  # adjust index if ID is in another column
                    # if account_id in skip_ids:
                    #     print(f"Skipping account: {account_id}")
                    #     continue
                    acount_info = row[:8]
                    running_accounts.append(acount_info)
                    # print("Running Account:", acount_info)
                    # if len(running_accounts) == 3:  # Stop after 3 running accounts
                    #     break

            print(f"Found {len(running_accounts)} running accounts.")
            # print("Running Account: ", running_accounts)
            return running_accounts

        except HttpError as err:
            print(f"An error occurred: {err}")
            return []
