# spreadsheet_controller.py
from typing import List, Any
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN


class SpreadsheetController:
    """Light wrapper around Google Sheets v4 API."""

    _ID_REGEX = re.compile(r"/d/([a-zA-Z0-9-_]+)")

    # ─────────────────── constructor ────────────────────
    def __init__(self, *, spreadsheet: str, tab: str, type: str = "SocialMedia"):
        self.spreadsheet_id = self._extract_id(spreadsheet)

        # strip spaces + outer quotes so 'BAJI' → BAJI
        clean_tab = tab.strip().strip("'\"")
        self.tab_name = clean_tab

        self.type = type
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

        creds = Credentials.from_service_account_info(
            config_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self.svc = build("sheets", "v4", credentials=creds)

    # ────────────────── private helpers ──────────────────
    @classmethod
    def _extract_id(cls, link_or_id: str) -> str:
        if "/" not in link_or_id:
            return link_or_id.strip()
        m = cls._ID_REGEX.search(link_or_id)
        if not m:
            raise ValueError("Could not parse spreadsheet ID from URL.")
        return m.group(1)

    def _true_last_row(self) -> int:
        """
        Return 1‑based index of the first empty row in column A,
        ignoring gaps inside the column.
        """
        col = (
            self.svc.spreadsheets()
            .values()
            .get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{self.tab_name}'!A:A",
                majorDimension="COLUMNS",
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
            .get("values", [[]])[0]
        )
        return len(col) + 1

    # ────────────────── public methods ───────────────────
    def append_rows_return_last(
        self,
        rows: List[List[Any]],
        start_cell: str = "A1",           # kept for API parity, not used
        value_input_option: str = "USER_ENTERED",
        debug: bool = False,
    ) -> int:
        """Append rows and return the final row index."""
        if not rows:
            raise ValueError("Nothing to write")

        # find real bottom of the sheet
        row_start = self._true_last_row()
        range_a1 = f"'{self.tab_name}'!A{row_start}"

        if debug:
            print("[debug] appending to", range_a1)

        try:
            resp = (
                self.svc.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_a1,
                    valueInputOption=value_input_option,
                    insertDataOption="INSERT_ROWS",
                    includeValuesInResponse=debug,
                    body={"values": rows},
                )
                .execute()
            )
        except HttpError as err:
            raise RuntimeError(f"Sheets append error: {err}") from err

        updated_range = resp["updates"]["updatedRange"]
        last_row_after = int(re.search(r"[A-Z]+(\d+):", updated_range).group(1))

        if debug:
            print("[debug] updatedRange →", updated_range)

        return last_row_after
