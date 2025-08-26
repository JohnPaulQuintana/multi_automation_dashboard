from config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, List, TypedDict

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
    
    def _initialize_google_sheets_service(self, retries: int = 3) -> bool:
        """Initialize Google Sheets service with SSL context and retry logic"""
        print("Initializing Google Sheets service with secure connection...")
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
            
            print("Google Sheets service initialized successfully")
            return True
            
        except ssl.SSLError as e:
            print(f"SSL Error: {e}")
            if retries > 0:
                print(f"Retrying... ({retries} attempts remaining)")
                return self._initialize_google_sheets_service(retries - 1)
            raise ConnectionError("Failed to establish secure connection after retries")
        except Exception as e:
            print(f"Initialization Error: {e}")
            raise