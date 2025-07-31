# app/config/config.py
from dotenv import load_dotenv
import os

# Load .env once at startup
load_dotenv()

# You can also define defaults here if needed
APP_NAME = os.getenv("APP_NAME", "Multi Automation Dashboard")

#Conversion Automation
USERNAME           = os.getenv("BO_USERNAME", "")
PASSWORD           = os.getenv("BO_PASSWORD", "")
SOCIAL_SHEET_ID    = os.getenv("SOCIALMEDIA_SHEET", "")
AFFILIATE_SHEET_ID = os.getenv("AFFILIATE_SHEET", "")


#Business Process Automation
SS_USERNAME = os.getenv("SS_USERNAME", "")
SS_PASSWORD = os.getenv("SS_PASSWORD", "")

TYPE = os.getenv("TYPE")
PROJECT_ID = os.getenv("PROJECT_ID")
PRIVATE_KEY_ID = os.getenv("PRIVATE_KEY_ID")
PRIVATE_KEY = os.getenv("PRIVATE_KEY").replace("\\n", "\n")  # Handle newline characters
CLIENT_EMAIL = os.getenv("CLIENT_EMAIL")
CLIENT_ID = os.getenv("CLIENT_ID")
AUTH_URI = os.getenv("AUTH_URI")
TOKEN_URI = os.getenv("TOKEN_URI")
AUTH_PROVIDER_X509_CERT_URL = os.getenv("AUTH_PROVIDER_X509_CERT_URL")
CLIENT_X509_CERT_URL = os.getenv("CLIENT_X509_CERT_URL")
UNIVERSE_DOMAIN = os.getenv("UNIVERSE_DOMAIN")