# app/config/config.py
from dotenv import load_dotenv
import os

# Load .env once at startup
load_dotenv()

# You can also define defaults here if needed
APP_NAME = os.getenv("APP_NAME", "Multi Automation Dashboard")

#===========================================================================

#Conversion Automation
USERNAME           = os.getenv("BO_USERNAME", "")
PASSWORD           = os.getenv("BO_PASSWORD", "")
SOCIAL_SHEET_ID    = os.getenv("SOCIALMEDIA_SHEET", "")
AFFILIATE_SHEET_ID = os.getenv("AFFILIATE_SHEET", "")

#===========================================================================

#Business Process Automation
SS_USERNAME = os.getenv("SS_USERNAME", "")
SS_PASSWORD = os.getenv("SS_PASSWORD", "")

#===========================================================================

# NSU AND FTD Tracker Automation
NSU_FTD_TRACKER_SHEET = os.getenv("NSU_FTD_TRACKER_SHEET")

#===========================================================================

# Badsha Report Automation
BR_USERNAME = os.getenv("BR_USERNAME", "")
BR_PASSWORD = os.getenv("BR_PASSWORD", "")
DAILY_BO_BADSHA = os.getenv("DAILY_BO_BADSHA", "")

#===========================================================================


# WinBDT Process Automation
WB_USERNAME = os.getenv("WB_USERNAME", "")
WB_PASSWORD = os.getenv("WB_PASSWORD", "")
WB_DRIVE = os.getenv("WB_DRIVE", "")
WB_WINBDT_SHEET = os.getenv("WB_WINBDT_SHEET", "")
WB_DAILY = os.getenv("WB_DAILY", "")
WB_WEEKLY = os.getenv("WB_WEEKLY", "")
WB_MONTHLY = os.getenv("WB_MONTHLY", "")

#===========================================================================

# Badsha Process Automation
BP_USERNAME = os.getenv("BP_USERNAME", "")
BP_PASSWORD = os.getenv("BP_PASSWORD", "")
BP_DRIVE = os.getenv("BP_DRIVE", "")
BP_WINBDT_SHEET = os.getenv("BP_WINBDT_SHEET", "")
BP_DAILY = os.getenv("BP_DAILY", "")
BP_WEEKLY = os.getenv("BP_WEEKLY", "")
BP_MONTHLY = os.getenv("BP_MONTHLY", "")

#===========================================================================

# Social Media Automation
# Variables for the Social Media
ACCOUNT_SHEET_ID = os.getenv("ACCOUNT_SHEET_ID", "")
FB_GAINED_SHEET_ID = os.getenv("FB_GAINED_SHEET_ID", "")
IG_GAINED_SHEET_ID = os.getenv("IG_GAINED_SHEET_ID", "")
YT_GAINED_SHEET_ID = os.getenv("YT_GAINED_SHEET_ID", "")
TW_GAINED_SHEET_ID = os.getenv("TW_GAINED_SHEET_ID", "")
CLIENT_SHEET_ID = os.getenv("CLIENT_SHEET_ID", "")

FACEBOOK_BASE_API_URL = os.getenv("FACEBOOK_BASE_API_URL", "")
YOUTUBE_BASE_API_URL = os.getenv("YOUTUBE_BASE_API_URL", "")
TWITTER_BASE_API_URL = os.getenv("TWITTER_BASE_API_URL", "")

# Variabless For POST SETUP
SINCE = os.getenv("SINCE", "")
UNTIL = os.getenv("UNTIL", "")

#For Testing purposes only
SPREADSHEET_RANGE = os.getenv("SPREADSHEET_RANGE", "")

# FOR BADSHA ONLY
RAJI_ACCOUNT = os.getenv("RAJI_ACCOUNT", "")

#===========================================================================

# Service Account Initilization
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

# Google Drive Account
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
OAUTH_PROJECT_ID = os.getenv("OAUTH_PROJECT_ID")
OAUTH_AUTH_URI = os.getenv("OAUTH_AUTH_URI")
OAUTH_TOKEN_URI = os.getenv("OAUTH_TOKEN_URI")
OAUTH_CERT_URL = os.getenv("OAUTH_CERT_URL")
OAUTH_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI")