import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from playwright.sync_api import sync_playwright

# Scopes required for YouTube Data + Analytics
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]



# Path to Chrome executable
chrome_path = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"

# Use the top-level User Data directory (not the Profile subfolder!)
user_data_dir = "C:\\Users\\crenz\\AppData\\Local\\Google\\Chrome\\User Data"

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=user_data_dir,
        headless=False,
        executable_path=chrome_path,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--profile-directory=Profile 1",
            "--window-size=1200,800",
            "--start-maximized"
        ]
    )

    page = context.new_page()
    page.goto("https://youtube.com")  # or wherever you extract the token

    input("Press Enter to close...")
    context.close()
