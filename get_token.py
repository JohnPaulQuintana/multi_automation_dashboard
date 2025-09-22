import pickle
from google_auth_oauthlib.flow import InstalledAppFlow

# Scopes required for YouTube Data + Analytics
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly"
]

def save_token_for_account(client_secret_path: str, token_output_path: str):
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    # not returning fresh token
    # creds = flow.run_local_server(port=8080)

    creds = flow.run_local_server(
    port=8080,
    access_type='offline',
    prompt='consent'
)

    with open(token_output_path, 'wb') as f:
        pickle.dump(creds, f)
    print(f"✅ Token saved to: {token_output_path}")

# Example: Run for all YouTube account
# client is baji npr email:
save_token_for_account(
    "app/config/shared_secret.json", 
   "app/controllers/media/youtube/tokens/token_badsha_bdt.pkl"
)