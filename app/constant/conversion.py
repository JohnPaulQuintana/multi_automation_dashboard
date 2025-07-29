from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
YESTERDAY        = (datetime.now() - timedelta(days=1)).date()
TARGET_DATE = YESTERDAY.strftime("%Y/%m/%d")   # 2025/07/14
SHEET_DATE  = YESTERDAY.strftime("%d/%m/%Y")   # 14/07/2025

SOCIAL_RANGES = {
    "BAJI": "SocialMedia!A1:A",
    "SIX6S": "SocialMedia!B1:B",
    "JEETBUZZ": "SocialMedia!C1:C",
    # "CITINOW": "SocialMedia!D1:D",
}
AFFILIATE_RANGES = {
    "BAJI": "Acquisition!A1:A",
    "SIX6S": "Acquisition!B1:B",
    "JEETBUZZ": "Acquisition!C1:C",
    # "CITINOW": "Acquisition!D1:D",
}
