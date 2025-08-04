from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
YESTERDAY = (datetime.now() - timedelta(days=1)).date()
START_DATE = YESTERDAY.strftime("%m/%d/%Y")   # 08/04/2025
END_DATE = datetime.now().strftime("%m/%d/%Y")


TRACKER_RANGE = {
    "USER": "USER!A2:H2",
    "BAJI": "BAJI!A1:J2",
    "6S": "6S!A2:J2",
    "JB": "JB!A2:J2",
    # "CITINOW": "SocialMedia!D1:D",
}