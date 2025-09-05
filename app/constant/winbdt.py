from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
TODAY = datetime.now().date()

YESTERDAY = TODAY - timedelta(days=1)
YESTERDAY_DATE = YESTERDAY.strftime("%d-%m-%Y")   # 07/11/2001
TODAY_DATE  = TODAY.strftime("%d-%m-%Y")    # 07/11/2001
TIME = "12:00:00"


WINBDT_RANGE = {
    "SUMMARY": "SUMMARY",
    "AccountCreation": "Account Creation",
    "DepositWithdrawal": "Deposit and Withdrawal (Data)",
    "OverallPerformance": "Overall Performance (Data)",
    "ProviderPerformance": "Provider Performance (Data)",
    
    "CODE": "CODE",
    "AFFILIATE": "AFFILIATE",
    "BOBADSHA": "BOBADSHA",
    "AFFIBO": "AFFIBO"
}
