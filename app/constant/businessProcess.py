from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
TODAY = datetime.now().date()

YESTERDAY = TODAY - timedelta(days=1)
YESTERDAY_DATE = YESTERDAY.strftime("%d-%m-%Y")   # 07/11/2001
TODAY_DATE  = TODAY.strftime("%d-%m-%Y")    # 07/11/2001
TIME = "12:00:00"

WINBDT_URL = [
    "https://ag.winbdt.co/index.jsp",
    "https://ag.winbdt.co/auth/agent/login",
    "https://ag.winbdt.co/service/agent/userActionLog",
    "https://ag.winbdt.co/service/agent/creditAllocatedLog",
    "https://ag.winbdt.co/page/agent/report/winLossDetailSetting.jsp",
    "https://ag.winbdt.co/page/agent/report/winLossProductSetting.jsp"
]

WINBDT_RANGE = {
    "SUMMARY": "SUMMARY",
    "AccountCreation": "Account Creation",
    "DepositWithdrawal": "Deposit and Withdrawal (Data)",
    "OverallPerformance": "Overall Performance (Data)",
    "ProviderPerformance": "Provider Performance (Data)"
}




BADSHA_RANGE = {
    "SUMMARY": "SUMMARY",
    "AccountCreation": "Account Creation",
    "DepositWithdrawal": "Deposit and Withdrawal (Data)",
    "OverallPerformance": "Overall Performance (Data)",
    "ProviderPerformance": "Provider Performance (Data)"   
}
BADSHA_URL = [
    "https://ag.badsha.live/index.jsp",
    "https://ag.badsha.live/auth/agent/login",
    "https://ag.badsha.live/service/agent/userActionLog",
    "https://ag.badsha.live/service/agent/creditAllocatedLog",
    "https://ag.badsha.live/page/agent/report/winLossDetailSetting.jsp",
    "https://ag.badsha.live/page/agent/report/winLossProductSetting.jsp"
]