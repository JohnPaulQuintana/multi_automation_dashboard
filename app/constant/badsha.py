from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
TODAY = datetime.now().date()

YESTERDAY = TODAY - timedelta(days=1)
YESTERDAY_DATE = YESTERDAY.strftime("%d-%m-%Y")   # 07/11/2001
TODAY_DATE  = TODAY.strftime("%d-%m-%Y")    # 07/11/2001
TIME = "12:00:00"


DAILY_BO_BADSHA_RANGE = {
    "NSU": "NSU DATA",
    "FTD": "FTD DATA",
    "DEPOSIT": "DEPOSIT",
    "WITHDRAWAL": "WITHDRAWAL",
    "VT/APL/TPL": "VT/APL/TPL",
    "CODE": "CODE",
    "AFFILIATE": "AFFILIATE",
    "BOBADSHA": "BOBADSHA",
    "AFFIBO": "AFFIBO"
}

url = "https://ag.badsha.live/index.jsp"
url_login_api_url = "https://ag.badsha.live/auth/agent/login"
fund_url = "https://ag.badsha.live/service/agent/creditAllocatedLog"
win_loss_url = "https://ag.badsha.live/page/agent/report/winLossProductSetting.jsp"