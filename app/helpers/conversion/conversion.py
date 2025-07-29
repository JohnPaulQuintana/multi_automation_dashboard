# ────────────────────── HELPERS ──────────────────────────
from typing import List, Dict, Any

def build_social_row(rec, sheet_date):
    return [
        sheet_date, "", rec["affiliate_username"], rec["currency"],
        rec["player_username"], rec["total_deposit"], rec["total_withdrawal"],
        rec["total_number_of_bets"], rec["total_turnover"],
        rec["total_profit_and_loss"], rec["total_bonus"]
    ]

def build_affiliate_row(rec, sheet_date):
    return [
        sheet_date, "", rec["affiliate_username"], rec["currency"],
        rec["registered_users"], rec["number_of_fd"], rec["first_deposit"],
        rec["active_player"], rec["total_deposit"],
        rec.get("total_withdrawal", ""), rec.get("total_turnover", ""),
        rec.get("total_profit_and_loss", ""), rec.get("total_bonus", ""),
    ]

def build_affiliate_row_socmed(rec, sheet_date):
    return [
        sheet_date, "", rec["affiliate_username"], rec["currency"],
        rec["registered_users"], rec["number_of_fd"], rec["first_deposit"],
        rec["active_player"]
    ]

_socmedlinks = {
        "BAJI": [
            "https://bjabo8888.com/page/manager/login.jsp",
            "https://bjabo8888.com/login/manager/managerController/login",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "SIX6S": [
            "https://666666bo.com/page/manager/login.jsp",
            "https://666666bo.com/login/manager/managerController/login",
            "https://666666bo.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://666666bo.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "JEETBUZZ": [
            "https://jbbo8888.com/page/manager/login.jsp",
            "https://jbbo8888.com/login/manager/managerController/login",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "CITINOW": [
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://ctncps.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
    }
_afflinks = {
        "BAJI": [
            "https://bjabo8888.com/page/manager/login.jsp",
            "https://bjabo8888.com/login/manager/managerController/login",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "SIX6S": [
            "https://666666bo.com/page/manager/login.jsp",
            "https://666666bo.com/login/manager/managerController/login",
            "https://666666bo.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "JEETBUZZ": [
            "https://jbbo8888.com/page/manager/login.jsp",
            "https://jbbo8888.com/login/manager/managerController/login",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "CITINOW": [
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
    }


def filter_rows_affiliate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # <raw API key> -> <friendly column name>
    DEFAULT_FIELD_MAP: Dict[str, str] = {
            "affiliateName":          "affiliate_username",
            "affiliateCurrency":      "currency",
            "registerCount":          "registered_users",
            "firstDepositCount":      "number_of_fd",
            "firstDeposit":           "first_deposit",
            "activePlayer":           "active_player",
            "deposit":                "total_deposit",
            "withdrawal":             "total_withdrawal",
            "turnover":               "total_turnover",
            "profit":                 "total_profit_and_loss",
            "bonus":                  "total_bonus",
        }
    cleaned_rows: list[dict] = []
    for row in rows:
        cleaned_rows.append({
            pretty: row.get(raw)                # raw key comes from JSON
            for raw, pretty in DEFAULT_FIELD_MAP.items()
        })
    return cleaned_rows


def filter_rows_player(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    DEFAULT_FIELD_MAP: Dict[str, str] = {
        "affiliateName":          "affiliate_username",
        "affiliateCurrency":      "currency",
        "player":                 "player_username",
        "deposit":                "total_deposit",
        "withdrawal":             "total_withdrawal",
        "betCount":               "total_number_of_bets",
        "turnover":               "total_turnover",
        "profit":                 "total_profit_and_loss",
        "bonus":                  "total_bonus",
    }
    cleaned_rows: list[dict] = []
    for row in rows:
        cleaned_rows.append({
            pretty: row.get(raw)                # raw key comes from JSON
            for raw, pretty in DEFAULT_FIELD_MAP.items()
        })
    return cleaned_rows


def filter_rows_affilliate_socmed(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    DEFAULT_FIELD_MAP: Dict[str, str] = {
        "affiliateName":          "affiliate_username",
        "affiliateCurrency":      "currency",
        "registerCount":          "registered_users",
        "firstDepositCount":      "number_of_fd",
        "firstDeposit":           "first_deposit",
        "activePlayer":           "active_player",
    }
    cleaned_rows: list[dict] = []
    for row in rows:
        cleaned_rows.append({
            pretty: row.get(raw)                # raw key comes from JSON
            for raw, pretty in DEFAULT_FIELD_MAP.items()
        })
    return cleaned_rows

