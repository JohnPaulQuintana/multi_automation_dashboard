import requests
from playwright.sync_api import sync_playwright
import hashlib
from bs4 import BeautifulSoup
import time
def transfer_cookies_to_playwright(session, context):
    playwright_cookies = []
    for cookie in session.cookies:
        playwright_cookies.append({
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain,
            "path": cookie.path,
            "httpOnly": cookie.has_nonstandard_attr("HttpOnly"),
            "secure": cookie.has_nonstandard_attr("Secure"),
        })
    context.add_cookies(playwright_cookies)

def scrape_win_with_playwright(session):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        # transfer requests cookies → playwright
        transfer_cookies_to_playwright(session, context)

        page = context.new_page()
        page.goto("https://ag.badsha.live/page/agent/report/winLossProductSetting.jsp")
        startDate = "18-09-2025"
        endDate = "18-09-2025"
        timeGrain = "12:00:00"
        max_retries = 3
        retries = 0
        all_results = []
        while retries < max_retries:
            try:
                page.evaluate(
                    """(date) => {
                        let el = document.querySelector('#startDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    startDate
                )
                time.sleep(.5)
                # Set endDate
                page.evaluate(
                    """(date) => {
                        let el = document.querySelector('#endDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    endDate
                )
                time.sleep(.5)


                page.click('#queryReport')

                page.wait_for_selector("#tbodyAgent .trTitle")
                html = page.inner_html("#tbodyAgent")

                soup = BeautifulSoup(html, "html.parser")
                data = []
                for row in soup.select("tr.trTitle"):
                    cols = row.find_all("td")
                    if len(cols) >= 3:
                        data.append({
                            "Product": row.select_one("td span#titleUseID").get_text(strip=True) if row.select_one("td span#titleUseID") else "",
                            "Valid Turnover": row.select_one("td#userTotalPlTurnover").get_text(strip=True) if row.select_one("td#userTotalPlTurnover") else "",
                            "Active Player": row.select_one("td span#userTotalActivePlayer").get_text(strip=True) if row.select_one("td span#userTotalActivePlayer") else "",
                            "Win/loss": row.select_one("td#userTotaldownlineWinloss").get_text(strip=True) if row.select_one("td#userTotaldownlineWinloss") else "",
                            "Jackpot Win/Loss": row.select_one("td#userTotaldownlineJackpot").get_text(strip=True) if row.select_one("td#userTotaldownlineJackpot") else "",
                            "Member Comm.": row.select_one("td#userTotaldownlineComm").get_text(strip=True) if row.select_one("td#userTotaldownlineComm") else "",
                            "Total P/L": row.select_one("td#userTotaldownlineProfitloss").get_text(strip=True) if row.select_one("td#userTotaldownlineProfitloss") else "",
                            "PT Win/Loss (Self)": row.select_one("td#userTotalselfWinloss").get_text(strip=True) if row.select_one("td#userTotalselfWinloss") else "",
                            "Self Comm.": row.select_one("td#userTotalselfComm").get_text(strip=True) if row.select_one("td#userTotalselfComm") else "",
                            "Total P/L (Self)": row.select_one("td#userTotalselfProfitloss").get_text(strip=True) if row.select_one("td#userTotalselfProfitloss") else "",
                        })

                all_results.extend(data)
                print(all_results)
                break  #

            except Exception as e:
                retries += 1
                print(f"Retry {retries}/{max_retries} failed for date {startDate}: {e}")
                if retries >= max_retries:
                    print(f"Failed to scrape data for {startDate} after several attempts.")
                    break

        return all_results

def authenticate():
    url = "https://ag.badsha.live/index.jsp"
    url_login_api = "https://ag.badsha.live/auth/agent/login"
    fund = "https://ag.badsha.live/service/agent/creditAllocatedLog"
    win = "https://ag.badsha.live/service/agent/transaction/winloss"
    winloss_page = "https://ag.badsha.live/page/agent/report/creditAllocatedLog.jsp"
    cookies = None

    session = requests.Session()
    resp = session.get(url, timeout=10)
    resp.raise_for_status() 

    soup = BeautifulSoup(resp.text, "html.parser")
    session_key_tag = soup.find("input", {"id": "sessionKey"})
    if session_key_tag is None:
        raise RuntimeError("❌ sessionKey input not found on login page.")

    session_key = session_key_tag["value"]
    print("Session key:", session_key)

    raw_password = "Abcd1234"
    hashed_once = hashlib.sha1(raw_password.encode()).hexdigest()
    final_password = hashlib.sha1((hashed_once + session_key).encode()).hexdigest()
    print("Final password:", final_password)

    payload = {
        "userID": "subbida",
        "password": final_password,
        "timeZone": "-480",   # or your timezone offset
        "rememberMe": "false",
        "deviceId": "3df819dfe9ae33784ad3c0a27e43f23e"
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://ag.badsha.live",
        "Referer": "https://ag.badsha.live/index.jsp",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    auth_response = session.post(url_login_api, data=payload, headers=headers)

    

    if auth_response.status_code == 200:
        print("✅ Login successful")
        cookies = session.cookies.get_dict()
        print("Authentication successful. Cookies captured.")

        params = {
            "timeZone": "GMT+8:00",
            "startDate": "02-09-2025 12:00:00",
            "endDate": "03-09-2025 12:00:00",
            "userType": "PLAYER",
            "userID": "",
            "remark": "",
            "currency": "",
            "pageNumber": 1,
            "pageSize": 500,
            "creditAllocatedType": "DEPOSIT",
            "isShowSystemLog": "false",
            "tagId": -1,
            "tagUserType": 1,
            "tagUserLevel": ""
        }
        data_response = session.get(fund, params=params, cookies=cookies)

        data = data_response.json()
        page_info = data.get("pageInfo", {})
        result = page_info.get("deposit", 0)
        print(result)
        # payloads = {
        #     "startDate": "18-09-2025 12:00:00",
        #     "endDate": "19-09-2025 12:00:00",
        #     "location": "",
        #     "platform": "BTC,CRICKET,SABASPORTS,EVOLUTION,EZUGI,HRG,MGLIVE,PPCASINO,SA,SEXYBCRT,SVCASINO,WINFINITY,ACEWIN,ADVANTPLAY,AMEBA,BNG,BTG,BUFFALO,EVOPLAY,FACHAI,FUNTA,HACKSAW,IDEALGAMING,ILOVEU,JDB,JILI,KINGMAKER,KINGMIDAS,MGGAMING,MIMI,NETENT,NOLIMITCITY,OCTOPLAY,POCKET,PLAYNGO,PLAYSTAR,PP,REDTIGER,RELAX,SPADE,SPRIBE,TURBOGAMES,WMSLOT,YB,YGG,OTHER",      # sometimes required
        #     "userID": "", 
        #     "isInternal": 0,     # <-- depends on the site
        #     "searchId": "", 
        #     "tagId": -1,     # <-- depends on the site
        #     "tagUserType": "",
        #     "tagUserLevel": "",  
        #     "affiliateGroup": ""
        # }

        # fund_resp = session.post(win, data=payloads, cookies=cookies)

        # html_response = fund_resp.text
        # soup = BeautifulSoup(html_response, "html.parser")
        # print("Winloss Response <td> values:")
        # for i, td in enumerate(soup.find_all("td"), start=1):
        #     text = td.get_text(strip=True)
        #     if text:
        #         print(f"{i:02d}. {text}")

        return session
    else:
        print("❌ Login failed", auth_response.status_code, auth_response.text[:500])
        return None

# Test
cookies = authenticate()

if cookies:
    scrape_win_with_playwright(cookies)
