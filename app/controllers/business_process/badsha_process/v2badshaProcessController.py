from app.constant.businessProcess import BADSHA_URL
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timedelta
from urllib.parse import urlparse
from app.automations.log.state import log
from collections import defaultdict
from bs4 import BeautifulSoup
import requests
import hashlib
import json
import re
import time

class badshaProcessController():
    def __init__(self, username, password, url, startDate, endDate, timeGrain):
        self.username = username
        self.password = password
        self.startDate = startDate
        self.endDate = endDate
        self.timeRange = "12:00:00"
        self.timeGrain = timeGrain
        self.max_retries = 3

        self.cookies = None
        self.session = requests.Session()
    
    def wait_for_navigation(self, page, job_id):
        try:
            page.wait_for_load_state('networkidle')
            return True
        except TimeoutError:
            log(job_id, "Navigation wait timeout.")
            return False

    # --- Helper function to run scraping with given date range ---
    def run_scrape_range(self, cookies, job_id, start_date, end_date):
        retries = 0
        while retries < self.max_retries:
            try:
                
                params = {
                    "userActionType": "CREATE_ACCOUNT",
                    "userID": "",
                    "updatorUserId": "",
                    "updatorIp": "",
                    "startDate": f"{start_date} {self.timeRange}",
                    "endDate": f"{end_date} {self.timeRange}",
                    "pageSize": 1000,
                    "pageNumber": 1
                }

                data_response = self.session.get(BADSHA_URL[2], params=params, cookies=cookies)
                data = data_response.json()
                page_info = data.get("pageInfo", {})
                result = page_info.get("totalCount", 0)
                log(job_id, f"Total of Data: {result}")

                log(job_id, f"📌 Finished Range {start_date} → {end_date}, Count: {result}")
                return result

            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")
                log(job_id, "Retrying...")

        return []
    
    def account_creation(self, cookies, job_id):
        log(job_id, "Navigating on Account Creation")

        # all_results = []
        
        if self.timeGrain.lower() in ["month", "monthly"]:
            start_dt = datetime.strptime(self.startDate, "%d-%m-%Y")
            # First half (15 days max)
            mid_dt = start_dt + timedelta(days=14)
            mid_str = mid_dt.strftime("%d-%m-%Y")

            log(job_id, f"Monthly Mode: Splitting into two parts")
            part1 = self.run_scrape_range(cookies, job_id, self.startDate, mid_str)
            # all_results.extend(part1)

            # Second half (rest)

            next_start = mid_dt.strftime("%d-%m-%Y")
            part2 = self.run_scrape_range(cookies, job_id, next_start, self.endDate)
            # all_results.extend(part2)

            all_results = part1 + part2

        else:
            # Normal (no split needed)
            all_results = self.run_scrape_range(cookies, job_id, self.startDate, self.endDate)

        # page.click("div.popup-XL a.close")
        return all_results 
        
    def deposit_withdrawal_Data(self, data, index):
        # Format creditAllocatedType
        credit_type = data.get("creditAllocatedType", "")
        if isinstance(credit_type, str):
            # Special cases mapping
            mapping = {
                "REWARD_TURNOVER_AMOUNT": "Reward by Turnover Amount",
                "BONUS": "Promotion Bonus",
                "RESCUE_BONUS": "Rescue Bonus",
                "COMM_BONUS": "Commission Bonus",
                "PROMOTION_MAX_WIN": "Max win",
                "SIGNUP_REBATE": "Signup Rebate",
                "SHARE_PROFIT": "Earned profit",
                "SIGNUP_REBATE": "Signup Rebate",
            }
            credit_type = mapping.get(credit_type, credit_type.capitalize())

        # Check if first deposit
        if data.get("isFirstDeposit", False) and data.get("creditAllocatedType") == "DEPOSIT":
            plus_minus = f"First Deposit{data.get('diff')}"
        else:
            plus_minus = f"{credit_type}{data.get('diff')}"

        return {
            "#": index + 1,
            "Login Name": f"{data.get('loginName', '')} ID:{data.get('userId', '')}".strip(),
            "Name": f"{data.get('parentUserId', '')} {data.get('name', '')}".strip(),
            "+/-": plus_minus,
            "Balance": f"{data.get('oldBalance', '')} → {data.get('newBalance', '')}".strip(),
            "Date": f"{data.get('allocatedDate')} (GMT+8:00)",
            "Exec User": data.get("execUserId"),
            "Remarks": data.get("remark"),
        }
        
    def deposit_withdrawal(self, cookies, job_id, types=None):
        try:
            log(job_id, "Scraping For Deposit and Withdrawal Data")
            all_results = []
            index = 0
            for type_val in types:
                retries = 0
                while retries < self.max_retries:
                    try:
                        log(job_id, f"Changing The Selection to {type_val}")
                        page_number = 1
                        page_size = 1000 
                        while True:
                            params = {
                                "timeZone": "GMT+8:00",
                                "startDate": f"{self.startDate} {self.timeRange}",
                                "endDate": f"{self.endDate} {self.timeRange}",
                                "userType": "PLAYER",
                                "userID": "",
                                "remark": "",
                                "currency": "",
                                "pageNumber": page_number,
                                "pageSize": page_size,
                                "creditAllocatedType": type_val,
                                "isShowSystemLog": "false",
                                "tagId": -1,
                                "tagUserType": 1,
                                "tagUserLevel": ""
                            }
                            data_response = self.session.get(BADSHA_URL[3], params=params, cookies=cookies)
                            try:
                                data = data_response.json()
                            except ValueError:
                                log(job_id, f"❌ Failed to parse JSON for {type_val}, page {page_number}")
                                break
                            data_list = data.get("data", [])
                            log(job_id, f"Getting the data for {type_val}, page {page_number} through Network Response")

                            for entry in data_list:
                                result = self.deposit_withdrawal_Data(entry, index)
                                log(job_id, f"{result}")
                                all_results.append(result)
                                index += 1

                            # break when last page
                            if len(data_list) < page_size:
                                break
                            page_number += 1
                        break  # Success, exit retry loop
                    except Exception as e:
                        retries += 1
                        log(job_id, f"⚠️ Error during scraping {type_val}, attempt {retries}: {e}")
                        if retries == self.max_retries:
                            log(job_id, f"❌ Max retries reached for {type_val}, skipping.")
                            break  # Break the retry loop for this type
                        time.sleep(5)  # Optional: add delay before retry
                        
            log(job_id, f"✅ Finished all types, Final Global Index Count: {index}")
            return all_results
        except Exception as e:
            log(job_id, f"❌ Critical error: {e}")
            return []  # Return an empty list in case of failure

    def transfer_cookies_to_playwright(self, session, context):
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

    def get_jackpot_value(self, row):
        try:
            # Option 1: More specific selector
            selector = "td[data-type='member']#userTotalPlJackpot span.textRed"
            element = row.query_selector(selector)
            
            if element:
                return element.inner_text().strip()
                
            # Option 2: Fallback selectors
            fallback_selectors = [
                "td#userTotalPlJackpot span.textRed",
                "td#userTotalPlJackpot",
                "[id='userTotalPlJackpot'] span"
            ]
            
            for selector in fallback_selectors:
                element = row.query_selector(selector)
                if element:
                    return element.inner_text().strip()
                    
            return "0"  # Default value when not found
            
        except Exception as e:
            print(f"Error getting jackpot value: {str(e)}")
            return "0"
        
    def get_company_value(page, row):
        try:
            # Primary selector with data-type attribute for specificity
            selector = "td[data-type='member']#userTotalCompany span.textRed"
            element = row.query_selector(selector)
            
            if element:
                return element.inner_text().strip()
                
            # Fallback selectors if primary fails
            fallback_selectors = [
                "td#userTotalCompany span.textRed",
                "td#userTotalCompany",
                "[id='userTotalCompany'] span"
            ]
            
            for selector in fallback_selectors:
                element = row.query_selector(selector)
                if element:
                    return element.inner_text().strip()
                    
            return "0"  # Default value when not found
            
        except Exception as e:
            print(f"Error getting company value: {str(e)}")
            return "0"

    def overall_performance(self, cookies, job_id):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()

            # transfer requests cookies → playwright
            self.transfer_cookies_to_playwright(self.session, context)

            page = context.new_page()
            page.goto(BADSHA_URL[4])
            retries = 0
            all_results = []
            start_date = datetime.strptime(self.startDate, "%d-%m-%Y").date()
            end_date = datetime.strptime(self.endDate, "%d-%m-%Y").date()
            end_dates = end_date - timedelta(days=1)
            formatted_startDate = start_date.strftime("%d-%m-%Y")
            formatted_endDate = end_dates.strftime("%d-%m-%Y")
            log(job_id, f"Processing date: {formatted_startDate} - {formatted_endDate}")
            while retries < self.max_retries:
                try:
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#startDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_startDate
                    )
                    time.sleep(.5)
                    # Set endDate
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#endDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_endDate
                    )
                    time.sleep(.5)


                    log(job_id, "Inserted Filter")
                    page.click('#queryReport')
                    time.sleep(5)

                    self.wait_for_navigation(page, job_id)
                    log(job_id, "Scraping the HTML....")

                    page.wait_for_selector("#tbodyAgent .trTitle")

                    rows = page.query_selector_all("#tbodyAgent .trTitle")
                    data = []
                    # time.sleep(7.5)
                    # page.wait_for_selector("#tbodyAgent .trTitle", timeout=10000)
                    # html = page.inner_html("#tbodyAgent")

                    # soup = BeautifulSoup(html, "html.parser")
                    # data = []
                    number = 1

                    for row in rows:
                        cols = [cell.inner_text().strip() for cell in row.query_selector_all("td")]
                        if len(cols) >= 3:
                            data.append({
                                # "Date": datetime.strptime(formatted_date, "%d-%m-%Y").strftime("%b %d %Y"),
                                "User ID": row.query_selector("td a#titleUseID").inner_text().strip(),
                                "Name": row.query_selector('td[data-type="name"]').inner_text().strip(),
                                "Valid Turnover": row.query_selector("td#userTotalPlTurnover").inner_text().strip(),
                                "Active Player": row.query_selector("td span#userTotalActivePlayer").inner_text().strip(),
                                "Win/loss": row.query_selector("td div.member span#userTotalPlWinloss").inner_text().strip(),
                                "Jackpot Win/Loss": self.get_jackpot_value(row),
                                "Member Comm.": row.query_selector("td#userTotalPlComm").inner_text().strip(),
                                "Total P/L": row.query_selector("td#userTotalPlProfitloss").inner_text().strip(),
                                "PT Win/Loss": row.query_selector("td#userTotaldownlineWinloss").inner_text().strip(),
                                "Direct Comm.": row.query_selector("td#userTotaldownlineComm").inner_text().strip(),
                                "Total P/L (Direct)": row.query_selector("td#userTotaldownlineProfitloss").inner_text().strip(),
                                "PT Win/Loss (Self)": row.query_selector("td#userTotalselfWinloss").inner_text().strip(),
                                "Self Comm.": row.query_selector("td#userTotalselfComm").inner_text().strip(),
                                "Total P/L (Self)": row.query_selector("td#userTotalselfProfitloss").inner_text().strip(),
                                "Company": self.get_company_value(row)
                            })
                            log(job_id, f"🔥 Done Processing on Row: {number}")
                            number += 1
                            time.sleep(.5) 



                    all_results.extend(data)
                    log(job_id, f"{all_results}")
                    browser.close
                    break  #

                except Exception as e:
                    retries += 1
                    log(job_id, f"Retry {retries}/{self.max_retries} failed for date {self.startDate}: {e}")
                    if retries >= self.max_retries:
                        log(job_id, f"Failed to scrape data for {self.startDate} after several attempts.")
                        break
            log(job_id, "✅ Processed Done")
            return all_results
        
    def provider_performance(self, cookies, job_id):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()

            # transfer requests cookies → playwright
            self.transfer_cookies_to_playwright(self.session, context)

            page = context.new_page()
            page.goto(BADSHA_URL[5])

            max_retries = 3
            retries = 0
            all_results = []
            start_date = datetime.strptime(self.startDate, "%d-%m-%Y").date()
            end_date = datetime.strptime(self.endDate, "%d-%m-%Y").date()
            end_dates = end_date - timedelta(days=1)
            formatted_startDate = start_date.strftime("%d-%m-%Y")
            formatted_endDate = end_dates.strftime("%d-%m-%Y")
            log(job_id, f"Processing date: {formatted_startDate} - {formatted_endDate}")
            while retries < max_retries:
                try:
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#startDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_startDate
                    )
                    time.sleep(.5)
                    # Set endDate
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#endDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_endDate
                    )
                    time.sleep(.5)
                    
                    log(job_id, "Insertion Filter")
                    page.click('#queryReport')
                    time.sleep(5)
                
                    # try:
                    #     with page.expect_response(lambda r: "winLossByProduct" in r.url, timeout=30000):
                    #         page.click('#queryReport')
                    # except TimeoutError:
                    #     retries += 1
                    #     log(job_id, f"Timeout waiting for data response. Retry {retries}/{self.max_retries}")
                    #     continue
                    self.wait_for_navigation(page, job_id)
                    log(job_id, "Scraping the HTML....")
                    # time.sleep(7.5)

                    # page.wait_for_selector("#tbodyAgent .trTitle")
                    # html = page.inner_html("#tbodyAgent")

                    # soup = BeautifulSoup(html, "html.parser")
                    # data = []
                    page.wait_for_selector("#tbodyAgent .trTitle")

                    rows = page.query_selector_all("#tbodyAgent .trTitle")
                    data = []
                    number = 1
                    for row in rows:
                        cols = [cell.inner_text().strip() for cell in row.query_selector_all("td")]
                        if len(cols) >= 3:
                            data.append({
                                # "Date": datetime.strptime(formatted_date, "%d-%m-%Y").strftime("%b %d %Y"),
                                "Product": row.query_selector("td span#titleUseID").inner_text().strip(),
                                # "RTp($)": row.query_selector('td[data-type="name"]').inner_text().strip(),
                                "Valid Turnover": row.query_selector("td#userTotalPlTurnover").inner_text().strip(),
                                "Active Player": row.query_selector("td span#userTotalActivePlayer").inner_text().strip(),
                                "Win/loss": row.query_selector("td#userTotaldownlineWinloss").inner_text().strip(),
                                "Jackpot Win/Loss": row.query_selector("td#userTotaldownlineJackpot").inner_text().strip(),
                                "Member Comm.": row.query_selector("td#userTotaldownlineComm").inner_text().strip(),
                                "Total P/L": row.query_selector("td#userTotaldownlineProfitloss").inner_text().strip(),
                                "PT Win/Loss (Self)": row.query_selector("td#userTotalselfWinloss").inner_text().strip(),
                                "Self Comm.": row.query_selector("td#userTotalselfComm").inner_text().strip(),
                                "Total P/L (Self)": row.query_selector("td#userTotalselfProfitloss").inner_text().strip(),
                                # "Company": self.get_company_value(row)
                            })
                            log(job_id, f"🔥 Done Processing on Row: {number}")
                            number += 1
                            time.sleep(.5)

                    all_results.extend(data)
                    log(job_id, f"{all_results}")
                    break  #

                except Exception as e:
                    retries += 1
                    log(job_id, f"Retry {retries}/{max_retries} failed for date {self.startDate}: {e}")
                    if retries >= max_retries:
                        log(job_id, f"Failed to scrape data for {self.startDate} after several attempts.")
                        break
            log(job_id, "✅ Processed Done")
            return all_results
    
    def run(self, job_id):
        for retry in range(self.max_retries):
            try:
                resp = self.session.get(BADSHA_URL[0], timeout=10)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")
                session_key_tag = soup.find("input", {"id": "sessionKey"})
                if session_key_tag is None:
                    raise RuntimeError("❌ sessionKey input not found on login page.")

                session_key = session_key_tag["value"]
                log(job_id, f"Session key: {session_key}")


                hashed_once = hashlib.sha1(self.password.encode()).hexdigest()
                final_password = hashlib.sha1((hashed_once + session_key).encode()).hexdigest()
                log(job_id, f"Final password: {final_password}")

                payload = {
                    "userID": self.username,
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

                auth_response = self.session.post(BADSHA_URL[1], data=payload, headers=headers)

                if auth_response.status_code == 200:
                    log(job_id, "✅ Login successful")
                    cookies = self.session.cookies.get_dict()
                    log(job_id, "Authentication successful. Cookies captured.")

                    account_creation = self.account_creation(cookies, job_id)
                    log(job_id, "✅ Account Creation Done")
                    time.sleep(1)

                    deposit_withdrawal = self.deposit_withdrawal(
                        cookies,
                        job_id,
                        types=[
                            "DEPOSIT", "WITHDRAW", "PROMOTION_MAX_WIN", "SHARE_PROFIT", 
                            "BONUS", "COMM_BONUS", "RESCUE_BONUS", "REWARD_TURNOVER_AMOUNT", "SIGNUP_REBATE"
                        ]
                    )
                    log(job_id, f"✅ Deposit_Withdrawal {len(deposit_withdrawal)}")
                    time.sleep(1)

                    overall_performance= self.overall_performance(cookies, job_id)
                    log(job_id, "✅ Overall Performance")
                    time.sleep(1)

                    provider_performance= self.provider_performance(cookies, job_id)
                    log(job_id, "✅ Provider Performance")
                    time.sleep(1)

                    data = {
                        "status": 200,
                        "text": "Data Fetched successfully",
                        "title": "Fetch Completed!",
                        "icon": "success",
                        "account_creation": account_creation,
                        "deposit_withdrawal" : deposit_withdrawal,
                        "overall_performance": overall_performance,
                        "provider_performance": provider_performance

                    }
                    return data

            except (PlaywrightTimeoutError, Exception) as e:
                log(job_id, f"Error during site visit on attempt {retry + 1}: {e}")
                if retry < self.max_retries - 1:
                    log(job_id, "Retrying after a short delay...")
                    time.sleep(5)  # Wait before retrying
                else:
                    log(job_id, "Max retries reached. Failing gracefully.")


        
        
