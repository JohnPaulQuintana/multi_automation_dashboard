from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from app.constant.businessProcess import WINBDT_URL
from datetime import datetime, timedelta
from urllib.parse import urlparse
from app.automations.log.state import log
from collections import defaultdict
from bs4 import BeautifulSoup
import hashlib
import requests
import json
import re
import time

class winbdtController:
    def __init__(self, username, password, url, startDate, endDate, timeGrain):
        self.username = username
        self.password = password
        self.url = url
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

    def authenticate(self, page, job_id):
        for attempt in range(1, self.max_retries + 1):
            try:
                log(job_id, f"Username: {self.username}")

                page.fill('input#account', self.username)
                time.sleep(.5)
                page.fill('input#password', self.password.replace(" ", ""))
                time.sleep(.5)
                page.click('button[type="submit"]')
                self.wait_for_navigation(page, job_id)
                log(job_id, "Authenticated Successfull")
                time.sleep(1)
                return True
            except Exception as e:
                log(job_id, f"Error Authentication: {e}")
            
            if attempt < self.max_retries:
                log(job_id, "Retrying login...")
                time.sleep(2)

        return False

    def account_creation_data(self, data, index):
        return {
            "#": index + 1,
            "User ID": data.get("userId").strip(),
            "Login Name": data.get("loginName"),
            "Updated By": f"{data.get('updatorUserId', '')} {data.get('updatorIp', '')}".strip(),
            "User Action Type": data.get("userActionType").strip(),
            "Description": data.get("actionDesc").strip(),
            "New/Old Value": f"New: {data.get('newValue', '')} Old: {data.get('oldValue', '')}".strip(),
            "Updated Time": data.get("createTime").strip()
        }
    # --- Helper function to run scraping with given date range ---
    def run_scrape_range(self, cookies, job_id, start_date, end_date, index):
        results = []
        retries = 0
        while retries < self.max_retries:
            try:
                page_number = 1
                page_size = 1000 
                while True:
                    params = {
                        "userActionType": "CREATE_ACCOUNT",
                        "userID": "",
                        "updatorUserId": "",
                        "updatorIp": "",
                        "startDate": f"{start_date} {self.timeRange}",
                        "endDate": f"{end_date} {self.timeRange}",
                        "pageSize": page_size,
                        "pageNumber": page_number
                    }

                    data_response = self.session.get(WINBDT_URL[2], params=params, cookies=cookies)
                    try:
                        data = data_response.json()
                    except ValueError:
                        log(job_id, f"❌ Failed to parse JSON, page {page_number}")
                        break

                    data_list = data.get("data", [])
                    log(job_id, f"Getting the data ({len(data_list)}) through Network Response, Page {page_number}")

                    for entry in data_list:
                        parsed = self.account_creation_data(entry, index)
                        results.append(parsed)
                        index += 1
                    # break when last page
                    if len(data_list) < page_size:
                        log(job_id, "No more page's to Scrape")
                        break
                    page_number += 1

                log(job_id, f"📌 Finished Range {start_date} → {end_date}, Count: {len(results)}")
                return results, index

            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")
                log(job_id, "Retrying...")

        return [], index
    
    def account_creation(self, cookies, job_id):
        log(job_id, "Account Creation")

        all_results = []
        index = 0
        retries = 0
        
        if self.timeGrain.lower() in ["month", "monthly"]:
            start_dt = datetime.strptime(self.startDate, "%d-%m-%Y")
            # end_dt = datetime.strptime(self.endDate, "%d-%m-%Y")

            # First half (15 days max)
            mid_dt = start_dt + timedelta(days=14)
            mid_str = mid_dt.strftime("%d-%m-%Y")

            log(job_id, f"Monthly Mode: Splitting into two parts")
            part1, index = self.run_scrape_range(cookies, job_id, self.startDate, mid_str, index)
            all_results.extend(part1)
           

            # Second half (rest)
            next_start = mid_dt.strftime("%d-%m-%Y")
            part2, index = self.run_scrape_range(cookies, job_id, next_start, self.endDate, index)
            all_results.extend(part2)

        else:
            # Normal (no split needed)
            all_results, index = self.run_scrape_range(cookies, job_id, self.startDate, self.endDate, index)

   
        return all_results if all_results else False      
        

    def deposit_withdrawal_Data(self, data, index):
        return {
            "#": index + 1,
            "Login Name": f"{data.get('loginName', '')} ID:{data.get('userId', '')}".strip(),
            "Name": f"{data.get('parentUserId', '')} {data.get('name', '')}".strip(),
            "+/-": data.get("diff"),
            "Balance": f"{data.get('oldBalance', '')} → {data.get('newBalance', '')}".strip(),
            "Date": data.get("allocatedDate"),
            "Exec User": data.get("execUserId"),
            "Remarks": data.get("remark")

        }

    def deposit_withdrawal(self, cookies, job_id, type):
        log(job_id, "Scraping For Deposit and Withdrawal Data")
        all_results = []
        index = 0
        retries = 0
        while retries < self.max_retries:
            try:
                log(job_id, f"Changing The Selection to {type}")
                page_number = 1
                page_size = 1000 
                while True:
                    params = {
                        "timeZone": "GMT+8:00",
                        "startDate": f"{self.startDate} {self.timeRange}",
                        "endDate": f"{self.endDate} {self.timeRange}",
                        "userType": "AGENT",
                        "userID": "",
                        "remark": "",
                        "currency": "",
                        "pageNumber": page_number,
                        "pageSize": page_size,
                        "creditAllocatedType": type,
                        "isShowSystemLog": "false",
                        "tagId": -1,
                        "tagUserType": "",
                        "tagUserLevel": ""
                    }
                    data_response = self.session.get(WINBDT_URL[3], params=params, cookies=cookies)
                    try:
                        data = data_response.json()
                    except ValueError:
                        log(job_id, f"❌ Failed to parse JSON for {type}, page {page_number}")
                        break
                    data_list = data.get("data", [])
                    log(job_id, f"Getting the data for {type}, page {page_number} through Network Response")

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
                log(job_id, f"⚠️ Error during scraping {type}, attempt {retries}: {e}")
                if retries == self.max_retries:
                    log(job_id, f"❌ Max retries reached for {type}, skipping.")
                    break  # Break the retry loop for this type
                time.sleep(5)  # Optional: add delay before retry
                    
        log(job_id, f"✅ Finished all types, Final Global Index Count: {index}")
        return all_results 

    def deposit_withdrawal_total(self, cookies, job_id, type):
        log(job_id, f"Getting the Grand Total For {type}")
        retries = 0
        total = 0
        while retries < self.max_retries:
            try: 
                page_number = 1
                page_size = 1000 
                params = {
                        "timeZone": "GMT+8:00",
                        "startDate": f"{self.startDate} {self.timeRange}",
                        "endDate": f"{self.endDate} {self.timeRange}",
                        "userType": "AGENT",
                        "userID": "",
                        "remark": "",
                        "currency": "",
                        "pageNumber": page_number,
                        "pageSize": page_size,
                        "creditAllocatedType": type,
                        "isShowSystemLog": "false",
                        "tagId": -1,
                        "tagUserType": "",
                        "tagUserLevel": ""
                    }
                data_response = self.session.get(WINBDT_URL[3], params=params, cookies=cookies)
                try:
                    data = data_response.json()
                except ValueError:
                    log(job_id, f"❌ Failed to parse JSON for {type}, page {page_number}")
                    break
                log(job_id, "Data Result Display")
                
                    
                data_total = data.get("pageInfo", {})
                log(job_id, "Getting the data through Network Response")

                if type.upper() == "DEPOSIT":
                    total = data_total.get("deposit", 0)
                elif type.upper() == "WITHDRAW":
                    total = data_total.get("withdraw", 0)
                else:
                    log(job_id, f"⚠️ Unknown type passed: {type}")
                    total = 0                    
                
                return total
            
            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")

        log(job_id, "❌ Max retries reached, returning partial data")
        return total 

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
            element = row.select_one(selector)
            
            if element:
                return element.get_text(strip=True)
            
            # Option 2: Fallback selectors
            fallback_selectors = [
                "td#userTotalPlJackpot span.textRed",
                "td#userTotalPlJackpot",
                "[id='userTotalPlJackpot'] span"
            ]
            
            for selector in fallback_selectors:
                element = row.select_one(selector)
                if element:
                    return element.get_text(strip=True)
            
            return "0"  # Default value when not found
            
        except Exception as e:
            print(f"Error getting jackpot value: {str(e)}")
            return "0"
        
    def get_company_value(page, row):
        try:
            # Primary selector with data-type attribute for specificity
            selector = "td[data-type='member']#userTotalCompany span.textRed"
            element = row.select_one(selector)
            
            if element:
                return element.get_text(strip=True)
            
            # Fallback selectors if primary fails
            fallback_selectors = [
                "td#userTotalCompany span.textRed",
                "td#userTotalCompany",
                "[id='userTotalCompany'] span"
            ]
            
            for selector in fallback_selectors:
                element = row.select_one(selector)
                if element:
                    return element.get_text(strip=True)
            
            return "0"  # Default value when not found
            
        except Exception as e:
            print(f"Error getting company value: {str(e)}")
            return "0"

    def overall_performance(self, page, job_id):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()

            # transfer requests cookies → playwright
            self.transfer_cookies_to_playwright(self.session, context)

            page = context.new_page()
            page.goto(WINBDT_URL[4])
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
                    # Set startDate
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#startDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_startDate
                    )

                    # Set endDate
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#endDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_endDate
                    )

                    log(job_id, "Inserted Filter")
                    page.click('#queryReport')

                    page.wait_for_selector("#loading", state="visible", timeout=5000)

                    page.wait_for_selector("#loading", state="hidden", timeout=15000)
                    
                    # try:
                    #     with page.expect_response(lambda r: "winloss" in r.url, timeout=30000):
                    #         page.click('#queryReport')
                    # except TimeoutError:
                    #     retries += 1
                    #     log(job_id, f"Timeout waiting for data response. Retry {retries}/{self.max_retries}")
                    #     continue
   

                    self.wait_for_navigation(page, job_id)
                    time.sleep(2.5)
                    page.wait_for_selector("#tbodyAgent .trTitle", timeout=10000)
                    html = page.inner_html("#tbodyAgent")

                    soup = BeautifulSoup(html, "html.parser")
                    data = []

                    for row in soup.select("tr.trTitle"):
                        cols = row.find_all("td")
                        if len(cols) >= 3:
                            data.append({
                                "User ID": row.select_one("td a#titleUseID").get_text(strip=True) if row.select_one("td a#titleUseID") else "",
                                "Name": row.select_one('td[data-type="name"]').get_text(strip=True) if row.select_one('td[data-type="name"]') else "",
                                "Valid Turnover": row.select_one("td#userTotalPlTurnover").get_text(strip=True) if row.select_one("td#userTotalPlTurnover") else "",
                                "Active Player": row.select_one("td span#userTotalActivePlayer").get_text(strip=True) if row.select_one("td span#userTotalActivePlayer") else "",
                                "Win/loss": row.select_one("td div.member span#userTotalPlWinloss").get_text(strip=True) if row.select_one("td div.member span#userTotalPlWinloss") else "",
                                "Jackpot Win/Loss": self.get_jackpot_value(row),  # Using the updated method
                                "Member Comm.": row.select_one("td#userTotalPlComm").get_text(strip=True) if row.select_one("td#userTotalPlComm") else "",
                                "Total P/L": row.select_one("td#userTotalPlProfitloss").get_text(strip=True) if row.select_one("td#userTotalPlProfitloss") else "",
                                "PT Win/Loss": row.select_one("td#userTotaldownlineWinloss").get_text(strip=True) if row.select_one("td#userTotaldownlineWinloss") else "",
                                "Direct Comm.": row.select_one("td#userTotaldownlineComm").get_text(strip=True) if row.select_one("td#userTotaldownlineComm") else "",
                                "Total P/L (Direct)": row.select_one("td#userTotaldownlineProfitloss").get_text(strip=True) if row.select_one("td#userTotaldownlineProfitloss") else "",
                                "PT Win/Loss (Self)": row.select_one("td#userTotalselfWinloss").get_text(strip=True) if row.select_one("td#userTotalselfWinloss") else "",
                                "Self Comm.": row.select_one("td#userTotalselfComm").get_text(strip=True) if row.select_one("td#userTotalselfComm") else "",
                                "Total P/L (Self)": row.select_one("td#userTotalselfProfitloss").get_text(strip=True) if row.select_one("td#userTotalselfProfitloss") else "",
                                "Company": self.get_company_value(row)  # Using the updated method
                            })

                    all_results.extend(data)
                    break  # ✅ exit retry loop if successful

                except Exception as e:
                    retries += 1
                    log(job_id, f"Retry {retries}/{self.max_retries} failed for date {formatted_startDate}: {e}")
                    if retries >= self.max_retries:
                        log(job_id, f"Failed to scrape data for {formatted_startDate} after several attempts.")
                        break

            return all_results

    def provider_performance(self, page, job_id):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()

            # transfer requests cookies → playwright
            self.transfer_cookies_to_playwright(self.session, context)

            page = context.new_page()
            page.goto(WINBDT_URL[5])
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
                    # Set startDate
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
                    page.wait_for_selector("#loading", state="visible", timeout=5000)

                    page.wait_for_selector("#loading", state="hidden", timeout=15000)
                
                    # try:
                    #     with page.expect_response(lambda r: "winLossByProduct" in r.url, timeout=30000):
                    #         page.click('#queryReport')
                    # except TimeoutError:
                    #     retries += 1
                    #     log(job_id, f"Timeout waiting for data response. Retry {retries}/{self.max_retries}")
                    #     continue
                    self.wait_for_navigation(page, job_id)
                    
                    time.sleep(2.5)

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
                    break  # ✅ exit retry loop if successful

                except Exception as e:
                    retries += 1
                    log(job_id, f"Retry {retries}/{self.max_retries} failed for date {formatted_startDate}: {e}")
                    if retries >= self.max_retries:
                        log(job_id, f"Failed to scrape data for {formatted_startDate} after several attempts.")
                        break

            return all_results
    
    def run(self, job_id):
        for retry in range(self.max_retries):
            try:
                resp = self.session.get(WINBDT_URL[0], timeout=10)
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
                    "Origin": "https://ag.winbdt.co",
                    "Referer": "https://ag.winbdt.co/index.jsp",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                }
                auth_response = self.session.post(WINBDT_URL[1], data=payload, headers=headers)
                if auth_response.status_code == 200:
                    log(job_id, "✅ Login successful")
                    cookies = self.session.cookies.get_dict()
                    log(job_id, "Authentication successful. Cookies captured.")

                    account_creation = self.account_creation(cookies, job_id)
                    log(job_id, "✅ Account Creation Done")
                    time.sleep(.5)

                    deposit_withdrawal = self.deposit_withdrawal(cookies, job_id, "ALL")
                    log(job_id, f"Result: {deposit_withdrawal}")
                    time.sleep(.5)

                    deposit_total = self.deposit_withdrawal_total(cookies, job_id, "DEPOSIT")
                    log(job_id, f"Total: {deposit_total}")
                    time.sleep(.5)

                    withdrawal_total = self.deposit_withdrawal_total(cookies, job_id, "WITHDRAW")
                    log(job_id, f"Total: {withdrawal_total}")
                    time.sleep(.5)

                    overall_performance= self.overall_performance(cookies, job_id)
                    log(job_id, f"Result: {overall_performance}")
                    time.sleep(.5)

                    provider_performance= self.provider_performance(cookies, job_id)
                    log(job_id, f"Result: {provider_performance}")
                    time.sleep(.5)

                    data = {
                            "status": 200,
                            "text": "Data Fetched successfully",
                            "title": "Fetch Completed!",
                            "icon": "success",
                            "account_creation": account_creation,
                            "deposit_withdrawal_results" : deposit_withdrawal,
                            "deposit_total" : deposit_total,
                            "withdrawal_total": withdrawal_total,
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