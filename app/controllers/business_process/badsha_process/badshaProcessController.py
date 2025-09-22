from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timedelta
from urllib.parse import urlparse
from app.automations.log.state import log
from collections import defaultdict
from bs4 import BeautifulSoup
import requests
import json
import re
import time

class badshaProcessController():
    def __init__(self, username, password, url, startDate, endDate, timeGrain):
        self.username = username
        self.password = password
        self.url = url
        self.startDate = startDate
        self.endDate = endDate
        self.timeRange = "12:00:00"
        self.timeGrain = timeGrain
        self.max_retries = 3
        parsed_url = urlparse(self.url)
        self.domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
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

    # --- Helper function to run scraping with given date range ---
    def run_scrape_range(self, page, job_id, start_date, end_date):
    
        retries = 0
        while retries < self.max_retries:
            try:
                log(job_id, f"IFrame Showed | Range: {start_date} → {end_date}")
                iframe_element = page.wait_for_selector(
                    "iframe[src*='userActionLog.jsp']", timeout=10000
                )
                frame = iframe_element.content_frame()

                frame.wait_for_selector("#userActionType", timeout=5000)
                self.wait_for_navigation(page, job_id)

                log(job_id, "Changing The Selection to Create Account")
                time.sleep(1.5)
                frame.select_option("#userActionType", "CREATE_ACCOUNT")

                # Set start date
                frame.evaluate(
                    """(date) => {
                        let el = document.querySelector('#startDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    start_date
                )
                frame.fill('#startTime', self.timeRange)

                # Set end date
                frame.evaluate(
                    """(date) => {
                        let el = document.querySelector('#endDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    end_date
                )
                frame.fill('#endTime', self.timeRange)

                log(job_id, "Inserted Filter")
                frame.select_option("#pageSize", "1000")
                log(job_id, "Set results per page to 1000")
                self.wait_for_navigation(page, job_id)
                time.sleep(1.5)
                
                with page.expect_response(lambda r: "userActionLog" in r.url, timeout=30000) as resp_info:
                    frame.click('a.btnAMain')

                # --- Handle response ---
                response = resp_info.value
                log(job_id, "Data Result Display")

                if "application/json" in response.headers.get("content-type", ""):
                    data = response.json()
                    data_list = data.get("pageInfo", [])
                    result = data_list.get("totalCount", 0)
                    log(job_id, f"Total of Data: {result}")

                    
                else:
                    html_text = response.text()
                    log(job_id, "📄 HTML Data:", html_text)

                log(job_id, f"📌 Finished Range {start_date} → {end_date}, Count: {result}")
                return result

            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")
                log(job_id, "Retrying...")

        return []
    
    def account_creation(self, page, job_id):
        log(job_id, "Navigating on Account Creation")
        self.wait_for_navigation(page, job_id)
        page.click("a[data-btn='modal-userActionLog']")

        # all_results = []
        
        if self.timeGrain.lower() in ["month", "monthly"]:
            start_dt = datetime.strptime(self.startDate, "%d-%m-%Y")
            # end_dt = datetime.strptime(self.endDate, "%d-%m-%Y")

            # First half (15 days max)
            mid_dt = start_dt + timedelta(days=14)
            mid_str = mid_dt.strftime("%d-%m-%Y")

            log(job_id, f"Monthly Mode: Splitting into two parts")
            part1 = self.run_scrape_range(page, job_id, self.startDate, mid_str)
            # all_results.extend(part1)
            page.click("div.popup-XL a.close")

            # Second half (rest)
            self.wait_for_navigation(page, job_id)
            page.click("a[data-btn='modal-userActionLog']")
            next_start = mid_dt.strftime("%d-%m-%Y")
            part2 = self.run_scrape_range(page, job_id, next_start, self.endDate)
            # all_results.extend(part2)

            all_results = part1 + part2

        else:
            # Normal (no split needed)
            all_results = self.run_scrape_range(page, job_id, self.startDate, self.endDate)

        page.click("div.popup-XL a.close")
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




    def deposit_withdrawal(self, page, job_id, types=None):
        """
        Scrapes deposit & withdrawal data for multiple types.
        :param types: list of type values (e.g., ["deposit", "withdrawal"])
                    if None, it will auto-detect from the dropdown.
        """
        log(job_id, "Scraping For Deposit and Withdrawal Data")
        all_results = []
        index = 0  # Track the global index across types and pages

        # ✅ Auto-detect available types if not provided
        if types is None:
            log(job_id, "Detecting available types from dropdown")
            types = page.eval_on_selector_all(
                "#creditAllocatedType option",
                "els => els.map(el => el.value).filter(v => v.trim() !== '')"
            )
            log(job_id, f"Available types detected: {types}")

        log(job_id, "Opening Fund in/out page...")
        page.locator('.navDropdown').nth(2).click()
        page.locator("#balanceItem").click()
        self.wait_for_navigation(page, job_id)

        for type_val in types:
            retries = 0
            while retries < self.max_retries:
                try:
                    log(job_id, f"Changing The Selection to {type_val}")
                    self.wait_for_navigation(page, job_id)
                    time.sleep(1)
                    page.select_option("#creditAllocatedType", type_val)

                    # Fill start date
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#startDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        self.startDate
                    )
                    time.sleep(.5)
                    page.fill('#startTime', self.timeRange)

                    # Fill end date
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#endDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        self.endDate
                    )
                    page.fill('#endTime', self.timeRange)
                    time.sleep(.5)
                    log(job_id, "Inserted Filter")

                    page.select_option("#pageSize", "1000")
                    log(job_id, "Set results per page to 1000")
                    self.wait_for_navigation(page, job_id)

                    first_page = True
                    while True:
                        if first_page:
                            with page.expect_response(lambda r: "creditAllocatedLog" in r.url, timeout=10000) as resp_info:
                                page.click('#searchBtn')
                            # response = resp_info.value

                            first_page = False
                    
                            response = resp_info.value
                        else:
                            next_link = page.locator('a.next')
                            href = next_link.get_attribute("href")

                            if href and "page-" in href:  # Means there's a next page
                                log(job_id, f"Moving to next page: {href}")
                                with page.expect_response(lambda r: "creditAllocatedLog" in r.url) as resp_info:
                                    next_link.click()
                       
                                response = resp_info.value
                            else:
                                log(job_id, f"No more pages to scrape for {type_val}")
                                break

                        log(job_id, "Data Result Display")
                        if "application/json" in response.headers.get("content-type", ""):
                            data = response.json()
                            data_list = data.get("data", [])
                            log(job_id, f"Getting the data for {type_val} through Network Response")

                            for entry in data_list:
                                result = self.deposit_withdrawal_Data(entry, index)
                                # ✅ Add type info for clarity
                                log(job_id, f"{result}")
                                all_results.append(result)
                                index += 1

                        else:
                            html_text = response.text()
                            log(job_id, "📄 HTML Data:", html_text)

                    log(job_id, f"📌 Finished scraping for {type_val}, current total count: {index}")
                    break  # ✅ Exit retry loop for this type

                except Exception as e:
                    retries += 1
                    log(job_id, f"⚠️ Error during scraping {type_val}, attempt {retries}: {e}")

        log(job_id, f"✅ Finished all types, Final Global Index Count: {index}")
        return all_results


    def fund_in_out(self, page, job_id, type):
        url = "https://ag.badsha.live/service/agent/creditAllocatedLog"

        payload = {
            "timeZone": "GMT+08:00", 
            "startDate": f"{self.startDate} {self.timeRange}", 
            "endDate": f"{self.endDate} {self.timeRange}", 
            "userType": "PLAYER",  
            "pageNumber": "1", 
            "pageSize": "1000", 
            "creditAllocatedType": type, 
            "isShowSystemLog": "value1", 
            "tagId": "-1"
        }  # data to send

        # Get cookies from Playwright
        cookies = page.context.cookies()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie["name"], cookie["value"])
        headers = {
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",  # example
            "Origin": "https://ag.badsha.live",
            "Referer": "https://ag.badsha.live/your/page",
            "User-Agent": "Mozilla/5.0 ..."  # same as browser
            # plus any other headers from resp.request.headers
        }

        try:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            response = session.post(url, data=payload, headers=headers, timeout=30)

            if response.status_code == 200 and "error" in response.text.lower():
                headers["Content-Type"] = "application/json"
                response = session.post(url, json=payload, headers=headers, timeout=30)

            response.raise_for_status()
            # print(f"[ ✅ Status: {response.status_code} ]")
            # print("🔎 Raw response:", response.text[:500])

            data = response.json()
            page_info = data.get("pageInfo", {})  # dict
            # results = page_info.get("deposit", 0)

            if type in ["DEPOSIT"]:
                result = page_info.get("deposit", 0)
            else:
                result = page_info.get("withdraw", 0)

            log(job_id, f"{result}")
            time.sleep(15)
            # return result

        except requests.exceptions.RequestException as e:
            print(f"[{job_id}] ❌ Network error:", e)
            return 0

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

    def overall_performance(self, page, job_id):
        log(job_id, "Scraping For overall_performance Data")

        # Parse the user-provided start date (format: DD-MM-YYYY)
        start_date = datetime.strptime(self.startDate, "%d-%m-%Y").date()
        end_date = datetime.strptime(self.endDate, "%d-%m-%Y").date()
        end_dates = end_date - timedelta(days=1)

        all_results = []

        # ✅ Trigger sidebar ONCE before looping through dates
        log(job_id, "Trigger Sidebar")
        page.locator('.navDropdown').nth(1).click()
        page.locator('.navDropdown').nth(1).locator('#reportDetailItem').click()
        self.wait_for_navigation(page, job_id)

        # Loop from yesterday back to start_date
        # current_date = yesterday
        
        formatted_startDate = start_date.strftime("%d-%m-%Y")
        formatted_endDate = end_dates.strftime("%d-%m-%Y")
        log(job_id, f"Processing date: {formatted_startDate} - {formatted_endDate}")

        retries = 0
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
                self.wait_for_navigation(page, job_id)
                page.click('#queryReport')
                time.sleep(5)

                page.wait_for_selector("#tbodyAgent tr#tempTitle")

                rows = page.query_selector_all("#tbodyAgent tr#tempTitle")
                data = []

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
        log(job_id, "Scraping For Provider Performance Data")

        # Parse the user-provided start date (format: DD-MM-YYYY)
        start_date = datetime.strptime(self.startDate, "%d-%m-%Y").date()
        end_date = datetime.strptime(self.endDate, "%d-%m-%Y").date()
        end_dates = end_date - timedelta(days=1)

        all_results = []

        # ✅ Trigger sidebar ONCE before looping through dates
        log(job_id, "Trigger Sidebar")
        page.locator('.navDropdown').nth(1).locator('#winLossProductItem').click()
        self.wait_for_navigation(page, job_id)

        # Loop from yesterday back to start_date
        # current_date = yesterday

        formatted_startDate = start_date.strftime("%d-%m-%Y")
        formatted_endDate = end_dates.strftime("%d-%m-%Y")
        log(job_id, f"Processing date: {formatted_startDate} - {formatted_endDate}")

        retries = 0
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
                time.sleep(1.5)
                # Set endDate
                page.evaluate(
                    """(date) => {
                        let el = document.querySelector('#endDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    formatted_endDate
                )
                time.sleep(1.5)

                log(job_id, "Inserted Filter")
                self.wait_for_navigation(page, job_id)
                page.click('#queryReport')
                time.sleep(.5)

                page.wait_for_selector("#tbodyAgent .trTitle")

                rows = page.query_selector_all("#tbodyAgent .trTitle")
                data = []

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
        with sync_playwright() as p:
            browser = None
            for retry in range(self.max_retries):
                try:
                    # Launch the browser with proxy and user agent
                    browser = p.chromium.launch(
                        headless=False,
                        # proxy={"server": proxy} if proxy else None
                    )

                    # Create a browser context with realistic settings
                    context = browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                        viewport={"width": 1920, "height": 1080},  # Set a realistic viewport
                        extra_http_headers={
                            "Accept": "*/*",
                            "Accept-Encoding": "gzip, deflate, br, zstd",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Origin": f"{self.domain}",
                            "Referer": f"{self.url}",
                            "Sec-CH-UA": '"Chromium";v="131", "Not_A Brand";v="24"',
                            "Sec-CH-UA-Mobile": "?0",
                            "Sec-CH-UA-Platform": '"Windows"',
                            "Sec-Fetch-Dest": "empty",
                            "Sec-Fetch-Mode": "cors",
                            "Sec-Fetch-Site": "same-origin",
                            "X-Requested-With": "XMLHttpRequest"
                        }
                    )
                    page = context.new_page()
                    log(job_id, f"Attempt {retry + 1} to visit site: {self.url}")
                    page.goto(self.url, timeout=30000)  # Set a reasonable timeout
                    log(job_id, "Page successfully loaded.")

                    auth = self.authenticate(page, job_id)

                    if not auth:
                        log(job_id, "Authentication Failed Hit Max Retries")
                        break

                    account_creation = self.account_creation(page, job_id)
                    log(job_id, "✅ Account Creation Done")
                    time.sleep(1)
                    deposit_withdrawal = self.deposit_withdrawal(
                        page, 
                        job_id, 
                        types=[
                            "DEPOSIT", "WITHDRAW", "PROMOTION_MAX_WIN", "SHARE_PROFIT", 
                            "BONUS", "COMM_BONUS", "RESCUE_BONUS", "REWARD_TURNOVER_AMOUNT", "SIGNUP_REBATE"
                        ]
                    )
                    log(job_id, f"✅ Deposit_Withdrawal{len(deposit_withdrawal)}")
                    time.sleep(1)

                    # deposit_withdrawal = self.fund_in_out(page, job_id, "WITHDRAW")
                    # log(job_id, "✅ Deposit_Withdrawal")
                    # time.sleep(1)

                    # if self.timeGrain in ["Weekly", "weekly", "Monthly", "monthly"]:
                    #     ftd = self.deposit_withdrawal(page, job_id, "DEPOSIT")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     ftd = self.deposit_withdrawal(page, job_id, "FIRSTDEPOSIT")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     ftd_amount = self.deposit_withdrawal(page, job_id, "WITHDRAW")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     total_top_ups = self.deposit_withdrawal(page, job_id, "PROMOTION_MAX_WIN")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     total_withdrawal = self.deposit_withdrawal(page, job_id, "SHARE_PROFIT")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     earned_profit = self.deposit_withdrawal(page, job_id, "BONUS")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     promotion_bonus = self.deposit_withdrawal(page, job_id, "COMM_BONUS")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     total_withdrawal = self.deposit_withdrawal(page, job_id, "RESCUE_BONUS")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     total_withdrawal = self.deposit_withdrawal(page, job_id, "REWARD_TURNOVER_AMOUNT")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    #     total_withdrawal = self.deposit_withdrawal(page, job_id, "SIGNUP_REBATE")
                    #     log(job_id, "✅ ftd")
                    #     time.sleep(.5)

                    overall_performance= self.overall_performance(page, job_id)
                    log(job_id, "✅ Overall Performance")
                    time.sleep(1)

                    provider_performance= self.provider_performance(page, job_id)
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
                finally:
                    if browser:
                        browser.close()