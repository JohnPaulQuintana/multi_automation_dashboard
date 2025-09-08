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

class winbdtController:
    def __init__(self, username, password, url, startDate, endDate):
        self.username = username
        self.password = password
        self.url = url
        self.startDate = startDate
        self.endDate = endDate
        self.timeRange = "12:00:00"
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

    def account_creation_data(self, data, index):
        return {
            "#": index + 1,
            "User ID": data.get("userId"),
            "Login Name": data.get("loginName"),
            "Updated By": f"{data.get('updatorUserId', '')} {data.get('updatorIp', '')}".strip(),
            "User Action Type": data.get("userActionType"),
            "Description": data.get("actionDesc"),
            "New/Old Value": f"New: {data.get('newValue', '')} Old: {data.get('oldValue', '')}".strip(),
            "Updated Time": data.get("createTime")
        }

    def account_creation(self, page, job_id):
        log(job_id, "Navigating on Account Creation")
        self.wait_for_navigation(page, job_id)
        page.click("a[data-btn='modal-userActionLog']")

        all_results = []
        index = 0
        retries = 0
        while retries < self.max_retries:
            try: 
                log(job_id, "IFrame Showed")
                iframe_element = page.wait_for_selector("iframe[src*='userActionLog.jsp']", timeout=10000)

                # 3. Get the frame object from iframe element
                frame = iframe_element.content_frame()
                
                # 4. Interact inside the iframe
                frame.wait_for_selector("#userActionType", timeout=5000)
                self.wait_for_navigation(page, job_id)

                log(job_id, "Changing The Selection to Create Account")
                time.sleep(1.5)
                frame.select_option("#userActionType", "CREATE_ACCOUNT")
                
            
                
                frame.evaluate(
                    """(date) => {
                        let el = document.querySelector('#startDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    self.startDate
                )
                frame.fill('#startTime', self.timeRange)

                frame.evaluate(
                    """(date) => {
                        let el = document.querySelector('#endDate');
                        el.value = date;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }""",
                    self.endDate
                )
                frame.fill('#endTime', self.timeRange)
                
                log(job_id, "Inserted Filter")
                frame.select_option("#pageSize", "1000")
                log(job_id, "Set results per page to 1000")
                self.wait_for_navigation(page, job_id)
                time.sleep(1.5)

                while True:
                    with page.expect_response(lambda r: "userActionLog" in r.url) as resp_info:
                        frame.click('a.btnAMain')

                    response = resp_info.value
                    log(job_id, "Data Result Display")
                    if "application/json" in response.headers.get("content-type", ""):
                        data = response.json()
                        
                        data_list = data.get("data", [])
                        log(job_id, "Getting the data through Network Response")


                        for entry in data_list:

                            data = self.account_creation_data(entry, index)
                            all_results.append(data)
                            index += 1
                    # return filtered_results
                        
                    else:
                        html_text = response.text()
                        log(job_id, "📄 HTML Data:", html_text)
                    
                    next_link = frame.locator('a.now.next')
                    href = next_link.get_attribute("href")

                    if href and "page-" in href:  # Means there's a next page
                        log(job_id, f"Moving to next page: {href}")
                        next_link.click()
                        self.wait_for_navigation(page, job_id)
                    else:
                        log(job_id, "No more pages to scrape")
                        break
                log(job_id, f"📌 Final Global Index Count: {index}")
                log(job_id, "Closing the IFrame")
                page.click("div.popup-XL a.close")

                return all_results            
        
            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")
        
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False

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

    def deposit_withdrawal(self, page, job_id, type):
        log(job_id, "Scraping For Deposit and Withdrawal Data")
        all_results = []
        # self.date= "10-08-2025"
        index = 0  # Track the global index across pages
        retries = 0
        while retries < self.max_retries:
            try: 
                log(job_id, "Triggering Fund In/Out")
                page.locator('.navDropdown').nth(2).click()
                page.locator("#balanceItem").click()
                log(job_id, f"Changing The Selection to {type}")
                self.wait_for_navigation(page, job_id)
                time.sleep(1)
                page.select_option("#creditAllocatedType", type)
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
                while True:
                    with page.expect_response(lambda r: "creditAllocatedLog" in r.url) as resp_info:
                        page.click('#searchBtn')

                    response = resp_info.value
                    log(job_id, "Data Result Display")
                    if "application/json" in response.headers.get("content-type", ""):
                        data = response.json()
                        
                        data_list = data.get("data", [])
                        data_total = data.get("pageInfo", {})
                        log(job_id, "Getting the data through Network Response")


                        for entry in data_list:
                            result = self.deposit_withdrawal_Data(entry, index)
                            all_results.append(result)
                            index += 1

                        # total = data_total.get("deposit", 0)
                        # return filtered_results
                        
                    else:
                        html_text = response.text()
                        log(job_id, "📄 HTML Data:", html_text)
                    
                    next_link = page.locator('a.next')
                    href = next_link.get_attribute("href")

                    if href and "page-" in href:  # Means there's a next page
                        log(job_id, f"Moving to next page: {href}")
                        next_link.click()
                        self.wait_for_navigation(page, job_id)
                    else:
                        log(job_id, "No more pages to scrape")
                        break
                        
                log(job_id, f"📌 Final Global Index Count: {index}")
                return all_results
            
            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")

        log(job_id, "❌ Max retries reached, returning partial data")
        return all_results, 0 

    def deposit_withdrawal_total(self, page, job_id, type):
        log(job_id, f"Getting the Grand Total For {type}")
        retries = 0
        total = 0
        while retries < self.max_retries:
            try: 
                page.select_option("#creditAllocatedType", type)

                time.sleep(.5)
                self.wait_for_navigation(page, job_id)
                
                with page.expect_response(lambda r: "creditAllocatedLog" in r.url) as resp_info:
                    page.click('#searchBtn')

                response = resp_info.value
                log(job_id, "Data Result Display")
                if "application/json" in response.headers.get("content-type", ""):
                    data = response.json()
                    
                    data_total = data.get("pageInfo", {})
                    log(job_id, "Getting the data through Network Response")

                    if type.upper() == "DEPOSIT":
                        total = data_total.get("deposit", 0)
                    elif type.upper() == "WITHDRAW":
                        total = data_total.get("withdraw", 0)
                    else:
                        log(job_id, f"⚠️ Unknown type passed: {type}")
                        total = 0                    
                else:
                    html_text = response.text()
                    log(job_id, "📄 HTML Data:", html_text)
                    
                
                return total
            
            except Exception as e:
                retries += 1
                log(job_id, f"⚠️ Error during scraping attempt {retries}: {e}")

        log(job_id, "❌ Max retries reached, returning partial data")
        return total 

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
        while start_date <= end_dates:
            formatted_date = start_date.strftime("%d-%m-%Y")
            log(job_id, f"Processing date: {formatted_date}")

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
                        formatted_date
                    )

                    # Set endDate
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#endDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_date
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
                    log(job_id, f"Retry {retries}/{self.max_retries} failed for date {formatted_date}: {e}")
                    if retries >= self.max_retries:
                        log(job_id, f"Failed to scrape data for {formatted_date} after several attempts.")
                        break

            # Move one day back
            start_date += timedelta(days=1)
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
        while start_date <= end_dates:
            formatted_date = start_date.strftime("%d-%m-%Y")
            log(job_id, f"Processing date: {formatted_date}")

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
                        formatted_date
                    )
                    time.sleep(1.5)
                    # Set endDate
                    page.evaluate(
                        """(date) => {
                            let el = document.querySelector('#endDate');
                            el.value = date;
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }""",
                        formatted_date
                    )
                    time.sleep(1.5)

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
                    log(job_id, f"Retry {retries}/{self.max_retries} failed for date {formatted_date}: {e}")
                    if retries >= self.max_retries:
                        log(job_id, f"Failed to scrape data for {formatted_date} after several attempts.")
                        break

            # Move one day back
            start_date += timedelta(days=1)
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
                    log(job_id, f"Result: {account_creation}")
                    time.sleep(1)
                    deposit_withdrawal = self.deposit_withdrawal(page, job_id, "ALL")
                    log(job_id, f"Result: {deposit_withdrawal}")
                    time.sleep(1)

                    deposit_total = self.deposit_withdrawal_total(page, job_id, "DEPOSIT")
                    log(job_id, f"Total: {deposit_total}")
                    time.sleep(1)

                    withdrawal_total = self.deposit_withdrawal_total(page, job_id, "WITHDRAW")
                    log(job_id, f"Total: {deposit_total}")
                    time.sleep(1)

                    overall_performance= self.overall_performance(page, job_id)
                    log(job_id, f"Result: {overall_performance}")
                    time.sleep(1)

                    provider_performance= self.provider_performance(page, job_id)
                    log(job_id, f"Result: {provider_performance}")
                    time.sleep(1)

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
                finally:
                    if browser:
                        browser.close()