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

class BadshaController:
    def __init__(self, username, password, url, TodayDate, YesterdayDate, timeRange):
        self.username = username
        self.password = password
        self.url = url
        self.todayDate = TodayDate
        self.yesterdayDate = YesterdayDate
        self.timeRange = timeRange
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
    
    def authentication(self, page, job_id):
        try:
            log(job_id, f"Username: {self.username}, Date Today: {self.todayDate}, Yesterday Date: {self.yesterdayDate}")

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
            return False
    def filter_nsu_data(self, data, index):

        return {
            "#": index + 1,
            "userId": data.get("userId"),
            "loginName": data.get("loginName"),
            "createTime": data.get("createTime"),
            "updateTime": data.get("updateTime"),
            "name": data.get("name"),
            "email": data.get("email"),
            "tel": data.get("tel"),
            "firstDeposit": "√" if data.get("firstDeposit") else "",
            "type": "New Signup",
            "status": "PENDING"

        }
        
    def nsu_data(self,page, job_id):
        log(job_id, "Scraping For NSU Data")
        all_results = []
        # self.date= "10-08-2025"
        global_index = 0  # Track the global index across pages
        filtered_count = 0
        retries = 0
        while retries < self.max_retries:
            log(job_id, "Trigger Sidebar")
            page.locator('.navDropdown').nth(1).click()
            page.locator('.navDropdown').nth(1).locator('#identityVerificationForm').click()
            self.wait_for_navigation(page, job_id)
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#startDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.yesterdayDate
            )
            page.fill('#startTime', self.timeRange)
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#endDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.todayDate
            )
            page.fill('#endTime', self.timeRange)
            log(job_id, "Inserted Filter")
            page.select_option("#pageSize", "1000")
            log(job_id, "Set results per page to 1000")
            # page.click('a.btn.btn-primary >> text=Search')
            self.wait_for_navigation(page, job_id)
            
            
            while True:
                with page.expect_response(lambda r: "queryIdentityVerificationForm" in r.url) as resp_info:
                    page.click('a.btn.btn-primary >> text=Search')

                response = resp_info.value
                log(job_id, "Data Result Display")
                if "application/json" in response.headers.get("content-type", ""):
                    data = response.json()
                    
                    data_list = data.get("data", [])
                    log(job_id, "Getting the data through Network Response")

                    filtered_results = []
                    for entry in data_list:
                        if not entry.get("kyc", False):
                            global_index += 1  # still increment even if skipped
                            continue

                        filtered = self.filter_nsu_data(entry, global_index)
                        all_results.append(filtered)
                        global_index += 1
                        filtered_count += 1

                    
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
            log(job_id, f"✅ Total filtered KYC=True: {filtered_count}")
            log(job_id, f"📌 Final Global Index Count: {global_index}")

            return all_results            
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False

    def filter_ftd_deposit_withdrawal_data(self, data, index, txn_type):
        # remarks_value = "Y" if data.get("isFirstDeposit") else "N"
        if txn_type in ("ftd", "deposit"):
            remarks_value = "Y" if data.get("isFirstDeposit") else "N"
        elif txn_type == "withdrawal":
            remarks_value = "-"

        allocated_date = data.get("allocatedDate")
        # Format the allocated date if it's present
        if allocated_date:
            try:
                # Parse the date from the string (assuming the format is 'DD-MM-YYYY HH:MM:SS')
                allocated_date_obj = datetime.strptime(allocated_date, "%d-%m-%Y %H:%M:%S")
                
                # Format the date to the desired format and append the timezone
                formatted_date = allocated_date_obj.strftime("%d-%m-%Y %H:%M:%S") + " (GMT+8:00)"
            except ValueError:
                formatted_date = allocated_date  # If parsing fails, keep the original value
        else:
            formatted_date = allocated_date

        return {
            "#": index + 1,
            "User Id": data.get("userId"),
            "Login Name": data.get("loginName"),
            "Name": data.get("name"),
            "Currency": data.get("currency"),
            "+ / -": data.get("diff"),
            "Old Balance": data.get("oldBalance"),
            "New Balance": data.get("newBalance"),
            "Date": formatted_date,
            "Upline": data.get("parentUserId"),
            "Exec User": data.get("execUserId"),
            "First Deposit": remarks_value,
            "Remarks": data.get("remark")
        }

    def ftd_data(self, page, job_id):
        log(job_id, "Scraping For FTD Data")
        all_results = []
        # self.date= "10-08-2025"
        global_index = 0  # Track the global index across pages
        retries = 0
        while retries < self.max_retries:
            log(job_id, "Trigger Sidebar")
            page.locator('.navDropdown').nth(3).click()
            page.locator('.navDropdown').nth(3).locator('#balanceItem').click()
            log(job_id, "Changing The Selection to First Deposit")
            self.wait_for_navigation(page, job_id)
            page.select_option("#creditAllocatedType", "FIRSTDEPOSIT")
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#startDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.yesterdayDate
            )
            page.fill('#startTime', self.timeRange)
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#endDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.todayDate
            )
            page.fill('#endTime', self.timeRange)
            log(job_id, "Inserted Filter")
            page.select_option("#pageSize", "1000")
            log(job_id, "Set results per page to 1000")
            
            # page.click('a.btn.btn-primary >> text=Search')
            self.wait_for_navigation(page, job_id)
            
            
            while True:
                with page.expect_response(lambda r: "creditAllocatedLog" in r.url) as resp_info:
                    page.click('#searchBtn')

                response = resp_info.value
                log(job_id, "Data Result Display")
                if "application/json" in response.headers.get("content-type", ""):
                    data = response.json()
                    
                    data_list = data.get("data", [])
                    log(job_id, "Getting the data through Network Response")


                    for entry in data_list:
                        filtered = self.filter_ftd_deposit_withdrawal_data(entry, global_index, "ftd")
                        all_results.append(filtered)
                        global_index += 1
                    
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
                    
            log(job_id, f"📌 Final Global Index Count: {global_index}")
            return all_results   
                 
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False
    
    def deposit_data(self, page, job_id):
        log(job_id, "Scraping For Deposit Data")
        all_results = []
        # self.date= "10-08-2025"
        global_index = 0  # Track the global index across pages
        retries = 0
        while retries < self.max_retries:
            log(job_id, "Changing The Selection to Deposit")
            self.wait_for_navigation(page, job_id)
            page.select_option("#creditAllocatedType", "DEPOSIT")
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#startDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.yesterdayDate
            )
            page.fill('#startTime', self.timeRange)
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#endDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.todayDate
            )
            page.fill('#endTime', self.timeRange)
            log(job_id, "Inserted Filter")
            page.select_option("#pageSize", "1000")
            log(job_id, "Set results per page to 1000")
            
            # page.click('a.btn.btn-primary >> text=Search')
            self.wait_for_navigation(page, job_id)
            
            
            while True:
                with page.expect_response(lambda r: "creditAllocatedLog" in r.url) as resp_info:
                    page.click('#searchBtn')

                response = resp_info.value
                log(job_id, "Data Result Display")
                if "application/json" in response.headers.get("content-type", ""):
                    data = response.json()
                    
                    data_list = data.get("data", [])
                    log(job_id, "Getting the data through Network Response")


                    for entry in data_list:
                        filtered = self.filter_ftd_deposit_withdrawal_data(entry, global_index, "deposit")
                        all_results.append(filtered)
                        global_index += 1
                    
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
                    
            log(job_id, f"📌 Final Global Index Count: {global_index}")
            return all_results   
                 
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False
    
    def withdrawal_data(self, page, job_id):
        log(job_id, "Scraping For Withdrawal Data")
        all_results = []
        # self.date= "10-08-2025"
        global_index = 0  # Track the global index across pages
        retries = 0
        while retries < self.max_retries:
            log(job_id, "Changing The Selection to Withdraw")
            self.wait_for_navigation(page, job_id)
            page.select_option("#creditAllocatedType", "WITHDRAW")
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#startDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.yesterdayDate
            )
            page.fill('#startTime', self.timeRange)
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#endDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.todayDate
            )
            page.fill('#endTime', self.timeRange)
            log(job_id, "Inserted Filter")
            page.select_option("#pageSize", "1000")
            log(job_id, "Set results per page to 1000")
            
            # page.click('a.btn.btn-primary >> text=Search')
            self.wait_for_navigation(page, job_id)
            
            
            while True:
                with page.expect_response(lambda r: "creditAllocatedLog" in r.url) as resp_info:
                    page.click('#searchBtn')

                response = resp_info.value
                log(job_id, "Data Result Display")
                if "application/json" in response.headers.get("content-type", ""):
                    data = response.json()
                    
                    data_list = data.get("data", [])
                    log(job_id, "Getting the data through Network Response")


                    for entry in data_list:
                        filtered = self.filter_ftd_deposit_withdrawal_data(entry, global_index, "withdrawal")
                        all_results.append(filtered)
                        global_index += 1
                    
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
                    
            log(job_id, f"📌 Final Global Index Count: {global_index}")
            return all_results   
                 
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False
    
    def filter_and_summarize_data(self, results_by_identifier):
        summarized_data = []
        
        for user_id, records in results_by_identifier.items():
            # Calculate totals for multiple fields
            clean_UserID = user_id.replace('BADSHA__', '')
            validTurnover = sum(record.get('turnover', 0) for record in records)
            # totalWinLoss = sum(record.get('playerWinLoss', 0) for record in records)
            # total_bet_count = sum(record.get('betCount', 0) for record in records)
            totalJackpot = sum(record.get('jackpotBetAmt', 0) for record in records)
            totalPL = sum(record.get('playerWinLoss', 0) for record in records)
            totalWinLoss = totalJackpot + totalPL

            
            summarized_data.append({
                'User ID': clean_UserID,
                'Name': records[0].get('accountName', ''),
                'Active Player': records[0].get('activePlayer', ''),
                'M Win/loss': totalWinLoss,
                'Jackpot Win/Loss': totalJackpot,
                'Valid Turnover': validTurnover,
                # 'total_bets': total_bet_count,
                'Total P/L': totalPL,
                # 'currency': records[0].get('"accountName": ', '') if records else '',  # assuming same for all records
                # 'platform': records[0].get('platform', '') if records else ''
            })
        
        return summarized_data

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

    def vt_apl_tpl_data(self, page, job_id):
        log(job_id, "Scraping For vt_apl_tpl_data Data")
        all_results = []
        # self.date= "10-08-2025"
        retries = 0
        while retries < self.max_retries:
            log(job_id, "Trigger Sidebar")
            page.locator('.navDropdown').nth(2).click()
            page.locator('.navDropdown').nth(2).locator('#reportDetailItem').click()
            self.wait_for_navigation(page, job_id)
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#startDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.yesterdayDate # Not Sure date
            )
            page.evaluate(
                """(date) => {
                    let el = document.querySelector('#endDate');
                    el.value = date;
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                self.yesterdayDate # Not Sure date
            )
            log(job_id, "Inserted Filter")
            
            self.wait_for_navigation(page, job_id)
            page.click('#queryReport')
            time.sleep(5)

            page.wait_for_selector("#tbodyAgent tr#tempTitle")  # wait for top-level rows

            data = []
            rows = page.query_selector_all("#tbodyAgent tr#tempTitle")

            for row in rows:
                cols = [cell.inner_text().strip() for cell in row.query_selector_all("td")]
                # If the table has fixed columns: User ID, Turnover, Name
                if len(cols) >= 3:
                    data.append({
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
                        "Self Comm.": row.query_selector("td#userTotalselfWinloss").inner_text().strip(),
                        "Total P/L (Self)": row.query_selector("td#userTotalselfComm").inner_text().strip(),
                        "Company": self.get_company_value(row)
                    })

            # Show clean result
            return data
            # return all_results            
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False
    
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

                    if self.authentication(page, job_id):
                        nsu_data = self.nsu_data(page, job_id)
                        log(job_id, "NSU Data Scraping Finished")
                        log(job_id, f"NSU Data: {nsu_data}")
                        time.sleep(3)

                        ftd_data = self.ftd_data(page, job_id)
                        log(job_id, "FTD Data Scraping Finished")
                        log(job_id, f"FTD Data: {ftd_data}")
                        time.sleep(3)

                        deposit_data = self.deposit_data(page, job_id)
                        log(job_id, "FTD Data Scraping Finished")
                        # log(job_id, f"Deposit Data: {deposit_data}")
                        time.sleep(3)
                        
                        withdrawal_data = self.withdrawal_data(page, job_id)
                        # log(job_id, f"Withdrawal Data: {withdrawal_data}")

                        vt_apl_tpl_data = self.vt_apl_tpl_data(page, job_id)
                        # log(job_id, f"vt_apl_tpl_data: {vt_apl_tpl_data}")

                        log(job_id, f"Scraping Done: {self.username}")

                        data = {
                            "status": 200,
                            "text": "Data Fetched successfully",
                            "title": "Fetch Completed!",
                            "icon": "success",
                            "NSU": nsu_data,
                            "FTD" : ftd_data,
                            "DEPOSIT": deposit_data,
                            "WITHDRAWAL": withdrawal_data,
                            "VT/APL/TPL": vt_apl_tpl_data

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
