from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timedelta
from urllib.parse import urlparse
from app.automations.log.state import log
from collections import defaultdict
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
            time.sleep(1)
            page.fill('input#password', self.password.replace(" ", ""))
            time.sleep(1)
            page.click('button[type="submit"]')
            self.wait_for_navigation(page, job_id)
            log(job_id, "Authenticated Successfull")
            time.sleep(10)
            return True
        except Exception as e:
            log(job_id, f"Error Authentication: {e}")
            return False
    def filter_user_data(self, data, index):
        return {
            "#": index + 1,
            "userId": data.get("userId"),
            "loginName": data.get("loginName"),
            "createTime": data.get("createTime"),
            "updateTime": data.get("updateTime"),
            "name": data.get("name"),
            "email": data.get("email"),
            "tel": data.get("tel"),
            "firstDeposit": data.get("firstDeposit"),
            "type": "New Signup",
            "status": "PENDING",
            "kyc": data.get("kyc")
        }
        

    def nsu_data(self,page, job_id):
        log(job_id, "Scraping For NSU Data")
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
            # page.click('a.btn.btn-primary >> text=Search')
            self.wait_for_navigation(page, job_id)
            

            with page.expect_response(lambda r: "queryIdentityVerificationForm" in r.url) as resp_info:
                page.click('a.btn.btn-primary >> text=Search')

            response = resp_info.value
            log(job_id, "Data Result Display")
            if "application/json" in response.headers.get("content-type", ""):
                data = response.json()
                
                data_list = data.get("data", [])
                log(job_id, "Getting the data through Network Response")
                for index, entry in enumerate(data_list):
                    filtered = self.filter_user_data(entry, index)
                    log(job_id, filtered)
                
            else:
                html_text = response.text()
                print("📄 HTML Data:", html_text)
            # rows = page.query_selector_all("table.tb-mult tbody tr")
            # data = []
            # for i, row in enumerate(rows, start=1):  # start numbering at 1
            #     cells = row.query_selector_all("td")
            #     if not cells:
            #         continue
            #     state = cells[1].inner_text().strip()
            #     created_time = cells[2].inner_text().strip()
            #     member_info = cells[3].inner_text().strip()
            #     verify_info = cells[4].inner_text().strip()
            #     first_deposit = cells[5].inner_text().strip()

            #     data.append({
            #         "#": i,  # ← running number
            #         "State": state,
            #         "Created/Updated": created_time,
            #         "Member Info": member_info,
            #         "Verify Info": verify_info,
            #         "First Deposit": first_deposit,
            #     })
            # log(job_id, "data")
            time.sleep(10)
            return True
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
                        # self.trigger_sidebar(page, job_id)
                        # table_data = self.extract_table_data(page, job_id)
                        nsu_data = self.nsu_data(page, job_id)
                        # log(job_id, f"Extracted Data: {table_data}, {table_data_ftd}")
                        log(job_id, f"Scraping Done: {self.username}")    
                        # data = {
                        #     "status": 200,
                        #     "text": "Data Fetched successfully",
                        #     "title": "Fetch Completed!",
                        #     "icon": "success",
                        #     "fe": table_data,
                        #     "ftd" : table_data_ftd,
                        # }
                        
                        return {
                            "status": "200"
                        }
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
