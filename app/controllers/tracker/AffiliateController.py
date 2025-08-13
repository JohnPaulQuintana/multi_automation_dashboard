from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timedelta
from urllib.parse import urlparse
from app.automations.log.state import log
from collections import defaultdict
import re
import time

class AffiliateController:
    def __init__(self, login_url, brand, username, password, currency, platform, rangeDate):
        self.url = login_url
        self.brand = brand
        self.username = username
        self.password = password
        self.currency = currency
        self.platform = platform
        self.startDate = rangeDate
        self.max_retries = 3

        parsed_url = urlparse(self.url)
        self.domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

    def combine_nsu_ftd(nsu_dict, ftd_dict, date_string):
        combined = []
        all_keywords = set(nsu_dict.keys()) | set(ftd_dict.keys())  # union

        for keyword in sorted(all_keywords):
            nsu = nsu_dict.get(keyword, 0)
            ftd = ftd_dict.get(keyword, 0)
            combined.append([keyword, nsu, ftd, date_string])
        
        return combined

    def parse_display_date(self, reg_time_text):
        try:
            match = re.match(r"(.*?) \(GMT([+-]\d{2}):?(\d{2})?\)", reg_time_text)
            if not match:
                return None
            datetime_part = match.group(1).strip()
            offset_hour = int(match.group(2))
            offset_minute = int(match.group(3)) if match.group(3) else 0
            original_dt = datetime.strptime(datetime_part, "%Y/%m/%d %H:%M:%S")
            offset = timedelta(hours=offset_hour, minutes=offset_minute)
            local_dt = original_dt - offset + timedelta(hours=8)  # Convert to Asia/Manila (GMT+8)
            return local_dt.strftime("%m/%d/%Y")
        except:
            return None
    
    def extract_keyword_ftd_only(self, page, rangeDate):
        keyword_counts = defaultdict(int)
        keyword_dates = {}

        # Wait for the table to load
        page.wait_for_selector("#performanceTable tbody tr")

        while True:
            rows = page.query_selector_all("#performanceTable tbody tr")
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 3:
                    continue
                
                datetime_text = cells[6].inner_text().strip()
                date_only = datetime_text.split(" ")[0] # Strip the Time

                date = datetime.strptime(rangeDate, "%Y-%m-%d").strftime("%Y/%m/%d")
                if date_only != date:
                    continue
                keyword = cells[2].inner_text().strip()
                keyword_counts[keyword] += 1

            # Look for "Next" button
            next_button = page.query_selector("#performanceTable_next")
            if next_button and "disabled" not in next_button.get_attribute("class"):
                first_row_text = page.query_selector(
                    "#performanceTable tbody tr:first-child"
                ).inner_text()

                next_button.click()

                try:
                    page.wait_for_function(
                        f"document.querySelector('#performanceTable tbody tr:first-child') && "
                        f"document.querySelector('#performanceTable tbody tr:first-child').innerText !== {repr(first_row_text)}",
                        timeout=8000
                    )
                except TimeoutError:
                    print("Timed out waiting for next page load")
                    break
            else:
                break

        return keyword_counts

    def extract_keyword_nsu_only(self, page):
        keyword_counts = defaultdict(int)
        keyword_dates = {}

        # Wait for the table to load
        page.wait_for_selector("#registrationsTable tbody tr")

        while True:
            rows = page.query_selector_all("#registrationsTable tbody tr")
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 3:
                    continue

                keyword = cells[2].inner_text().strip()
                reg_time_text = cells[4].inner_text().strip()

                # Parse and format the date
                formatted_date = self.parse_display_date(reg_time_text)
                keyword_dates[keyword] = formatted_date

                keyword_counts[keyword] += 1

            # Check for the "Next" button
            next_button = page.query_selector("#registrationsTable_next")
            if next_button and "disabled" not in next_button.get_attribute("class"):
                first_row_text = page.query_selector(
                    "#registrationsTable tbody tr:first-child"
                ).inner_text()

                next_button.click()

                try:
                    page.wait_for_function(
                        f"document.querySelector('#registrationsTable tbody tr:first-child') && "
                        f"document.querySelector('#registrationsTable tbody tr:first-child').innerText !== {repr(first_row_text)}",
                        timeout=8000
                    )
                except TimeoutError:
                    print("Timed out waiting for next page load")
                    break
            else:
                break

        result = {}
        for keyword, count in keyword_counts.items():
            result[keyword] = (count, keyword_dates.get(keyword))
        return result

    
    def extract_table_data(self, page, job_id):
        # ====== Scraping For NSU =========
        rangeDate = datetime.strptime(self.startDate, "%m/%d/%Y").strftime("%Y-%m-%d")
        try:
            log(job_id, f"original Date: {self.startDate}")
            # formatted_date = originaldate.replace('/','-')
            # formatted_date = "2025-07-29"
            log(job_id, f"Date Format: {rangeDate}")
            page.fill("//*[@id=\"registrationsForm\"]/div/div[2]/div[2]/div[1]/input", rangeDate)
            time.sleep(1)
            page.fill("//*[@id=\"registrationsForm\"]/div/div[2]/div[2]/div[2]/input", rangeDate)

            log(job_id, "Filled successfully for input date.")
        except PlaywrightTimeoutError:
            log(job_id, "Failed to fill date input.")
                
        self.wait_for_navigation(page, job_id)
        time.sleep(1)

        page.select_option('select[name="registrationsTable_length"]', value="100")
        log(job_id, "Selected 100 entries from the dropdown.")

        try:
            self.wait_for_navigation(page, job_id)
            time.sleep(2)
            # return True
        except TimeoutError:
            # logging.warning(f"Tab trigger failed, retrying {retries + 1}/{self.max_retries}...")
                # retries += 1
            time.sleep(3)

        data = self.extract_keyword_nsu_only(page)
        log(job_id, "Finished Scraping For NSU")

        # ===== Scraping for FTD ========
        fdt_btn = '//*[@id="menu_8"]'
        try:
            # Try using JavaScript to click the second menu button
            page.eval_on_selector(f'xpath={fdt_btn}', "element => element.click()")
            log(job_id, "FTD button clicked using JavaScript.")
        except PlaywrightTimeoutError:
            log(job_id, "Failed to click the FTD button using JavaScript.")
            # Wait for the network to be idle after clicking
            page.wait_for_load_state('networkidle')
            time.sleep(3)

        page.wait_for_selector('input[name="startTime"]', state='visible')
 
        page.fill("//*[@id=\"performanceForm\"]/div[3]/div[1]/input", rangeDate)
        time.sleep(1)
        page.fill("//*[@id=\"performanceForm\"]/div[3]/div[2]/input", rangeDate)
        log(job_id, "Filled successfully for input date.")
        time.sleep(1)

        self.wait_for_navigation(page, job_id)
        time.sleep(1)

        page.select_option('select[name="performanceTable_length"]', value="100")
        log(job_id, "Selected 100 entries from the dropdown.")

        try:
            self.wait_for_navigation(page, job_id)
            time.sleep(1)
            # return True
        except TimeoutError:
            # logging.warning(f"Tab trigger failed, retrying {retries + 1}/{self.max_retries}...")
                # retries += 1
            time.sleep(3)
        time.sleep(1)

        ftd_data = self.extract_keyword_ftd_only(page, rangeDate)

        # ======= Merging the Results ========
        example_date = next(iter(data.values()))[1] if data else datetime.strptime(rangeDate, "%Y-%m-%d").strftime("%m/%d/%Y")
        

        combined_rows = []
        all_keywords = set(data.keys()) | set(ftd_data.keys())

        for keyword in sorted(all_keywords):
            nsu = data.get(keyword, (0, example_date))[0]
            ftd = ftd_data.get(keyword, 0)
            combined_rows.append((example_date, self.brand, self.username, self.currency, self.platform, keyword, nsu, ftd))
        if not combined_rows:
            combined_rows.append((example_date, self.brand, self.username, self.currency, self.platform, "-", 0, 0))

        return combined_rows
        
    def trigger_sidebar(self, page, job_id):
        retries = 0
        while retries < self.max_retries:
            log(job_id, "Trigger Sidebar")
            menu_selector = '//*[@id="menu_4"]'
            menu_selector_nsu = '//*[@id="menu_7"]'
            time.sleep(1)
            try:
                # Try using JavaScript to click the first menu button
                page.wait_for_selector(menu_selector, timeout=10000)
                page.eval_on_selector(f'xpath={menu_selector}', "element => element.click()")
                log(job_id, "Registration button clicked using JavaScript.")
            except PlaywrightTimeoutError:
                log(job_id, "Failed to click the menu button using JavaScript.")
            
            # Wait for the network to be idle after clicking
            self.wait_for_navigation(page, job_id)
            
            try:
                # Try using JavaScript to click the second menu button
                page.wait_for_selector(menu_selector_nsu, timeout=10000)
                page.eval_on_selector(f'xpath={menu_selector_nsu}', "element => element.click()")
                log(job_id, "NSU button clicked using JavaScript.")
            except PlaywrightTimeoutError:
                log(job_id, "Failed to click the NSU button using JavaScript.")
            
            # Wait for the network to be idle after clicking
            self.wait_for_navigation(page, job_id)
            
            try:
                log(job_id, "Menu item clicked.")
                self.wait_for_navigation(page, job_id)
                time.sleep(1)
                return True
            except TimeoutError:
                log(job_id, f"Tab trigger failed, retrying {retries + 1}/{self.max_retries}...")
                retries += 1
                time.sleep(2)
        
        log(job_id, "Failed to trigger sidebar after several attempts.")
        return False
    
    def wait_for_navigation(self, page, job_id):
        try:
            page.wait_for_load_state('networkidle')
            return True
        except TimeoutError:
            log(job_id, "Navigation wait timeout.")
            return False
        
    def authentication(self, page, job_id):
        try:
            log(job_id, f"brand: {self.brand}, Username: {self.username}, Password: {self.password}")
            page.fill('input[name="userId"]', self.username)
            time.sleep(3)
            page.fill('input[name="password"]', self.password.replace(" ", ""))
            time.sleep(3)
            page.click('button#login')
            self.wait_for_navigation(page, job_id)
            log(job_id, "Authenticated Successfull")
            return True

        except Exception as e:
            log(job_id, f"Error Authentication: {e}")
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
                        self.trigger_sidebar(page, job_id)
                        table_data = self.extract_table_data(page, job_id)
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
                        
                        return table_data
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
