from playwright.async_api import async_playwright
from app.automations.log.state import log
from datetime import datetime, timedelta
import asyncio
import os
import json
import time

class supersetScraping:
    def __init__(self, username, password, brand, currency, timeGrain, startDate, endDate):
        self.username = username
        self.password = password
        self.brand = brand
        self.currency = currency
        self.timeGrain = timeGrain
        self.startDate = startDate
        self.endDate = endDate

    # ────────────────────────────────────────────────────────────
    # Scraping Method on the Network Request 
    # ────────────────────────────────────────────────────────────
    # def json_save(self, job_id, data, filename):
    #     json_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".", "json"))
    #     output_path = os.path.join(json_dir, filename)  
    #     with open(output_path, 'w', encoding='utf-8') as f:
    #         json.dump(data, f, ensure_ascii=False, indent=2)
    #     log(job_id, f"🎯 Results saved to {output_path}")

    def missing_date(self, merged_results, startDate, raw_end_date):
        if self.timeGrain == "Day":
            start_date = datetime.strptime(startDate, "%Y-%m-%d")
            end_date = datetime.strptime(raw_end_date, "%Y-%m-%d") - timedelta(days=1)

            # Generate all dates in the range
            all_dates = {
                (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                for i in range((end_date - start_date).days + 1)
            }

            # Get existing dates from merged_results
            existing_dates = set(merged_results.keys())

            # Identify missing dates
            missing_dates = all_dates - existing_dates

            # Add empty placeholders for missing dates
            for date in missing_dates:
                merged_results[date] = {"Date": date}

            # Return a dict sorted by date key
            return dict(sorted(merged_results.items()))
        else:
            if not merged_results:
                return {
                    start_date : {
                        "Date": start_date
                    }
                }
            return merged_results

    def merge_result(self,job_id, all_result):
        merged_results = {}
        for sql_file_name, records in all_result.items():
            log(job_id, f"Processing {sql_file_name}...")  # Optional logging for debugging
            for entry in records:
                date_raw = entry.get("Date", "")
                if not date_raw:
                    continue

                # Standardize date: use YYYY-MM-DD
                date_key = date_raw.split(" ")[0]
                entry["Date"] = date_key

                # Initialize the date section if needed
                if date_key not in merged_results:
                    merged_results[date_key] = {}

                # Product Type-based grouping
                product_type = entry.get("PRD Product Type")
                if product_type:
                    # Make a shallow copy and remove Date
                    clean_entry = dict(entry)
                    clean_entry.pop("Date", None)

                    if "PRD Products" not in merged_results[date_key]:
                        merged_results[date_key]["PRD Products"] = []
                    merged_results[date_key]["PRD Products"].append(clean_entry)
                    continue  # done with this entry

                # Purpose-based grouping
                purpose = entry.get("PMT Purpose", "").strip()
                if purpose:
                    # Make a shallow copy and remove Date
                    clean_entry = dict(entry)
                    clean_entry.pop("Date", None)

                    if "PMT Purpose" not in merged_results[date_key]:
                        merged_results[date_key]["PMT Purpose"] = []
                    merged_results[date_key]["PMT Purpose"].append(clean_entry)
                    continue  # done with this entry

                # Otherwise: treat as summary data, merge flat into date
                merged_results[date_key].update(entry)

        return merged_results

    async def process_response(self, response, result):
        try:
            json_data = await response.json()
            if not result.done():
                result.set_result(json_data)
        except Exception as e:
            print(f"Error processing response: {e}")

    async def handle_response(self, response, result):
        try:
            if "superset/sql_json/" in response.url and response.status == 200:
                json_body = await response.json()
                if "error" in json_body:
                    if not result.done():
                        result.set_exception(Exception(f"Query Error: {json_body['error']}"))
                    return
                
                asyncio.create_task(self.process_response(response, result))
        except Exception as e:
            if not result.done():
                result.set_exception(e)

    async def sql_run(self, page, job_id, file_path, sql_files):
        try:
            with open(file_path, 'r') as f:
                sql = f.read()
                sql = sql.replace("{{start_date}}", self.startDate)
                sql = sql.replace("{{end_date}}", self.endDate)
                sql = sql.replace("{{time_grain}}", self.timeGrain)
                sql = sql.replace("{{currency}}", self.currency)

            for attempt in range(3):
                try:
                    log(job_id, f"Processing: {sql_files}")
                    await page.wait_for_selector('#ace-editor')
                    await page.click('#ace-editor')
                    await page.keyboard.press('Control+A')
                    await page.keyboard.press('Backspace')
                    time.sleep(3)
                    log(job_id, f'Injecting SQL Query: {sql_files}')
                    await page.evaluate("""
                        (sql) => {
                            const editor = ace.edit("ace-editor");
                            editor.setValue(sql, -1);  // -1 keeps cursor at the beginning
                        }
                    """, sql)

                    # Set up a listener to capture the desired network response asynchronously
                    result_future = asyncio.get_event_loop().create_future()
                    page.on("response", lambda response: self.handle_response(response, result_future))

                    # SQL Execution
                    stime = time.time()
                    await page.click('button.superset-button.cta:has-text("Run")')
                    log(job_id, f"{sql_files} : Executing SQL... ")

                    json_result = await asyncio.wait_for(result_future, timeout=60)
                    elapsed = (time.time() - stime)
                    await asyncio.sleep(0.5)


                    log(job_id, f"✅ {sql_files} completed in {elapsed:.2f} seconds.")
                    data = json_result.get("data", [])
                    return {sql_files: data}
                except Exception as e:
                    log(job_id, f"⚠️ Attempt {attempt + 1} failed for {sql_files}: {e}")
                    await asyncio.sleep(5)

            log(job_id, f"❌ Retry attempts failed for {sql_files}")
            raise Exception(f"Failed to execute query after 3 attempts: {sql_files}")
        
        except Exception as e:
            print(f"Error executing {sql_files}: {str(e)}")
    
    def get_sql_files(self, job_id, brand):
        "Return List of SQL Files. ./sql/(name).sql"
        print("getting SQL FILE")
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
        sql_dir = os.path.join(project_root, 'sql', brand)
        print("Looking inside:", sql_dir)
        return [ 
            os.path.join(sql_dir, f) for f in os.listdir(sql_dir) if f.endswith('.sql')
        ]
    
    async def authenticate(self, page, job_id):
        print("Authentication on Process")
        log(job_id, "Authentication on Process")
        await page.fill('input[name="username"]', self.username)
        await page.fill('input[name="password"]', self.password)
        await asyncio.sleep(.5)
        await page.click('input[type="submit"]')
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(1.5)
        log(job_id, "Authenticated Success")
        
    async def scraping(self, job_id):
        log(job_id, f"Scraping started with: brand={self.brand}, currency={self.currency}, timeGrain={self.timeGrain}, start={self.startDate}, end={self.endDate}")
        try:
            start_time = time.time()
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                page = await browser.new_page()
                await page.goto('https://ar0ytyts.superdv.com/superset/sqllab/')
                await self.authenticate(page, job_id)

                sql_files = self.get_sql_files(job_id, self.brand)
                all_results = {}
                for file_path in sql_files:
                    try:
                        sql_file = os.path.basename(file_path)
                        result_data = await self.sql_run(page, job_id, file_path, sql_file)
                        all_results.update(result_data)
                        await asyncio.sleep(3)
                    except Exception as e:
                        log(job_id, f"❌ Aborting due to failure in {sql_file}: {e}")
                        await browser.close()
                        return {
                            "status": 500,  # Use 500 for internal server error
                            "text": f"Failed to complete automation. Error in {sql_file}: {e}",
                            "data": {}
                        }
                await browser.close()
    
                merged_results = self.merge_result(job_id, all_results)
                # merged_list = [{"Date": date, **data} for date, data in merged_results.items()]
                complete_date = self.missing_date(merged_results, self.startDate, self.endDate)

                # Save result Not needed
                # self.json_save(job_id, complete_date, filename="result.json")

            end_time = time.time() - start_time
            total_minutes = end_time / 60
            log(job_id, f"🕒 Total scraping time: {total_minutes:.2f} minutes")

            return {
                "status": 200,
                "text": "Scraping and Data has been saved successfully",
                "data": complete_date,
            }
        except Exception as e:
            log(job_id, f"❌ Job failed...Authentication error:\n{e}")
            print(f"Authentication error:\n{e}")
            return {
                "status": 500,  # Server error in case of unexpected exceptions
                "text": f"Error: {str(e)}",
                "data": {}
            }