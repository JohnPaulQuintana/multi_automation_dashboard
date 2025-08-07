from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN

# from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from app.controllers.business_process.spreadsheet_extractor import GoogleSheetURLParser
from app.controllers.business_process.key_mapping import key_daily_map, key_weekly_map, key_monthly_map, jb_monthly_map
from app.automations.log.state import log
from datetime import datetime
import time
import logging
import json
import os

class Spreadsheet:
    def __init__(self, brand, currency, timeGrain, startDate, endDate, data):
        self.brand = brand
        self.currency = currency
        self.timeGrain = timeGrain
        self.starDate = startDate
        self.endDate = endDate
        self.json_data = data

        self.scope = ["https://www.googleapis.com/auth/spreadsheets"]
        config_dict = {
            "type": TYPE,
            "project_id": PROJECT_ID,
            "private_key_id": PRIVATE_KEY_ID,
            "private_key": PRIVATE_KEY,
            "client_email": CLIENT_EMAIL,
            "client_id": CLIENT_ID,
            "auth_uri": AUTH_URI,
            "token_uri": TOKEN_URI,
            "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": CLIENT_X509_CERT_URL,
            "universe_domain": UNIVERSE_DOMAIN,
        }
        # service_account_file = os.path.join(os.path.dirname(__file__), "BI-Gcredentials.json")
        self.creds = Credentials.from_service_account_info(config_dict, scopes=self.scope)
        self.service = build("sheets", "v4", credentials=self.creds)
        self.links = { # Original Links
            "BAJI": {
                "BDT": "https://docs.google.com/spreadsheets/d/152CJzozRocf7DD8DCf949zIbq-7SWJtHIZU2aCAbHxE/edit?gid=0#gid=0",
                "INR": "https://docs.google.com/spreadsheets/d/1O3eM2QA_WQOk4Jeb-rOmBj89kq3Ugyo3jzvLxFd0gOg/edit?gid=0#gid=0",
                "PKR": "https://docs.google.com/spreadsheets/d/1vBejYz8pBUTmoFD6444WoEEJQG40rxT-bLDhjKLB4ik/edit?gid=0#gid=0",
                "NPR": "https://docs.google.com/spreadsheets/d/1kxn12rsQEIwo_vhy9pW8_aFf1QTiP2QV8YShx3nk5_Q/edit?gid=0#gid=0"
            },
            "S6": {
                "BDT": "https://docs.google.com/spreadsheets/d/1NnFYpJZ5gm0vDavZddZlB1bzzOjrT2FxBD9p-X307sM/edit?gid=0#gid=0",
                "INR": "https://docs.google.com/spreadsheets/d/1c1tjP-yS3TyHMhz3o4n4A_Sj5HVs2IW7RhABEycRSv0/edit?gid=0#gid=0",
                "PKR": "https://docs.google.com/spreadsheets/d/1z0tnZ-bOC4J5yhJdQnGuuNj1_D5kMlw_OoWe28dNrgQ/edit?gid=0#gid=0"
            },
            "JB": {
                "BDT": "https://docs.google.com/spreadsheets/d/1vclzD57w7lVacscwgDO0Z68krX6c7bXKltwEhUp6ouY/edit?gid=0#gid=0",
                "INR": "https://docs.google.com/spreadsheets/d/1Zk0ZQIekRltmcUBvOBarv7nSbr20DI89DMXI3FM1SIo/edit?gid=0#gid=0",
                "PKR": "https://docs.google.com/spreadsheets/d/1JYqBy6DQ51cl9EmHm7nugmCZaIQyrb1O5e5_HOLMwd4/edit?gid=0#gid=0"
            },
            "CTN": {
                "AUD": "https://docs.google.com/spreadsheets/d/1k2zV1uwoGwXmpnJst3oQzsZ-IFTjcj-_6N5avPGtwgA/edit?gid=0#gid=0",
                "CNY": "https://docs.google.com/spreadsheets/d/1Ao1NZWrYJxYeDNZx7DXPTBE1kmS8vyTJ9R8k4DjdM1U/edit?gid=0#gid=0",
                "SGD": "https://docs.google.com/spreadsheets/d/1IZP_lf9Sj_pCUAMt3frFtt04zBoRiovjcUeDSZRZJ0Y/edit?gid=0#gid=0",
                "HKD": "https://docs.google.com/spreadsheets/d/1MKQYxOTYfDiWi1XAU5bEsenfQ_v3UWk7xzLE-Dmau08/edit?gid=0#gid=0",
                "MYR": "https://docs.google.com/spreadsheets/d/1Z4J3CSaGcN-DuLnmir6h3LZW0LfIeN_PNiN3dfNve0Q/edit?gid=0#gid=0"
            }
        }
        self.test_links = { # Replace Beta-Test Link if need's Testing
            "BAJI": { # Change for Real Googlesheet URL (Beta Testing URL)
                "BDT": "https://docs.google.com/spreadsheets/d/1bisw2oX8OdpO1idTBtAh-Q_Ui-clGJ6qOvuu8ZmRD2E/edit?gid=0#gid=0",
                "INR": "https://docs.google.com/spreadsheets/d/1L6GoN7NDi1TSbjNmFPW97lS2VOSpVrI79cpZ-iuLm-k/edit?gid=0#gid=0",
                "PKR": "https://docs.google.com/spreadsheets/d/1nsfXw4PoxeFUHDHszcxpRDWZ-wGpIb-kTxJzCfqlpUM/edit?gid=0#gid=0",
                "NPR": "https://docs.google.com/spreadsheets/d/1St0u8qCPyFjJBowWWg20N4jsH8hDgnNMsFu3fHmYwVA/edit?gid=0#gid=0"
            },
            "S6": {
                "BDT": "https://docs.google.com/spreadsheets/d/11yjjGFjKeodndLVqUPOpIkMnB_02MmdPwIKuaMZiWY8/edit?gid=0#gid=0",
                "INR": "https://docs.google.com/spreadsheets/d/1kkFV_sNPJ3QBpfobZx_JmKw5fYgwm24U4Vbyaxj7Zwg/edit?gid=0#gid=0",
                "PKR": "https://docs.google.com/spreadsheets/d/19WOHV3OmhYMSg4PF05lDFvR-6zOB6NuA6m8MOZXIibA/edit?gid=0#gid=0"
            },
            "JB": {
                "BDT": "https://docs.google.com/spreadsheets/d/11V2OiSw_ztEKd1oKnTjT5YHMhmZnknFtEGI_TXSTDuQ/edit?gid=0#gid=0",
                "INR": "https://docs.google.com/spreadsheets/d/1pbnM95UhA0aIxoRaJlekqjHSTFsxZcXIyRVb_tJgfts/edit?gid=0#gid=0",
                "PKR": "https://docs.google.com/spreadsheets/d/10u4d6qu1pzpHgpdBKyuC9JNq69sIThnRuExl2TU7_YU/edit?gid=0#gid=0"
            },
            "CTN": {
                "AUD": "https://docs.google.com/spreadsheets/d/11n97ZKv7rqZGalln40SeDaLZLl1dVv00I6hu2KzbGsA/edit?gid=0#gid=0",
                "CNY": "https://docs.google.com/spreadsheets/d/1X-g2-SgVS7uf1UT75bScyB8GXCom8R1DiUSiFlLA8Ls/edit?gid=0#gid=0",
                "SGD": "https://docs.google.com/spreadsheets/d/1na6cF9arI2l25b1WSv9xx0C7CJyoyR-lqE8TKBqCdN8/edit?gid=0#gid=0",
                "HKD": "https://docs.google.com/spreadsheets/d/11ftPVIHH9dKojowsujyqlWDjg5wNrKRzn7v01pbesLg/edit?gid=0#gid=0",
                "MYR": "https://docs.google.com/spreadsheets/d/126DJVMzl5od8NDFnZMb9m2J80n5GGSV72PKZqm1C8-o/edit?gid=0#gid=0"
            }
        }
    
    def non_monthly_key_map(self, key_map, json_row, sheet_range, column_index):
        non_monthly_pmt_fields = [
            ("PMT Bonus Cost", 0), 
            ("PMT Total Claimed", 1), 
            ("PMT Total Unique Player Claimed", 2),
            ("Rank_1", 3), 
            # Rank_2 to Rank_10 share offset 4 (we'll pick the first non-"-" from Rank_10 → Rank_2)
            ("Rank_2", 4), ("Rank_3", 4), ("Rank_4", 4), ("Rank_5", 4), 
            ("Rank_6", 4), ("Rank_7", 4), ("Rank_8", 4), ("Rank_9", 4), ("Rank_10", 4)
        ]
        update_requests = []
        
        # Process bonuses that exist in the data
        for bonus in json_row.get("PMT Purpose", []):
            purpose = bonus.get("PMT Purpose")
            mapping = key_map.get(purpose)
            if not mapping:
                print(f"Skipping unmapped bonus purpose: {purpose}")
                continue

            base_row = mapping["row"]
            
            # Process non-rank fields normally
            for field, offset in non_monthly_pmt_fields:
                if not field.startswith("Rank_"):
                    value = bonus.get(field)
                    update_requests.append({
                        "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + offset + 1}",
                        "values": [[value]]
                    })
            
            # Special handling for ranks: Check Rank_10 → Rank_2 and take the first non-"-" value
            rank_value = "-"
            for rank in ["Rank_10", "Rank_9", "Rank_8", "Rank_7", "Rank_6", 
                        "Rank_5", "Rank_4", "Rank_3", "Rank_2"]:
                value = bonus.get(rank, "-")
                if value != "-":
                    rank_value = value
                    break  # Take the first valid value (starting from Rank_10)
                    
            # Update the shared Rank position (offset 4) with the found value
            update_requests.append({
                "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + 4 + 1}",
                "values": [[rank_value]]
            })
            
            # Still include Rank_1 separately (offset 3)
            rank_1_value = bonus.get("Rank_1", "-")
            update_requests.append({
                "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + 3 + 1}",
                "values": [[rank_1_value]]
            })
        
        # Handle missing purposes (set all to 0 or "-")
        for purpose, mapping in key_map.items():
            if purpose in ["Acquisition", "Retention", "Conversion", "Reactivation"]:
                found = any(p.get("PMT Purpose") == purpose for p in json_row.get("PMT Purpose", []))
                
                if not found:
                    base_row = mapping["row"]
                    for field, offset in non_monthly_pmt_fields:
                        if field in ["PMT Bonus Cost", "PMT Total Claimed", "PMT Total Unique Player Claimed"]:
                            value = 0
                        elif field.startswith("Rank_"):
                            value = '-'
                        else:
                            continue
                        update_requests.append({
                            "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + offset + 1}",
                            "values": [[value]]
                        })
        
        return update_requests

    def monthly_key_map(self, key_map, json_row, sheet_range, column_index):
        monthly_pmt_fields = [
            ("PMT Bonus Cost", 0), ("PMT Total Claimed", 1), ("PMT Total Unique Player Claimed", 2),
            ("Rank_1", 4), ("Rank_2", 5), ("Rank_3", 6), ("Rank_4", 7), ("Rank_5", 8), ("Rank_6", 9),
            ("Rank_7", 10), ("Rank_8", 11), ("Rank_9", 12), ("Rank_10", 13)
        ]
        update_requests = []
        for bonus in json_row.get("PMT Purpose", []):
            purpose = bonus.get("PMT Purpose")
            mapping = key_map.get(purpose)
            if not mapping:
                print(f"Skipping unmapped bonus purpose: {purpose}")
                continue

            base_row = mapping["row"]
            for field, offset in monthly_pmt_fields:
                value = bonus.get(field)
                if value is not None:
                    update_requests.append({
                        "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + offset + 1}",
                        "values": [[value]]
                    })
        for purpose, mapping in key_map.items():
            # Check if the product type is in the valid list
            if purpose in ["Acquisition", "Retention", "Conversion", "Reactivation"]:
                # Check if the product type exists in json_row["PRD Products"]
                found = any(p.get("PMT Purpose") == purpose for p in json_row.get("PMT Purpose", []))

                if not found:
                    # If the product type is missing, set all related fields to 0 (for monthly PMT)
                    base_row = mapping["row"]
                    for field, offset in monthly_pmt_fields:
                        if field in ["PMT Bonus Cost", "PMT Total Claimed", "PMT Total Unique Player Claimed"]:
                            value = 0
                        elif field in ["Rank_1", "Rank_2", "Rank_3", "Rank_4", "Rank_5", "Rank_6", "Rank_7", "Rank_8", "Rank_9", "Rank_10"]:
                            value = '-'
                        else:
                            continue

                        update_requests.append({
                            "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + offset + 1}",
                            "values": [[value]]  # Default value for missing product type in monthly PMT
                        })
        
                else:
                    # If the product type is found, use its data (already handled in the first loop)
                    continue
        return update_requests
    
    def key_map_update(self, key_map, json_row, sheet_range, column_index):
        prd_fields = [
            ("PRD Number of Unique Player", 1),
            ("PRD Total Turnover", 2),
            ("PRD Profit/Loss", 3),
            ("PRD Margin", 4)
        ]
        keys_to_skip = ["Sport", "SLOT", "CASINO", "TABLE", "SLOT", "P2P", "FH", "LOTTERY", "ARCADE", "ESport", "CARD", "OTHERS", "CRASH"]

        update_requests = []
        for key, mapping in key_map.items():
            row_index = mapping["row"]
            value = json_row.get(key, None)
        
            # If value exists in json_row, use it; otherwise, default to 0
            if value is not None:
                # Check if row_index is 4 (special handling for percentages)
                if row_index == 4:  # Row 4 (index 3) needs special handling for percentages
                    try:
                        # Convert the value to percentage format (e.g., 0.482 → 48.20%)
                        percentage_value = float(value) * 100
                        formatted_value = f"{percentage_value:.2f}%"
                        update_requests.append({
                            "range": f"{sheet_range}!{chr(65 + column_index)}{row_index + 1}",
                            "values": [[formatted_value]]
                        })
                    except (ValueError, TypeError):
                        # If conversion fails, use the original value
                        update_requests.append({
                            "range": f"{sheet_range}!{chr(65 + column_index)}{row_index + 1}",
                            "values": [[value]]
                        })
                else:
                    # For all other rows, use the value as-is
                    update_requests.append({
                        "range": f"{sheet_range}!{chr(65 + column_index)}{row_index + 1}",
                        "values": [[value]]
                    })
            else:
                # If the value is missing in json_row, set it to 0
                if key not in keys_to_skip:
                # If the value is missing in json_row and not in the skip list, set it to 0
                    update_requests.append({
                        "range": f"{sheet_range}!{chr(65 + column_index)}{row_index + 1}",
                        "values": [[0]]
                })

                
        for product in json_row.get("PRD Products", []):
            product_type = product.get("PRD Product Type")
            mapping = key_map.get(product_type)
            if not mapping:
                print(f"Skipping unmapped Product Type: {product_type}")
                continue

            base_row = mapping["row"]
            for field, offset in prd_fields:
                value = product.get(field)
                if value is None or value == "":
                    value = 0
                if field == "PRD Margin":
                    try:
                        value = float(value) * 100  # Convert 0.482 → 48.2
                        value = f"{value:.2f}%"    # Format as "48.20%"
                    except (ValueError, TypeError):
                        value = "0.00%"

                update_requests.append({
                    "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + offset + 1}",
                    "values": [[value]]
                })
        # Now, handle the missing product types (those in key_map but not in json_row)
        for product_type, mapping in key_map.items():
            # Check if the product type is "CARD" or "OTHER"
            if product_type in ["Sport", "SLOT", "CASINO", "TABLE", "P2P", "FH", "LOTTERY", "ARCADE", "ESport", "CARD", "OTHERS", "CRASH"]:
                # Check if the product type exists in the json_row["PRD Products"]
                found = any(p.get("PRD Product Type") == product_type for p in json_row.get("PRD Products", []))

                if not found:
                    # If the product type is missing, set the value to 0 for all fields in the specified row
                    base_row = mapping["row"]
                    for field, offset in prd_fields:
                        update_requests.append({
                            "range": f"{sheet_range}!{chr(65 + column_index)}{base_row + offset + 1}",
                            "values": [[0]]  # Set default value to 0 for missing product types
                        })
                else:
                    # If the product type is found, use its data (already handled in the first loop)
                    continue
        return update_requests
        
    def insert_column_header(self, spreadsheet_id, sheet_id, column_index, sheet_range, formatted_date):
        
        insert_column_request = {
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": column_index,
                    "endIndex": column_index + 1
                },
                "inheritFromBefore": False
            }
        }
        copy_format_request = {
            "copyPaste": {
                "source": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,  # Adjust as needed
                    "startColumnIndex": column_index + 1,
                    "endColumnIndex": column_index + 2
                },
                "destination": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1000,
                    "startColumnIndex": column_index,
                    "endColumnIndex": column_index + 1
                },
                "pasteType": "PASTE_FORMAT",
                "pasteOrientation": "NORMAL"
            }
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    insert_column_request,
                    copy_format_request
                ]
            }
        ).execute()

            # Update header
        self.service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_range}!{chr(65 + column_index)}1",
            valueInputOption="USER_ENTERED",
            body={"values": [[formatted_date]]}
        ).execute()

    def get_sheet_id(self, spreadsheet_id, sheet_name, retries=3, delay=5):
        for attempt in range(retries):
            try:
                response = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                for sheet in response["sheets"]:
                    if sheet["properties"]["title"] == sheet_name:
                        return sheet["properties"]["sheetId"]
                raise ValueError(f"Sheet name {sheet_name} not found")
            except HttpError as e:
                if e.resp.status in [500, 502, 503, 504]:
                    logging.warning(f"Temporary error (HTTP {e.resp.status}) on attempt {attempt+1}: {e}")
                    time.sleep(delay)
                    continue
                else:
                    raise
        raise RuntimeError(f"Failed to fetch sheet ID after {retries} retries due to: {e}")

    def get_json_data(self):
        root_dir = os.path.dirname(os.path.dirname(__file__))  # one level up from scraper/
        json_path = os.path.join(root_dir, 'json', 'result.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def transfer(self, job_id):
        try:
            # data = self.get_json_data()
            data = self.json_data
            sorted_dates = sorted(data.keys(), reverse=True)

            spreadsheet_url = self.test_links[self.brand][self.currency]
            parser = GoogleSheetURLParser(spreadsheet_url)
            spreadsheet_id = parser.get_spreadsheet_id()

            # Logical Statement for Spreadsheet SheetName(Timegrain) and Keymapping, Status(For Least Rank or Rank 1-10)
            if self.timeGrain == "Day":
                status = self.timeGrain
                sheet_range = "Daily"
                key_map = key_daily_map
            elif self.timeGrain == "Week":
                status = self.timeGrain
                sheet_range = "Weekly"
                key_map = key_weekly_map
            elif self.timeGrain == "Month":
                sheet_range = "Monthly"
                if self.brand in ["BAJI", "CTN"]:
                    status = self.timeGrain
                    key_map = key_monthly_map
                elif self.brand in ["JB"]:
                    status = self.timeGrain
                    key_map = jb_monthly_map
                else:
                    status = "Week"
                    key_map = key_weekly_map
            else:
                log(job_id, f"Unsupported time grain: {self.timeGrain}")
                return {"status": "error", "message": f"Unsupported time grain: {self.timeGrain}"}

            sheet_id = self.get_sheet_id(spreadsheet_id, sheet_range)
            full_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid={sheet_id}#gid={sheet_id}"
            log(job_id, f"Selected Timegrain: {self.timeGrain} & sheet_range: {sheet_range}, {key_map}")

            # Step 1: Insert Each Date Column
            start_col = 4  # Starting from column E (index 4)
            for i, date_key in enumerate(sorted_dates):
                json_row = data[date_key]
                if not json_row:
                    print(f"Skipping empty data for {date_key}")
                    continue

                raw_date = json_row.get("Date")
                if not raw_date:
                    print(f"Skipping Missing Date {date_key}")
                    continue
                raw_date = raw_date.lstrip("'")
                date_object = datetime.strptime(raw_date.split(" ")[0], "%Y-%m-%d")
                formatted_date = date_object.strftime("%d/%m/%Y")
                log(job_id, formatted_date)
                column_index = start_col + i

                # Insert new column
                self.insert_column_header(spreadsheet_id, sheet_id, column_index, sheet_range, formatted_date)

                # Stored Request Data
                update_requests = []

                # 1. Add standard key_map & PRD product updates
                update_requests.extend(self.key_map_update(key_map, json_row, sheet_range, column_index))
                
                if status == "Month":
                    # Monthly PMT handling
                    update_requests.extend(self.monthly_key_map(key_map, json_row, sheet_range, column_index))

                else:
                # Non-monthly PMT handling with least claimed rank logic
                    update_requests.extend(self.non_monthly_key_map(key_map, json_row, sheet_range, column_index))
                # Batch update all cells
                if update_requests:
                    self.service.spreadsheets().values().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={
                            "valueInputOption": "USER_ENTERED",
                            "data": update_requests
                        }
                    ).execute()

                log(job_id, f"✅ Column {column_index} updated with header and {len(update_requests)} data points")

            return {
                "status": 200,
                "text": "Data Successfully Fetch"
                
            }
        except Exception as e:
            return {
                "status": "Failed"
            }