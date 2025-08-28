from app.automations.log.state import log

class ClientHelper:
    def _process_data(self, job_id, BRAND_CURRENCY: str, SPREADSHEET_ID: str, TARGET:str, client_sheet, DATA:list):
        """
        Executes the client helper logic.
        :param sheet: The Google Sheets API service object.
        :param data: The data to be processed.
        :return: True if successful, False otherwise.
        """
        try:
            log(job_id, f"{BRAND_CURRENCY}, {SPREADSHEET_ID}, {TARGET}")
            # Process the data and update the spreadsheet
            # 1. Get your target rows (from your existing function)
            target_rows = client_sheet.batch_find_targets(
                job_id,
                spreadsheet_id=SPREADSHEET_ID,
                tab_configs={
                    f"{BRAND_CURRENCY}": {
                        "targets": [TARGET],
                        "start_row": 8,
                        "column": "B"
                    }
                }
            )[BRAND_CURRENCY]
            log(job_id, f"Target rows: {target_rows}")

            # 2. Prepare your values (example data)
            # platform_values = {
            #     "FACEBOOK PAGE": ["1500", "1800", "2000"],
            #     "INSTAGRAM CASINO": ["800", "1000", "1200"]
            # }
            
            # 3. Convert to object format
            platform_cells = client_sheet.convert_to_object_format(target_rows, {
                f"{TARGET}": DATA
            })
            # Result: {'FACEBOOK PAGE': [{'row':9,'value':'1500'}, ...]}

            # 4. Execute update
            results = client_sheet.update_platform_cells(
                job_id,
                spreadsheet_id=SPREADSHEET_ID,
                tab_name=BRAND_CURRENCY,
                platform_cells=platform_cells
            )

            # 5. Print results
            log(job_id, "\nUpdate Results:")
            for platform, success in results.items():
                log(job_id, f"{platform:20} {'✓' if success else '✗'}")

            # return True
        except Exception as e:
            log(job_id, f"Error in ClientHelper execution: {e}")
            return False