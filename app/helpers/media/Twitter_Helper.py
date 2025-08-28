from app.automations.log.state import log
from collections import defaultdict

import re

class TwitterHelper:
    def __init__(self, post_data_list: list):
        self.posts = [self._parse_post(data) for data in post_data_list]

    def process_twitter_insights_by_page_id(self, job_id, code, followers,all_insights, pages_info, spreadsheet) -> bool:
        try:
            log(job_id, code, pages_info)
            CURRENCY = pages_info[1]
            BRAND = pages_info[2]
            FOLLOWERS = followers
            SPREADSHEET = pages_info[8]

            # Extract spreadsheet ID from the URL
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", SPREADSHEET)
            if not match:
                log(job_id, f"❌ Invalid spreadsheet URL for {BRAND}")

            spreadsheet_id = match.group(1)
            log(job_id, f"\n🔄 Processing {len(all_insights)} insights for {BRAND}")

            try:
                # spreadsheet.transfer_insight_header_only(spreadsheet_id, CURRENCY, insights)
                spreadsheet.transfer_timeline_insight_data(job_id, spreadsheet_id, CURRENCY, all_insights, FOLLOWERS)
                spreadsheet.hide_old_rows(job_id, spreadsheet_id, CURRENCY)

                log(job_id, f"✅ Insight data transfer completed for {BRAND} Followers: {FOLLOWERS}")
            except Exception as e:
                log(job_id, f"❌ Failed processing {BRAND}: {str(e)}")
            
            return True

        except Exception as e:
            log(job_id, f"❌ Unexpected error during processing: {str(e)}")
            return False