
from app.automations.log.state import log
from collections import defaultdict
import re

class FacebookHelper:
    def __init__(self, post_data_list: list):
        self.posts = [self._parse_post(data) for data in post_data_list]

    # Parses a single post data dictionary into a structured format.
    def _parse_post(self, data: dict):
        return {
            "source_page_id": data.get("source_page_id"),
            "source_page_token": data.get("source_page_token"),
            "post_id": data.get("post_id"),
            "created_time": data.get("created_time"),
            "message": data.get("message"),
            "post_link": data.get("post_link"),
            "insights": self._parse_insights(data.get("insights", {}))
        }
    
    # Parses insights data from a post into a structured format.
    def _parse_insights(self, insights: dict):
        return {
            "reach": insights.get("reach", 0),
            "impressions": insights.get("impressions", 0),
            "reactions": insights.get("reactions", 0)
        }

    # return sorted posts by created time.
    def get_sorted_posts(self, reverse: bool = False):
        return sorted(self.posts, key=lambda x: x["created_time"], reverse=reverse)
    
    def get_currency(self, currency, brand):
        curr = None
        if currency == "PKR" and brand=='BAJI':
            curr = "bajilive.casino"
        elif currency == "NPR" and brand=='BAJI':
            curr = "baji.sports"
        elif currency == "BDT" and brand=='JEETBUZZ':
            curr="jeetbuzzcasino"
        elif currency=="INR" and brand=="JEETBUZZ":
            curr="jeetbuzzsports"
        elif currency=="PKR" and brand=="SIX6S":
            curr="six6s.sport"
        elif currency=="INR" and brand=="SIX6S":
            curr="six6s.casino"
        return curr

    def process_facebook_insights_by_page_id(self, job_id, all_insights, pages_info, spreadsheet) -> bool:
        try:
            # Step 1: Group insights by source_page_id
            insights_by_page_id = defaultdict(list)
            for insight in all_insights:
                page_id = insight.get('source_page_id')
                if page_id:
                    insights_by_page_id[page_id].append(insight)

            # Step 2: Create lookup map from pages_info
            pages_info_map = {page['page_id']: page for page in pages_info if 'page_id' in page}

            # Step 3: Process each IG ID group
            for page_id, insights in insights_by_page_id.items():
                matched_info = pages_info_map.get(page_id)
                if not matched_info:
                    print(f"⚠️ PAGE ID {page_id} not found in pages list")
                    continue

                CURRENCY = matched_info["currency"]
                BRAND = matched_info["brand"]
                FOLLOWERS = matched_info["followers"]
                SPREADSHEET = matched_info["spreadsheet"]

                # Extract spreadsheet ID from the URL
                match = re.search(r"/d/([a-zA-Z0-9-_]+)", SPREADSHEET)
                if not match:
                    print(f"❌ Invalid spreadsheet URL for {BRAND}")
                    continue

                spreadsheet_id = match.group(1)
                log(job_id, f"\n🔄 Processing {len(insights)} insights for {BRAND} (Page {page_id})")
                try:
                    # spreadsheet.transfer_insight_header_only(spreadsheet_id, CURRENCY, insights)
                    spreadsheet.transfer_insight_data(spreadsheet_id, self.get_currency(CURRENCY, BRAND), insights, FOLLOWERS)
                    spreadsheet.hide_old_rows(spreadsheet_id, CURRENCY)

                    print(f"✅ Insight data transfer completed for {BRAND} (IG {page_id}) Followers: {FOLLOWERS}")
                except Exception as e:
                    log(job_id, f"❌ Failed processing {BRAND} (IG {page_id}): {str(e)}")

            return True

        except Exception as e:
            print(f"❌ Unexpected error during processing: {str(e)}")
            return False