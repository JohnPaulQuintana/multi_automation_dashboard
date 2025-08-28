import re
from collections import defaultdict
from app.automations.log.state import log

class YoutubeHelper:
    def __init__(self, post_data_list: list):
        self.posts = [self._parse_post(data) for data in post_data_list]

    # Parses a single post data dictionary into a structured format.
    def _parse_post(self, data: dict):
        return {
            "video_id": data.get("video_id"),
            "published_at": data.get("published_at"),
            "title": data.get("title"),
            "url": data.get("url"),
            "insights": 
                self._parse_insights(
                    data.get("views", 0),
                    data.get("engaged_views", 0),
                    data.get("likes", 0),
                    data.get("comments", 0),
                    data.get("shares", 0)
                )
        }
    
    # Parses insights data from a post into a structured format.
    def _parse_insights(self, views, engaged_views, likes, comments, shares):
        return {
            "reach": engaged_views,
            "impressions": views,
            "reactions": sum([likes, comments, shares])
        }

    def process_youtube_insights_by_page_id(self, job_id, code, all_insights, pages_info, spreadsheet) -> bool:
        try:
            log(job_id, f"{code}, {pages_info}")
            CURRENCY = pages_info[1]
            BRAND = pages_info[2]
            FOLLOWERS = all_insights["channel"]["subscribers"]
            SPREADSHEET = pages_info[7]

            # Extract spreadsheet ID from the URL
            match = re.search(r"/d/([a-zA-Z0-9-_]+)", SPREADSHEET)
            if not match:
                log(job_id, f"❌ Invalid spreadsheet URL for {BRAND}")

            spreadsheet_id = match.group(1)
            log(job_id, f"\n🔄 Processing {len(all_insights)} insights for {BRAND} (Page {all_insights["channel"]["channel_id"]})")

            try:
                # spreadsheet.transfer_insight_header_only(spreadsheet_id, CURRENCY, insights)
                spreadsheet.transfer_video_insight_data(job_id, spreadsheet_id, CURRENCY, all_insights["video_insights"], FOLLOWERS)
                spreadsheet.hide_old_rows(job_id, spreadsheet_id, CURRENCY)

                log(job_id, f"✅ Insight data transfer completed for {BRAND} (YOUTUBE {all_insights["channel"]["channel_id"]}) Followers: {FOLLOWERS}")
            except Exception as e:
                log(job_id, f"❌ Failed processing {BRAND} (YOUTUBE {all_insights["channel"]["channel_id"]}): {str(e)}")

            return True

        except Exception as e:
            log(job_id, f"❌ Unexpected error during processing: {str(e)}")
            return False