import logging
import time
import traceback
from typing import List, Dict, Any

import requests
from app.automations.log.state import log  # ✅ import from new file

class BoDataAPI:
    """
    Small helper that fetches *Affiliates* or *SocialMedia* rows from
    the BO endpoint in (keyword‑batch × page) blocks and returns a flat
    list. Re‑uses the caller's requests.Session so you keep cookies.
    """

    def __init__(
        self,
        job_id,
        session,
        cookies: Dict[str, str],
        currency_type: int = -1,
        page_size: int = 100,   # BO's hard limit
        max_pages: int = 100     # New safety limit
    ):
        self.session = session
        self.cookies = cookies
        self.currency_type = currency_type
        self.page_size = page_size
        self.max_pages = max_pages
        self.job_id = job_id
    def fetch(
        self,
        *,
        endpoint: str,
        data_type: str,
        keywords: List[str],
        target_date: str,
        batch_size: int = 5,
        max_retries: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Returns a flat list of dict rows for all keywords & pages.
        """
        log(self.job_id, f"🚀 [START] Fetching {data_type} data | Keywords: {len(keywords)-2} | Date: {target_date}")
        print(f"\n[START] Fetching {data_type} data | Keywords: {len(keywords)-2} | Date: {target_date}")

        all_rows = []
        user_ids = keywords[2:]  # skip brand & sheetId

        for start in range(0, len(user_ids), batch_size):
            batch = user_ids[start:start + batch_size]
            log(self.job_id, f"⏳Processing batch {start+1}-{min(start+batch_size, len(user_ids))}: {batch}")
            print(f"\nProcessing batch {start+1}-{min(start+batch_size, len(user_ids))}: {batch}")

            page = 1
            last_row_count = -1
            duplicate_count = 0
            
            while page <= self.max_pages:
                for attempt in range(max_retries):
                    try:
                        print(f"  Page {page} (Attempt {attempt+1})...", end=" ", flush=True)
                        log(self.job_id, f"🚀 Page {page} (Attempt {attempt+1})...")
                        rows = self._fetch_page(
                            endpoint=endpoint,
                            batch=batch,
                            data_type=data_type,
                            target_date=target_date,
                            page=page,
                        )
                        
                        if not rows:
                            log(self.job_id, f"✅ (empty rows)...")
                            print("✓ (empty)")
                            break
                            
                        # Check for duplicate data (infinite loop protection)
                        if len(rows) == last_row_count:
                            duplicate_count += 1
                            if duplicate_count >= 3:
                                log(self.job_id, f"⚠️ Duplicate data detected 3 times, stopping...")
                                print("⚠️  Duplicate data detected 3 times, stopping")
                                break
                        else:
                            duplicate_count = 0
                            last_row_count = len(rows)
                        
                        all_rows.extend(rows)
                        log(self.job_id, f"✅ Added {len(rows)} rows (Total: {len(all_rows)})")
                        print(f"✓ Added {len(rows)} rows (Total: {len(all_rows)})")
                        
                        # Termination conditions
                        if len(rows) < self.page_size:
                            log(self.job_id, f"→ End of data (received {len(rows)} < {self.page_size} rows)")
                            print(f"  → End of data (received {len(rows)} < {self.page_size} rows)")
                            break
                            
                        page += 1
                        break  # Success, exit retry loop

                    except requests.exceptions.Timeout:
                        print(f"× Timeout")
                        log(self.job_id, f"❌ Job failed... Timeout")
                        if attempt == max_retries - 1:
                            log(self.job_id, f"  → Max retries reached, skipping")
                            print("→ Max retries reached, skipping")
                            break
                        time.sleep(2 ** attempt)

                    except Exception as e:
                        log(self.job_id, f"❌ Job failed... Error: {str(e)}")
                        print(f"× Error: {str(e)}")
                        break

                else:  # No break occurred, all retries failed
                    log(self.job_id, f"→ All attempts failed, moving to next batch")
                    print("  → All attempts failed, moving to next batch")
                    break

                # Check termination conditions again
                if not rows or len(rows) < self.page_size or duplicate_count >= 3:
                    break
            
            log(self.job_id, f"✅ Batch complete. Total rows: {len(all_rows)}")
            print(f"  Batch complete. Total rows: {len(all_rows)}")
            if page > self.max_pages:
                log(self.job_id, f"⚠️ Warning: Hit max page limit!")
                print("  ⚠️ Warning: Hit max page limit!")

        log(self.job_id, f"✅ [COMPLETE] Fetched {len(all_rows)} total rows for {data_type}.")
        print(f"\n[COMPLETE] Fetched {len(all_rows)} total rows for {data_type}")
        return all_rows

    def _fetch_page(
        self,
        *,
        endpoint: str,
        batch: List[str],
        data_type: str,
        target_date: str,
        page: int,
    ) -> List[Dict[str, Any]]:
        params = {
            "resultBy": "" if data_type == "SocialMedia" else 1,
            "visibleColumns": "",
            "currencyType": self.currency_type,
            "searchStatus": -99,
            "userId": ",".join(batch),
            "affiliateInternalType": -1,
            "searchTimeStart": target_date,
            "searchTimeEnd": target_date,
            "pageNumber": page,
            "pageSize": self.page_size,
            "sortCondition": 14,
            "sortName": "turnover",
            "sortOrder": 1,
            "searchText": "",
        }

        try:
            resp = self.session.get(
                endpoint, 
                params=params, 
                cookies=self.cookies,
                timeout=30
            )
            resp.raise_for_status()
            
            # # 🧪 Debug log BEFORE attempting to parse as JSON
            # if "application/json" not in resp.headers.get("Content-Type", ""):
            #     logging.error(f"Unexpected Content-Type: {resp.headers.get('Content-Type')}")
            #     logging.error(f"Raw Response:\n{resp.text[:300]}")
            #     return []
            # if not resp.text.strip():
            #     logging.error("Empty response body")
            #     return []

            payload = resp.json()
            # print(payload)
            
            if not isinstance(payload.get("aaData", []), list):
                logging.error("Invalid response format: aaData is not a list")
                return []

            rows = payload["aaData"]
            return rows  
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {str(e)}")
            return []
        except ValueError as e:
            logging.error(f"JSON decode error: {str(e)}")
            return []