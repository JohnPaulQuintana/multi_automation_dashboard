import math
import hashlib
import logging, hashlib, traceback
from typing import List
import time
import requests
import re, hashlib, traceback, time, requests, logging
from bs4 import BeautifulSoup

from app.helpers.conversion.conversion import _socmedlinks, _afflinks, filter_rows_affiliate,filter_rows_player,filter_rows_affilliate_socmed
from app.api.v1.conversion.get_request import BoDataAPI
from app.automations.log.state import log  # ✅ import from new file

class AcquisitionController:
    
    def __init__(
        self,
        email: str,
        password: str,
        currency: str,
        currency_type: int,
        brand: str,
        targetdate: str,
        max_retries: int = 3,
    ):
        self.email = email
        self.password = password
        self.currency = currency
        self.brand = brand
        self.targetdate = targetdate
        self._currency_type = currency_type
        self.max_retries = max_retries

        self.session = requests.Session()
        self.cookies = None  # populated after _authenticate()

    # ────────────────────────────────────────────────────────────
    # Public helper: fetch every keyword in batches of five
    # ────────────────────────────────────────────────────────────
    def fetch_bo_batched(self, job_id, type: str, keywords: List[str], targetdate:str, batch_size: int = 3, page_size: int = 100):
        """
        Fetch BO data for an arbitrary keyword list, five at a time.
        Returns: dict(status, text, data=[…], total=int)
        """
        if not keywords:
            log(job_id, f"❌ Job failed. No keywords provided.")
            return {"status": 400, "text": "No keywords provided.", "data": [], "total": 0}

        auth_ok = self._authenticate(job_id, type)
        if not auth_ok:
            log(job_id, f"❌ Job failed. Authentication failed.")
            return {"status": 401, "text": "Authentication failed."}
        
        link_dict = _socmedlinks if type == "SocialMedia" else _afflinks
        urls = link_dict.get(self.brand)

        if not urls or len(urls) < 2:
            log(job_id, f"❌ Job failed. No login URLs found for brand: {self.brand}.")
            raise ValueError(f"No login URLs found for brand: {self.brand}")

        print("Initialized API for fetching data...")
        log(job_id, f"🚀 Initialized API for fetching data...")
        api = BoDataAPI(job_id, session=self.session,cookies=self.cookies,currency_type=self._currency_type, page_size=page_size)
        print("-------------------------------------------------------------------------------------")
        print("Initialized API for fetching data completed...")
        print("-------------------------------------------------------------------------------------")
        log(job_id, f"✅ Initialized API for fetching data completed...")
        # ---- end of batches ----------------------------------------------- 
        # 🔄  Delegate filtering/renaming to the helper
        print("Identifying processing type:", type)
        log(job_id, f"⏳ Identifying processing type: {type}")
        if type == "SocialMedia":
            log(job_id,f"⏳ Collecting SocialMedia data for player...")
            print("Collecting SocialMedia data for player...")
            all_rows_socmed_player = api.fetch(endpoint=urls[2],data_type="SocialMedia", keywords=keywords,target_date=targetdate, batch_size=batch_size)
            # print(all_rows_socmed_player)
            filtered_rows = filter_rows_player(all_rows_socmed_player)
            print("-------------------------------------------------------------------------------------")
            print("Collecting SocialMedia data for player completed...")
            log(job_id,f"✅ Collecting SocialMedia data for player completed...")
            print("-------------------------------------------------------------------------------------")
            print("Collecting SocialMedia data for affiliates...")
            log(job_id,f"⏳ Collecting SocialMedia data for affiliates...")
            all_rows_socmend_aff = api.fetch(endpoint=urls[3],data_type="Affiliates", keywords=keywords,target_date=targetdate, batch_size=batch_size)
            # print(all_rows_socmend_aff)
            filtered_rows_socmed = filter_rows_affilliate_socmed(all_rows_socmend_aff)
            print("-------------------------------------------------------------------------------------")
            print("Collecting SocialMedia data for affiliates completed...")
            log(job_id,f"✅ Collecting SocialMedia data for affiliates completed...")
            print("-------------------------------------------------------------------------------------")
        else:
            print("Collecting Affiliate data...")
            log(job_id,f"⏳ Collecting Affiliate data...")
            all_rows_aff = api.fetch(endpoint=urls[2],data_type="Affiliates", keywords=keywords,target_date=targetdate, batch_size=batch_size)
            # print(all_rows_aff)
            filtered_rows = filter_rows_affiliate(all_rows_aff)    
            print("-------------------------------------------------------------------------------------")
            print("Collecting Affiliate data completed...")
            log(job_id,f"✅ Collecting Affiliate data completed...")
            print("-------------------------------------------------------------------------------------")
        print("Fetching Completed.......:", type)
        log(job_id,f"✅ Fetching Completed.......: {type}")
        print("-------------------------------------------------------------------------------------")
        return {
            "status": 200,
            "text": "Data fetched and filtered successfully.",
            "data": filtered_rows,
            "data_socmed": filtered_rows_socmed if type == "SocialMedia" else [],
            "total": len(filtered_rows),
        }

    # ────────────────────────────────────────────────────────────
    # Internal: sign‑in once per controller instance
    # ────────────────────────────────────────────────────────────
    def _authenticate(self,job_id, type) -> bool:
        """
        Login + capture cookies.
        Returns True on success, False on ANY failure,
        while printing the full error/traceback.
        """
        print("Authenticating user")          # keep your console cue
        try:
            # -------- 0) Choose brand‑specific URLs --------
            link_dict = _socmedlinks if type == "SocialMedia" else _afflinks
            urls = link_dict.get(self.brand)

            if not urls or len(urls) < 2:
                log(job_id, f"❌ Job failed. No login URLs found for brand: {self.brand}")
                raise ValueError(f"No login URLs found for brand: {self.brand}")

            print(f"Using URLs: {urls[0]} and {urls[1]}")
            log(job_id, f"⏳ Processing...using urls: {urls[0]} and {urls[1]}")
            # -------- 1) GET login page --------
            self.session.cookies.clear()  # <- force fresh login
            resp = self.session.get(urls[0], timeout=10)
            resp.raise_for_status()            # throws for 4xx / 5xx
            # time.sleep(3)  # avoid hammering the server
            # -------- 2) Scrape randomCode --------
            soup = BeautifulSoup(resp.text, "html.parser")
            random_tag = soup.find("input", {"id": "randomCode"})
            if random_tag is None:
                log(job_id, f"❌ Job failed...randomCode input not found on login page.")
                raise RuntimeError("randomCode input not found on login page.")
            random_code_val = random_tag["value"]

            # -------- 3) POST credentials --------
            auth_payload = {
                "username": self.email,
                "password": hashlib.sha1(self.password.encode()).hexdigest(),
                "randomCode": random_code_val,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "*/*",
            }
            login = self.session.post(urls[1], data=auth_payload,
                                    headers=headers, timeout=10)
            login.raise_for_status()

            # -------- 4) Success --------
            self.cookies = self.session.cookies.get_dict()
            print("Authentication successful.")
            log(job_id, f"✅ Job complete... authentication successful.")
            return True

        except (requests.RequestException, Exception) as e:
            # Print full traceback so you immediately see *where* it failed
            log(job_id, f"❌ Job failed...Authentication error:\n%s {traceback.format_exc()}")
            print("Authentication error:\n%s", traceback.format_exc())
            return False
