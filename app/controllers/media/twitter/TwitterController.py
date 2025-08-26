from time import sleep
from datetime import datetime, timedelta, timezone
from datetime import date as dt
from collections import defaultdict
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from app.automations.log.state import log

import requests
import json
import re
import time
import pickle
import os

class TwitterController:
    """Controller for Twitter API interactions."""
    def __init__(self, TWITTER_BASE_API_URL:str, key:str):
        self.base_url = TWITTER_BASE_API_URL
        print("Twitter initialized...")
        self.headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
        }