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


class YoutubeController:
    """Controller for YOUTUBE API interactions."""
    def __init__(self, YOUTUBE_BASE_API_URL:str):
        self.base_url = YOUTUBE_BASE_API_URL
        print("Youtube Controller initialized...")