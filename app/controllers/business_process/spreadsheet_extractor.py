import re

class GoogleSheetURLParser:
    def __init__(self, url):
        self.url = url

    def get_spreadsheet_id(self):
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", self.url)
        if match:
            return match.group(1)
        raise ValueError("Invalid Google Sheets URL")
    


