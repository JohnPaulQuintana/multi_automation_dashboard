import hashlib
import logging
import traceback
import requests
import json
from bs4 import BeautifulSoup  # pip install beautifulsoup4

def capture_login_payload(username, password):
    url_login_page = "https://bajipartners.xyz/page/affiliate/login.jsp"
    url_login_api = "https://bajipartners.xyz/affiliate/main/login"

    session = requests.Session()

    # Step 1: Load login page
    try:
        resp = session.get(url_login_page, timeout=10)
        print("🔍 Preview login page HTML:\n", resp.text[:500], "\n")
    except Exception as e:
        logging.error("Failed to open login page: %s", e)
        traceback.print_exc()
        return {"error": "cannot load login page"}

    # Step 2: Look for hidden inputs (e.g. csrf tokens)
    soup = BeautifulSoup(resp.text, "html.parser")
    hidden_inputs = {tag["name"]: tag["value"] for tag in soup.find_all("input", {"type": "hidden"}) if tag.has_attr("name")}
    print("🔑 Hidden inputs found:", hidden_inputs)

    # Step 3: Construct login payload
    auth_payload = {
        "userId": username,
        "password": hashlib.sha1(password.encode()).hexdigest(),
        "fingerprint": "4074067417",
        "fingerprint2": "21d6e0d0e3bf74c62af100c5d940800d",
        "fingerprintCanvas": "809282357",
        "fingerprintActiveX": "4074067417",
        "fingerprintResolution": "423511283",
    }
    # Add hidden tokens if any
    auth_payload.update(hidden_inputs)

    headers = {
        "Accept": "*/*",
        "Referer": url_login_page,
        "Origin": "https://bajipartners.xyz",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/139.0.0.0 Safari/537.36",
    }

    # Step 4: Try FORM payload
    headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    try:
        resp_form = session.post(url_login_api, data=auth_payload, headers=headers, timeout=10)
        print("📩 Form login status:", resp_form.status_code)
    except Exception as e:
        print("❌ Form login failed:", e)
        resp_form = None

    # Step 5: Try JSON payload
    headers["Content-Type"] = "application/json"
    try:
        resp_json = session.post(url_login_api, json=auth_payload, headers=headers, timeout=10)
        print("📩 JSON login status:", resp_json.status_code)
    except Exception as e:
        print("❌ JSON login failed:", e)
        resp_json = None

    # Step 6: Return results
    return {
        "url": url_login_api,
        "request_payload": auth_payload,
        "form_attempt": resp_form.text[:500] if resp_form else "failed",
        "json_attempt": resp_json.text[:500] if resp_json else "failed",
        "cookies": session.cookies.get_dict()
    }

if __name__ == "__main__":
    data = capture_login_payload("bjsportradar", "qaz123")
    print("\nFinal captured login payload:\n", json.dumps(data, indent=2))
