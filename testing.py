from playwright.sync_api import sync_playwright
import json

def capture_login_payload(username, password):
    url_login_page = "https://bajipartners.xyz/page/affiliate/login.jsp"
    url_login_api = "/affiliate/main/login"  # relative path match

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # set True if you want headless
        context = browser.new_context()
        page = context.new_page()

        payload_holder = {}

        # Intercept requests
        def on_request(request):
            if url_login_api in request.url and request.method == "POST":
                try:
                    post_data = request.post_data
                    payload_holder["url"] = request.url
                    payload_holder["headers"] = request.headers
                    payload_holder["body"] = json.loads(post_data)
                    print("Captured payload:", json.dumps(payload_holder["body"], indent=2))
                except Exception as e:
                    print("Error parsing request:", e)

        context.on("request", on_request)

        # Go to login page
        page.goto(url_login_page)

        # Fill login form (adjust selectors as needed)
        page.fill('input[name="userId"]', username)
        page.fill('input[name="password"]', password)
        page.click('button#login')

        # Wait for navigation or response
        page.wait_for_timeout(5000)

        browser.close()
        return payload_holder

if __name__ == "__main__":
    data = capture_login_payload("bjsportradar", "qaz123")
    print("\nFinal captured login payload:\n", json.dumps(data, indent=2))
