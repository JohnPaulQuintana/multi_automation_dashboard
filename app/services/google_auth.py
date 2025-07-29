import httpx

GOOGLE_CLIENT_ID = "266581007685-pd687uda2mlgdgccdbbnsedk2lu6ssb4.apps.googleusercontent.com"

# ✅ Only allow emails from this domain
ALLOWED_DOMAIN = "auroramy.com"

# ✅ (Optional) Allow only specific emails as well
# WHITELIST_EMAILS = {
#     "ceo@auroramy.com",
#     "admin@auroramy.com",
#     "john@auroramy.com"
# }

async def verify_token(token: str):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://oauth2.googleapis.com/tokeninfo?id_token={token}")

    if resp.status_code != 200:
        return None

    data = resp.json()

    # Validate the audience
    if data.get("aud") != GOOGLE_CLIENT_ID:
        return None

    email = data.get("email")
    domain = email.split("@")[-1] if email else ""

    # ✅ Check domain
    if domain != ALLOWED_DOMAIN:
        return None

    # ✅ (Optional) Check specific whitelist
    # if WHITELIST_EMAILS and email not in WHITELIST_EMAILS:
    #     return None

    return {
        "email": email,
        "name": data.get("name"),
        "picture": data.get("picture")
    }
