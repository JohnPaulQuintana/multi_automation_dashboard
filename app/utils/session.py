from itsdangerous import URLSafeSerializer
from fastapi import Request

SECRET_KEY = "your-very-secret-key"  # use environment variable in real app
COOKIE_NAME = "session"

serializer = URLSafeSerializer(SECRET_KEY, salt="auth")

def create_session_cookie(data: dict) -> str:
    return serializer.dumps(data)

def read_session_cookie(cookie: str) -> dict | None:
    try:
        return serializer.loads(cookie)
    except Exception:
        return None

def get_current_user(request: Request) -> dict | None:
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return None
    return read_session_cookie(cookie)
