from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse,JSONResponse,RedirectResponse
from app.services.google_auth import verify_token
from app.utils.session import create_session_cookie, COOKIE_NAME
from app.dependencies.auth import auth_required
from datetime import datetime

templates = Jinja2Templates(directory="app/templates")



router = APIRouter()

# return authentication page
@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("auth/auth.html", {"request": request})

#verify authentication using google auth
@router.post("/auth/verify")
async def verify_google_token(request: Request):
    data = await request.json()
    token = data.get("token")
    user_info = await verify_token(token)

    if user_info:
        cookie_value = create_session_cookie(user_info)
        response = JSONResponse({"status": "success", "user": user_info})
        response.set_cookie(
            key=COOKIE_NAME,
            value=cookie_value,
            httponly=True,
            secure=False,  # True in production (HTTPS only)
            samesite="Lax",
            max_age=60 * 60 * 24  # 1 day
        )
        return response

    return JSONResponse({"status": "error"}, status_code=401)

@router.get("/auth/logout")
def logout(request: Request):
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie(COOKIE_NAME)
    return response

# return the main dashboard
@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, user=Depends(auth_required)):
    
    return templates.TemplateResponse(
        "pages/dashboard/index.html",
        {
            "request": request,
            "user": user,
            "version": int(datetime.utcnow().timestamp())
        }
    )

@router.get("/.well-known/appspecific/com.chrome.devtools.js")
def ignore_devtools_request():
    return JSONResponse(content=None, status_code=204)
