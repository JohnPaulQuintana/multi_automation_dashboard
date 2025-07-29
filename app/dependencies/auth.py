from fastapi import Request, HTTPException
from app.utils.session import get_current_user
from starlette.responses import RedirectResponse

def auth_required(request: Request):
    user = get_current_user(request)
    if not user:
        # Raise exception with RedirectResponse
        response = RedirectResponse(url="/")
        raise HTTPException(status_code=302, headers={"Location": "/"})
    
    return user
