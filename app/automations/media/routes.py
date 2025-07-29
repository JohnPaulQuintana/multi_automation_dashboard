from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from .logic import run
from app.helpers.template import templates

router = APIRouter()

@router.get("/", response_class=HTMLResponse)
def automation1_page(request: Request):
    return templates.TemplateResponse("pages/media.html", {"request": request})

@router.post("/start")
def start_automation():
    run()
    return JSONResponse({"message": "Social Media Automation started"})
