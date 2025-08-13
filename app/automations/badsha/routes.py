from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from app.helpers.template import templates
from .logic import run  # make sure run(job_id) is updated to call `log(job_id, ...)`
from ..log.state import job_logs
import uuid


router = APIRouter()

# @router.get("/", response_class=HTMLResponse)
# def conversion_page(request: Request):
#     return templates.TemplateResponse("pages/conversion.html", {"request": request})

@router.post("/start")
def start_automation(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    job_logs[job_id] = ["🟡 Job accepted. Waiting to start..."]  # init
    background_tasks.add_task(run, job_id)
    return JSONResponse({"message": "Automation started", "job_id": job_id})

@router.get("/logs/{job_id}")
def get_job_logs(job_id: str):
    logs = job_logs.get(job_id)
    if logs is None:
        return JSONResponse({"logs": ["❌ Job ID not found"]}, status_code=404)
    if not logs:
        return JSONResponse({"logs": ["🟡 Job is initializing..."]})
    return JSONResponse({"logs": logs})


