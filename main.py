from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from app.core import base_router
from app.automations.conversion import routes as coversion_routes
from app.automations.media import routes as media_routes
from app.automations.business_process import routes as business_routes
from app.automations.tracker import routes as nsuftd_tracker
from app.automations.badsha import routes as badsha_report

origins = [
    "http://localhost:8000",
    "http://127.0.0.1:8000"
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(base_router.router)
app.include_router(coversion_routes.router, prefix="/api/v1/conversion")
app.include_router(media_routes.router, prefix="/api/v1/media")
app.include_router(business_routes.router, prefix="/api/v1/business")
app.include_router(nsuftd_tracker.router, prefix="/api/v1/tracker")
app.include_router(badsha_report.router, prefix="/api/v1/badsha")

app.mount("/static", StaticFiles(directory="static"), name="static")
