@echo off

REM === Start FastAPI backend ===
echo Starting FastAPI backend...
start "" /min cmd /c "call env\Scripts\activate && uvicorn main:app"

echo Opening Centralized in browser...
start http://127.0.0.1:8000

echo Centralized should be running now.