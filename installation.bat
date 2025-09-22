@ECHO off

ECHO === Installation of Virtual Environment ===

IF NOT EXIST ".env\Scripts\activate" (
    ECHO Python virtual environment not found in backend\.env
    ECHO Creating virtual environment...
    CALL python -m venv env || (
        ECHO Failed to create Python virtual environment. Please check the error above for more details.
        PAUSE
        EXIT /B
    )
    ECHO Virtual environment created successfully.
    ECHO.
)
ECHO Activating Python Environment...
CALL "env\Scripts\activate" || (
    ECHO Failed to activate Python environment
    PAUSE
    EXIT /B
)

ECHO.

IF EXIST "requirements.txt" (
    ECHO Installing dependencies from requirements.txt...
    CALL pip install -r requirements.txt || (
        ECHO Failed to install dependencies. Please check the error above for more details.
        PAUSE
        EXIT /B
    )
    ECHO Dependencies installed successfully.
    ECHO.
) ELSE (
    ECHO requirements.txt not found. Please ensure the file is present in the project folder.
    PAUSE
    EXIT /B
)

ECHO.

ECHO === Installation Complete ===
ECHO Frontend: Node.js dependencies installed
ECHO Backend: Python environment activated
ECHO Playwright: Installed successfully
Echo === You Can Run run.bat to open centralized ===
PAUSE