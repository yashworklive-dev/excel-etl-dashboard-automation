@echo off
title Excel ETL Dashboard Automation
echo ============================================================
echo   Coffee Shop Sales - Automated Excel Dashboard (Python)
echo ============================================================
echo.

:: Step 1: Activate virtual environment if it exists
if exist "%~dp0venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call "%~dp0venv\Scripts\activate.bat"
) else (
    echo No virtual environment found. Proceeding with system Python.
)

:: Step 2: Ensure required dependencies are installed
echo.
echo Checking and installing dependencies if missing...
python -m pip install --upgrade pip >nul 2>&1
pip install -r "%~dp0requirements.txt" >nul 2>&1

:: Step 3: Run ETL script
echo.
echo Running ETL automation...
python "%~dp0run_etl.py"

:: Step 4: Completion message
echo.
echo ------------------------------------------------------------
echo  ✅ Process completed successfully!
echo  📁 Output files are available inside the "output" folder.
echo ------------------------------------------------------------
echo.
pause
