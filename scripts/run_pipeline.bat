@echo off
REM All-in-one pipeline runner (Windows batch version)

setlocal enabledelayedexpansion

set CONFIG=%~1
if "!CONFIG!"=="" set CONFIG=examples\build_local_jsonl.yaml

set MONITOR=%~2

echo ==========================================
echo Clean Corpus Platform - Pipeline Runner
echo ==========================================
echo Config: %CONFIG%
echo Started: %date% %time%
echo ==========================================

REM Bootstrap PII detectors
echo.
echo Bootstrapping PII detectors...
python scripts\bootstrap_pii.py
if errorlevel 1 (
    echo ERROR: Failed to bootstrap PII detectors
    exit /b 1
)

REM Verify config
echo.
echo Verifying configuration...
python scripts\show_sources.py "%CONFIG%"

REM Run pipeline
echo.
echo Running pipeline...
clean-corpus build --config "%CONFIG%"
if errorlevel 1 (
    echo ERROR: Pipeline failed
    exit /b 1
)

REM Show results
echo.
echo ==========================================
echo Results
echo ==========================================
python scripts\show_run_info.py storage_example 2>nul || echo Run completed

REM Launch monitor if requested
if "%MONITOR%"=="--monitor" goto monitor
if "%MONITOR%"=="-m" goto monitor
goto end

:monitor
echo.
echo Launching monitoring dashboard...
clean-corpus monitor storage_example
goto end

:end
echo.
echo ==========================================
echo Complete: %date% %time%
echo ==========================================

endlocal
