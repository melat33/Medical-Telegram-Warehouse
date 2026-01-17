@echo off
echo ==================================================
echo    MEDICAL TELEGRAM WAREHOUSE - dbt PIPELINE
echo ==================================================
echo Date:       %date% %time%
echo Project:    medical_warehouse
echo ==================================================
echo.

REM Check if dbt is installed
where dbt >nul 2>nul
if %errorlevel% neq 0 (
    echo ERROR: dbt is not installed
    echo Install with: pip install dbt-postgres
    exit /b 1
)

REM Navigate to dbt project
cd medical_warehouse
if %errorlevel% neq 0 (
    echo ERROR: medical_warehouse directory not found
    exit /b 1
)

REM Test database connection
echo Testing database connection...
dbt debug | find "All checks passed" >nul
if %errorlevel% neq 0 (
    echo ERROR: Database connection failed
    dbt debug
    exit /b 1
)
echo ✓ Database connection successful
echo.

REM Run dbt pipeline
echo Cleaning previous artifacts...
dbt clean
echo.

echo Installing dependencies...
dbt deps
echo.

echo Running dbt models...
dbt run
if %errorlevel% neq 0 (
    echo ERROR: Model creation failed
    exit /b 1
)
echo ✓ Models created successfully
echo.

echo Running data quality tests...
dbt test
if %errorlevel% neq 0 (
    echo ⚠ Some tests failed (check test results)
) else (
    echo ✓ All tests passed
)
echo.

echo Generating documentation...
dbt docs generate
echo ✓ Documentation generated
echo.

echo ==================================================
echo                 PIPELINE COMPLETE
echo ==================================================
echo Next Steps:
echo   1. View data: SELECT * FROM analytics.dim_channels LIMIT 5;
echo   2. Access docs: dbt docs serve
echo ==================================================