@echo off
REM Windows batch script to set up data directory for Bibliotheca
REM This ensures parity with Docker environment setup on Windows

echo Setting up data directory for standalone execution on Windows...
echo This ensures parity with Docker environment setup

REM Create data directory if it doesn't exist
if not exist "data" (
    mkdir data
    echo ✓ Data directory created: %CD%\data
) else (
    echo ✓ Data directory exists: %CD%\data
)

REM Create empty database file if it doesn't exist (matches Docker setup)
if not exist "data\books.db" (
    type nul > "data\books.db"
    echo ✓ Database file created: %CD%\data\books.db
) else (
    echo ✓ Database file exists: %CD%\data\books.db
)

echo.
echo Data directory setup complete for Windows!
echo    Data directory: %CD%\data
echo    Database path: %CD%\data\books.db
echo    Permissions: Using Windows default file permissions
echo.
echo You can now run Bibliotheca using:
echo    python -m gunicorn -w 4 -b 0.0.0.0:5054 run:app
echo.
echo Or if you have gunicorn installed globally:
echo    gunicorn -w 4 -b 0.0.0.0:5054 run:app

pause
