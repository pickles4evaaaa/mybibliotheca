@echo off
rem OCR Dependencies Installation Script for Bibliotheca (Windows)
rem This script installs the required dependencies for OCR barcode scanning

echo 🔧 Installing OCR dependencies for Bibliotheca...

rem Check if we're in a virtual environment
if defined VIRTUAL_ENV (
    echo ✅ Virtual environment detected: %VIRTUAL_ENV%
) else (
    echo ⚠️ Warning: No virtual environment detected. Consider using one.
    set /p "choice=Continue anyway? (y/N): "
    if /i not "%choice%"=="y" exit /b 1
)

rem Install Python packages
echo 📦 Installing Python packages...
pip install opencv-python==4.8.1.78 pyzbar==0.1.9 pytesseract==0.3.10 numpy>=1.21.0

rem Check for Tesseract OCR
echo 🔍 Checking for Tesseract OCR...
where tesseract >nul 2>&1
if errorlevel 1 (
    echo ⚠️ Tesseract OCR not found.
    echo   Download and install from: https://github.com/UB-Mannheim/tesseract/wiki
    echo   Make sure to add it to your PATH environment variable
    echo   Default installation path: C:\Program Files\Tesseract-OCR\tesseract.exe
) else (
    echo ✅ Tesseract OCR found
    tesseract --version | findstr "tesseract"
)

echo.
echo 🧪 Testing OCR functionality...
python -c "
try:
    from app.ocr_scanner import is_ocr_available
    if is_ocr_available():
        print('✅ OCR functionality is ready!')
    else:
        print('❌ OCR dependencies not properly installed')
        exit(1)
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
"

if errorlevel 1 (
    echo.
    echo ❌ Installation failed. Please check the error messages above.
    echo.
    echo Additional Notes for Windows:
    echo   - Make sure Microsoft Visual C++ Redistributable is installed
    echo   - For pyzbar, you may need to install vcredist_x64.exe
    echo   - Download from: https://aka.ms/vs/17/release/vc_redist.x64.exe
    pause
    exit /b 1
) else (
    echo.
    echo 🎉 Installation complete! OCR barcode scanning is now available.
    echo.
    echo Usage:
    echo   1. Go to Add Book page
    echo   2. Click 'Upload Image' button
    echo   3. Select an image containing a barcode or ISBN text
    echo   4. The ISBN will be automatically extracted and populated
    echo.
    pause
)
