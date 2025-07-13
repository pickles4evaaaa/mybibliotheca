#!/bin/bash

# OCR Dependencies Installation Script for Bibliotheca
# This script installs the required dependencies for OCR barcode scanning

echo "🔧 Installing OCR dependencies for Bibliotheca..."

# Check if we're in a virtual environment
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment detected: $VIRTUAL_ENV"
else
    echo "⚠️  Warning: No virtual environment detected. Consider using one."
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Install Python packages
echo "📦 Installing Python packages..."
pip install opencv-python==4.8.1.78 pyzbar==0.1.9 pytesseract==0.3.10 numpy>=1.21.0

# Check for system dependencies
echo "🔍 Checking system dependencies..."

# Check for Tesseract OCR
if ! command -v tesseract &> /dev/null; then
    echo "⚠️  Tesseract OCR not found. Installing instructions:"
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        echo "  For macOS, install using Homebrew:"
        echo "  brew install tesseract"
        
        if command -v brew &> /dev/null; then
            read -p "  Install Tesseract using Homebrew now? (y/N): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                brew install tesseract
            fi
        fi
        
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        echo "  For Ubuntu/Debian:"
        echo "  sudo apt-get install tesseract-ocr"
        echo "  For CentOS/RHEL:"
        echo "  sudo yum install tesseract"
        
    else
        echo "  Please install Tesseract OCR manually for your system"
        echo "  Visit: https://github.com/tesseract-ocr/tesseract"
    fi
else
    echo "✅ Tesseract OCR found: $(tesseract --version | head -1)"
fi

# Check for libzbar (required by pyzbar)
echo "🔍 Checking for libzbar..."

if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    if ! brew list zbar &> /dev/null; then
        echo "⚠️  libzbar not found. Installing with Homebrew..."
        if command -v brew &> /dev/null; then
            brew install zbar
        else
            echo "  Please install Homebrew first: https://brew.sh/"
            echo "  Then run: brew install zbar"
        fi
    else
        echo "✅ libzbar found"
    fi
    
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    if ! ldconfig -p | grep libzbar &> /dev/null; then
        echo "⚠️  libzbar not found. Install with:"
        echo "  Ubuntu/Debian: sudo apt-get install libzbar0"
        echo "  CentOS/RHEL: sudo yum install zbar"
    else
        echo "✅ libzbar found"
    fi
fi

echo ""
echo "🧪 Testing OCR functionality..."
python3 -c "
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

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Installation complete! OCR barcode scanning is now available."
    echo ""
    echo "Usage:"
    echo "  1. Go to Add Book page"
    echo "  2. Click 'Upload Image' button"
    echo "  3. Select an image containing a barcode or ISBN text"
    echo "  4. The ISBN will be automatically extracted and populated"
else
    echo ""
    echo "❌ Installation failed. Please check the error messages above."
    exit 1
fi
