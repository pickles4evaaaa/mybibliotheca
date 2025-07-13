# OCR Barcode Scanner for Bibliotheca

This feature allows you to extract ISBNs from uploaded images using OCR (Optical Character Recognition) and barcode detection.

## Features

- **Barcode Detection**: Automatically detects and reads EAN/UPC barcodes containing ISBNs
- **OCR Fallback**: Uses text recognition to find ISBN numbers in images when barcode detection fails
- **Multiple Image Formats**: Supports PNG, JPEG, GIF, BMP, TIFF, and WebP
- **Smart Preprocessing**: Automatically enhances images for better recognition
- **ISBN Validation**: Validates extracted ISBNs to ensure accuracy

## Installation

### Automatic Installation (Recommended)

**Linux/macOS:**
```bash
./scripts/install_ocr.sh
```

**Windows:**
```cmd
scripts\install_ocr.bat
```

### Manual Installation

1. Install Python dependencies:
```bash
pip install opencv-python==4.8.1.78 pyzbar==0.1.9 pytesseract==0.3.10 numpy>=1.21.0
```

2. Install system dependencies:

**macOS (with Homebrew):**
```bash
brew install tesseract zbar
```

**Ubuntu/Debian:**
```bash
sudo apt-get install tesseract-ocr libzbar0
```

**CentOS/RHEL:**
```bash
sudo yum install tesseract zbar
```

**Windows:**
- Download Tesseract from: https://github.com/UB-Mannheim/tesseract/wiki
- Install and add to PATH
- Install Visual C++ Redistributable if needed

## Usage

1. Go to the **Add Book** page
2. Click the **"Upload Image"** button (📷)
3. Select an image containing a barcode or visible ISBN text
4. The system will:
   - Process the image
   - Extract the ISBN
   - Automatically populate the ISBN field
   - Optionally fetch book data if available

## Supported Image Types

- **Best Results**: Clear, well-lit images with visible barcodes
- **Good Results**: Images with clearly readable ISBN text
- **Formats**: PNG, JPEG, GIF, BMP, TIFF, WebP
- **Size Limit**: 10MB maximum

## Tips for Best Results

1. **For Barcodes**:
   - Ensure the barcode is clearly visible and well-lit
   - Avoid shadows or reflections on the barcode
   - Keep the camera steady and at an appropriate distance

2. **For Text-based ISBNs**:
   - Make sure the ISBN text is clear and readable
   - Good contrast between text and background
   - Avoid skewed or rotated images when possible

3. **General Tips**:
   - Use good lighting conditions
   - Avoid blurry images
   - Crop the image to focus on the ISBN area if possible

## How It Works

1. **Barcode Detection**: Uses `pyzbar` to detect and decode barcodes
2. **Image Preprocessing**: Enhances image quality using `opencv-python`
3. **OCR Processing**: Uses `pytesseract` for text recognition as fallback
4. **ISBN Validation**: Validates extracted ISBNs using pattern matching
5. **Book Data Lookup**: Automatically fetches book information if ISBN is found

## Troubleshooting

### Common Issues

**"OCR functionality not available"**
- Run the installation script or manually install dependencies
- Check that Tesseract is properly installed and in PATH

**"No ISBN found in image"**
- Try a clearer image with better lighting
- Ensure the barcode or ISBN text is visible
- Try cropping the image to focus on the ISBN area

**Import errors**
- Make sure all dependencies are installed in the correct environment
- Check that you're using the same Python environment as your Flask app

### Debug Mode

The OCR system includes detailed logging. Check the Flask logs for debug information if issues occur.

## Technical Details

### Dependencies

- **opencv-python**: Image preprocessing and computer vision
- **pyzbar**: Barcode detection and decoding
- **pytesseract**: OCR text recognition
- **numpy**: Numerical operations for image processing
- **Pillow**: Image manipulation (already included in Bibliotheca)

### Supported Barcode Types

- EAN-13 (most common for books)
- EAN-8
- UPC-A
- UPC-E
- Code 39

### ISBN Validation

The system validates ISBNs using:
- Length validation (10 or 13 digits)
- Format validation (ISBN-13 must start with 978 or 979)
- Pattern matching for various ISBN formats

## Security Considerations

- File type validation prevents malicious uploads
- File size limits (10MB) prevent abuse
- Temporary files are automatically cleaned up
- No persistent storage of uploaded images

## Performance

- Typical processing time: 1-3 seconds per image
- Memory usage: ~10-50MB per image during processing
- Supports concurrent requests
- Automatic cleanup prevents memory leaks
