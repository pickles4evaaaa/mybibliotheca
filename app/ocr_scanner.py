"""
OCR Barcode Scanner for Bibliotheca.

This module provides lightweight OCR functionality to extract ISBNs from
uploaded images using barcode detection and OCR fallback.
"""

import re
import tempfile
import os
from typing import Optional, List, Tuple
import logging

try:
    from PIL import Image, ImageEnhance, ImageFilter
    import cv2
    import numpy as np
    from pyzbar import pyzbar
    import pytesseract
    OCR_AVAILABLE = True
except ImportError as e:
    OCR_AVAILABLE = False
    logging.warning(f"OCR dependencies not available: {e}")

logger = logging.getLogger(__name__)

class ISBNExtractor:
    """Lightweight OCR barcode scanner for ISBN extraction."""
    
    def __init__(self):
        self.isbn_patterns = [
            # ISBN-13 (starts with 978 or 979)
            r'(?:978|979)[-\s]?(?:\d[-\s]?){9}\d',
            # ISBN-10 
            r'(?:\d[-\s]?){9}[\dXx]',
            # General pattern for any 10 or 13 digit sequence that might be an ISBN
            r'\b(?:978|979)?[-\s]?(?:\d[-\s]?){8,12}\d\b'
        ]
    
    def extract_isbn_from_image(self, image_file) -> Optional[str]:
        """
        Extract ISBN from an uploaded image file.
        
        Args:
            image_file: Flask file upload object
            
        Returns:
            str: Extracted ISBN if found, None otherwise
        """
        if not OCR_AVAILABLE:
            logger.error("OCR dependencies not installed")
            return None
        
        try:
            # Save uploaded file temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as temp_file:
                image_file.save(temp_file.name)
                temp_path = temp_file.name
            
            try:
                # Try barcode detection first (most reliable)
                isbn = self._extract_from_barcode(temp_path)
                if isbn:
                    logger.info(f"ISBN extracted from barcode: {isbn}")
                    return isbn
                
                # Fall back to OCR
                isbn = self._extract_from_ocr(temp_path)
                if isbn:
                    logger.info(f"ISBN extracted from OCR: {isbn}")
                    return isbn
                
                logger.warning("No ISBN found in image")
                return None
                
            finally:
                # Clean up temporary file
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                    
        except Exception as e:
            logger.error(f"Error extracting ISBN from image: {e}")
            return None
    
    def _extract_from_barcode(self, image_path: str) -> Optional[str]:
        """Extract ISBN from barcode using pyzbar."""
        try:
            # Read image
            image = cv2.imread(image_path)
            if image is None:
                return None
            
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Detect barcodes
            barcodes = pyzbar.decode(gray)
            
            for barcode in barcodes:
                # Decode barcode data
                barcode_data = barcode.data.decode('utf-8')
                barcode_type = barcode.type
                
                logger.debug(f"Found barcode: {barcode_data} (type: {barcode_type})")
                
                # Check if it's a valid ISBN
                isbn = self._validate_and_clean_isbn(barcode_data)
                if isbn:
                    return isbn
            
            return None
            
        except Exception as e:
            logger.error(f"Error in barcode detection: {e}")
            return None
    
    def _extract_from_ocr(self, image_path: str) -> Optional[str]:
        """Extract ISBN using OCR as fallback."""
        try:
            # Open and preprocess image
            image = Image.open(image_path)
            
            # Try multiple preprocessing approaches
            preprocessed_images = self._preprocess_image_for_ocr(image)
            
            for processed_img in preprocessed_images:
                try:
                    # Extract text using OCR
                    text = pytesseract.image_to_string(processed_img, config='--psm 6')
                    logger.debug(f"OCR extracted text: {text[:200]}...")
                    
                    # Find ISBN patterns in extracted text
                    isbn = self._find_isbn_in_text(text)
                    if isbn:
                        return isbn
                        
                except Exception as e:
                    logger.debug(f"OCR attempt failed: {e}")
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"Error in OCR processing: {e}")
            return None
    
    def _preprocess_image_for_ocr(self, image: Image.Image) -> List[Image.Image]:
        """Preprocess image for better OCR results."""
        processed_images = []
        
        try:
            # Convert to grayscale
            gray = image.convert('L')
            processed_images.append(gray)
            
            # Enhance contrast
            enhancer = ImageEnhance.Contrast(gray)
            high_contrast = enhancer.enhance(2.0)
            processed_images.append(high_contrast)
            
            # Apply sharpening
            sharp = gray.filter(ImageFilter.SHARPEN)
            processed_images.append(sharp)
            
            # Resize for better OCR (if image is very small or very large)
            width, height = gray.size
            if width < 300 or height < 300:
                # Upscale small images
                new_size = (width * 2, height * 2)
                upscaled = gray.resize(new_size, Image.Resampling.LANCZOS)
                processed_images.append(upscaled)
            elif width > 2000 or height > 2000:
                # Downscale large images
                new_size = (width // 2, height // 2)
                downscaled = gray.resize(new_size, Image.Resampling.LANCZOS)
                processed_images.append(downscaled)
            
        except Exception as e:
            logger.warning(f"Error in image preprocessing: {e}")
            # Return original image as fallback
            processed_images = [image.convert('L')]
        
        return processed_images
    
    def _find_isbn_in_text(self, text: str) -> Optional[str]:
        """Find and validate ISBN in extracted text."""
        # Clean and normalize text
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Try each ISBN pattern
        for pattern in self.isbn_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                isbn = self._validate_and_clean_isbn(match)
                if isbn:
                    return isbn
        
        return None
    
    def _validate_and_clean_isbn(self, isbn_candidate: str) -> Optional[str]:
        """Validate and clean an ISBN candidate."""
        if not isbn_candidate:
            return None
        
        # Remove all non-digit characters except X
        cleaned = re.sub(r'[^\dXx]', '', isbn_candidate.strip())
        
        # Check length
        if len(cleaned) not in [10, 13]:
            return None
        
        # Basic validation
        if len(cleaned) == 13:
            # ISBN-13 should start with 978 or 979
            if not cleaned.startswith(('978', '979')):
                return None
            return cleaned
        
        elif len(cleaned) == 10:
            # ISBN-10 validation (basic)
            if cleaned[-1].upper() not in '0123456789X':
                return None
            return cleaned
        
        return None


def extract_isbn_from_image(image_file) -> Optional[str]:
    """
    Convenience function to extract ISBN from image file.
    
    Args:
        image_file: Flask file upload object
        
    Returns:
        str: Extracted ISBN if found, None otherwise
    """
    extractor = ISBNExtractor()
    return extractor.extract_isbn_from_image(image_file)


def is_ocr_available() -> bool:
    """Check if OCR dependencies are available."""
    return OCR_AVAILABLE
