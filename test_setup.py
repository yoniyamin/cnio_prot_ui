"""
Test script to verify font file access and logging setup.
"""
import os
import logging
from src.logging_utils import get_logger

# Create a test logger
logger = get_logger("test_setup")

def check_font_files():
    """Check that font files are present and accessible"""
    font_dir = os.path.join("static", "fonts")
    font_files = [
        "poppins-regular.woff2", 
        "Poppins-Medium.woff2", 
        "poppins-semibold.woff2"
    ]
    
    logger.info("Checking font files in %s", font_dir)
    missing_files = []
    
    for font_file in font_files:
        path = os.path.join(font_dir, font_file)
        if os.path.exists(path):
            file_size = os.path.getsize(path)
            logger.info("[OK] Font file found: %s (%d bytes)", font_file, file_size)
        else:
            logger.error("[MISSING] Font file missing: %s", font_file)
            missing_files.append(font_file)
    
    if missing_files:
        logger.error("Some font files are missing: %s", missing_files)
        return False
    else:
        logger.info("All font files available and correctly referenced in CSS")
        return True

def check_css_references():
    """Check that CSS references to Google Fonts have been removed"""
    css_files = [
        os.path.join("static", "style.css"),
        os.path.join("static", "css", "fonts.css")
    ]
    
    logger.info("Checking CSS files for Google Fonts references")
    google_fonts_found = False
    
    for css_file in css_files:
        if not os.path.exists(css_file):
            logger.warning("CSS file not found: %s", css_file)
            continue
            
        with open(css_file, "r", encoding="utf-8") as f:
            content = f.read()
            if "fonts.googleapis.com" in content:
                logger.error("[ERROR] Google Fonts reference found in %s", css_file)
                google_fonts_found = True
            else:
                logger.info("[OK] No Google Fonts references in %s", css_file)
    
    return not google_fonts_found

if __name__ == "__main__":
    logger.info("Starting setup verification")
    
    # Check font files
    fonts_ok = check_font_files()
    
    # Check CSS references
    css_ok = check_css_references()
    
    # Print summary
    if fonts_ok and css_ok:
        logger.info("SETUP VERIFICATION SUCCESSFUL")
        logger.info("Local fonts are correctly configured")
    else:
        logger.error("SETUP VERIFICATION FAILED")
        if not fonts_ok:
            logger.error("Font files issue: Some font files are missing or cannot be accessed")
        if not css_ok:
            logger.error("CSS issue: Google Fonts references still present")
    
    logger.info("Font and logging verification complete") 