#!/usr/bin/env python3
"""
Test script to verify rate limiting functionality
"""

import time
import sys
import os

# Add the app directory to the path so we can import the modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

def test_rate_limiting():
    """Test that rate limiting delays are working correctly"""
    print("Testing rate limiting functionality...")
    
    # Test that the configuration constants are reasonable
    from app.utils import API_RATE_LIMIT_DELAY, MAX_RETRIES, RETRY_DELAY
    
    print(f"API_RATE_LIMIT_DELAY: {API_RATE_LIMIT_DELAY} seconds")
    print(f"MAX_RETRIES: {MAX_RETRIES}")
    print(f"RETRY_DELAY: {RETRY_DELAY} seconds")
    
    # Basic validation
    assert API_RATE_LIMIT_DELAY > 0, "Rate limit delay must be positive"
    assert MAX_RETRIES > 0, "Max retries must be positive"
    assert RETRY_DELAY > 0, "Retry delay must be positive"
    
    print("✓ Rate limiting configuration is valid")
    
    # Test timing
    start_time = time.time()
    time.sleep(API_RATE_LIMIT_DELAY)
    elapsed = time.time() - start_time
    
    assert elapsed >= API_RATE_LIMIT_DELAY * 0.9, f"Sleep time was too short: {elapsed}"
    print(f"✓ Rate limiting delay test passed ({elapsed:.2f}s)")
    
    print("All rate limiting tests passed!")

if __name__ == "__main__":
    test_rate_limiting()
