#!/usr/bin/env python3
"""Test script for Gemini extraction functionality."""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from services.gemini_client import GeminiClient
from services.web_scraper import WebScraper

async def test_gemini_extraction():
    """Test Gemini extraction."""
    print("Testing Gemini extraction...")
    
    scraper = WebScraper()
    gemini_client = GeminiClient()
    
    # Test with a sample company page
    test_url = "https://personalnet.co.jp/"
    
    print(f"Fetching content from: {test_url}")
    
    # Fetch page content
    page_data = await scraper.fetch_page_content(test_url)
    
    if page_data["status"] != "success":
        print(f"Failed to fetch content: {page_data['status']}")
        return
    
    print(f"Content length: {page_data['content_length']} characters")
    print(f"Title: {page_data['title']}")
    
    # Extract with Gemini
    print("Extracting information with Gemini...")
    
    extracted_data = await gemini_client.extract_company_info(
        page_data["content"],
        "株式会社パーソナルネット",
        "通信業界"
    )
    
    print("Extracted data:")
    print(f"  Company: {extracted_data['company_name']}")
    print(f"  Address: {extracted_data['address_info']}")
    print(f"  Employee Count: {extracted_data['employee_count']}")
    print(f"  Founded Year: {extracted_data['founded_year']}")
    print(f"  Capital: {extracted_data['capital']}")
    print(f"  Services: {extracted_data['services']}")
    print(f"  Products: {extracted_data['products']}")
    print(f"  Business Description: {extracted_data['business_description']}")
    print(f"  Company Features: {extracted_data['company_features']}")
    print(f"  Status: {extracted_data['extraction_status']}")

if __name__ == "__main__":
    asyncio.run(test_gemini_extraction())

