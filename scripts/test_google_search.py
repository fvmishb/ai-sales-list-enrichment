#!/usr/bin/env python3
"""Test script for Google Search API functionality."""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from services.google_search import GoogleSearchClient

async def test_google_search():
    """Test Google Search API."""
    print("Testing Google Search API...")
    
    client = GoogleSearchClient()
    
    # Test company site search
    test_company = {
        "name": "株式会社パーソナルネット",
        "website": "https://personalnet.co.jp/",
        "industry": "通信業界"
    }
    
    domain = test_company["website"].replace("https://", "").replace("http://", "").split("/")[0]
    
    print(f"Searching for: {test_company['name']} on {domain}")
    
    results = await client.search_company_site(
        domain, 
        test_company["name"], 
        max_results=5
    )
    
    print(f"Found {len(results)} results:")
    for i, result in enumerate(results, 1):
        print(f"{i}. {result['title']}")
        print(f"   URL: {result['url']}")
        print(f"   Type: {result['page_type']}")
        print(f"   Relevance: {result['relevance_score']:.2f}")
        print(f"   Snippet: {result['snippet'][:100]}...")
        print()
    
    # Test address search
    print("Testing address search...")
    address_results = await client.search_address_specific(
        test_company["name"],
        test_company["industry"]
    )
    
    print(f"Found {len(address_results)} address results:")
    for i, result in enumerate(address_results, 1):
        print(f"{i}. {result['title']}")
        print(f"   URL: {result['url']}")
        print(f"   Address Relevance: {result['address_relevance']:.2f}")
        print(f"   Snippet: {result['snippet'][:100]}...")
        print()

if __name__ == "__main__":
    asyncio.run(test_google_search())

