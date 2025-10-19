#!/usr/bin/env python3
"""
特定企業での住所検索テストスクリプト
"""

import asyncio
import logging
import aiohttp
import re
from bs4 import BeautifulSoup
from urllib.parse import quote

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_company_search(company_name: str, website: str = ""):
    """特定企業の住所検索をテスト"""
    logger.info(f"Testing search for: {company_name}")
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    ) as session:
        
        # 検索クエリ
        queries = [
            f'"{company_name}" 本社 住所',
            f'"{company_name}" 会社概要',
            f'"{company_name}" 所在地',
            f'site:{website} 会社概要' if website else None
        ]
        
        for query in queries:
            if not query:
                continue
                
            try:
                logger.info(f"Searching: {query}")
                search_url = f"https://www.google.com/search?q={quote(query)}&num=10"
                
                async with session.get(search_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # 検索結果のテキストを取得
                        text_content = soup.get_text()
                        
                        # 住所パターンを検索
                        address_patterns = [
                            r'〒\d{3}-\d{4}[^。]*',
                            r'[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
                            r'[都道府県][^。]*[市区町村][^。]*',
                        ]
                        
                        found_addresses = []
                        for pattern in address_patterns:
                            matches = re.findall(pattern, text_content)
                            for match in matches:
                                if len(match) > 15:
                                    found_addresses.append(match.strip())
                        
                        if found_addresses:
                            logger.info(f"Found addresses: {found_addresses[:3]}")  # 最初の3件
                            return found_addresses[0]
                        else:
                            logger.info("No addresses found in search results")
                            
                        # 検索結果のリンクを取得
                        links = []
                        for link in soup.find_all('a', href=True):
                            href = link['href']
                            if href.startswith('/url?q='):
                                href = href.split('/url?q=')[1].split('&')[0]
                            if href.startswith('http') and 'google.com' not in href:
                                links.append(href)
                        
                        logger.info(f"Found {len(links)} links: {links[:3]}")
                        
                        # 最初のリンクをスクレイピング
                        if links:
                            try:
                                async with session.get(links[0]) as response:
                                    if response.status == 200:
                                        html = await response.text()
                                        soup = BeautifulSoup(html, 'html.parser')
                                        text_content = soup.get_text()
                                        
                                        for pattern in address_patterns:
                                            matches = re.findall(pattern, text_content)
                                            for match in matches:
                                                if len(match) > 15:
                                                    logger.info(f"Found address in scraped page: {match.strip()}")
                                                    return match.strip()
                            except Exception as e:
                                logger.debug(f"Failed to scrape {links[0]}: {e}")
                    else:
                        logger.warning(f"Search failed with status {response.status}")
                        
                await asyncio.sleep(2)  # 検索間隔
                
            except Exception as e:
                logger.error(f"Error searching '{query}': {e}")
                continue
        
        logger.warning(f"No address found for: {company_name}")
        return None

async def main():
    """メイン実行関数"""
    test_companies = [
        ("NTT西日本株式会社", "https://www.ntt-west.co.jp"),
        ("楽天コミュニケーションズ株式会社", "https://comm.rakuten.co.jp"),
        ("KDDI株式会社", "https://www.kddi.com/"),
        ("ソフトバンクグループ株式会社", ""),
        ("株式会社NTTデータ", "")
    ]
    
    for company_name, website in test_companies:
        result = await test_company_search(company_name, website)
        if result:
            logger.info(f"SUCCESS: {company_name} -> {result}")
        else:
            logger.warning(f"FAILED: {company_name}")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(main())

