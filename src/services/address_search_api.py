"""
正確な住所検索API
Cloud Run環境でGoogle検索を実行し、実際の住所を取得
"""

import asyncio
import logging
import aiohttp
import json
import re
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin, urlparse
import random

logger = logging.getLogger(__name__)

class AccurateAddressSearcher:
    """正確な住所検索クラス"""
    
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def _generate_search_queries(self, company_name: str, website: str = "") -> List[str]:
        """検索クエリを生成"""
        queries = [
            f'"{company_name}" 本社 住所',
            f'"{company_name}" 会社概要 住所',
            f'"{company_name}" 企業情報 所在地',
            f'"{company_name}" お問い合わせ 住所',
            f'"{company_name}" アクセス 住所',
            f'"{company_name}" 本社所在地'
        ]
        
        if website:
            domain = urlparse(website).netloc
            if domain:
                queries.extend([
                    f'site:{domain} 会社概要',
                    f'site:{domain} 企業情報',
                    f'site:{domain} お問い合わせ',
                    f'site:{domain} アクセス'
                ])
        
        return queries
    
    async def _search_google(self, query: str) -> Optional[BeautifulSoup]:
        """Google検索を実行"""
        try:
            search_url = f"https://www.google.com/search?q={quote(query)}&num=10"
            
            # ランダムな待機時間（API制限対策）
            await asyncio.sleep(random.uniform(1, 3))
            
            async with self.session.get(search_url) as response:
                if response.status == 200:
                    html = await response.text()
                    return BeautifulSoup(html, 'html.parser')
                else:
                    logger.warning(f"Google search failed with status {response.status}")
                    return None
                    
        except Exception as e:
            logger.warning(f"Google search error for query '{query}': {e}")
            return None
    
    def _extract_address_from_search_results(self, soup: BeautifulSoup, company_name: str) -> Optional[Dict[str, str]]:
        """検索結果から住所を抽出"""
        if not soup:
            logger.warning("No soup content to extract from")
            return None
        
        text_content = soup.get_text()
        logger.info(f"Search result text length: {len(text_content)}")
        logger.info(f"Search result text preview: {text_content[:200]}...")
        
        # より厳密な住所パターン
        address_patterns = [
            r'〒\d{3}-\d{4}[^。]*[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
            r'〒\d{3}-\d{4}[^。]*',
        ]
        
        for i, pattern in enumerate(address_patterns):
            matches = re.findall(pattern, text_content)
            logger.info(f"Pattern {i+1} found {len(matches)} matches")
            for j, match in enumerate(matches):
                logger.info(f"Match {j+1}: {match[:100]}...")
                if len(match) > 20 and (company_name in text_content or len(match) > 25):
                    prefecture = self._extract_prefecture(match)
                    logger.info(f"Extracted prefecture: {prefecture}")
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        # より緩いパターンも試す
        loose_patterns = [
            r'[都道府県][^。]*[市区町村]',
            r'〒\d{3}-\d{4}',
        ]
        
        for i, pattern in enumerate(loose_patterns):
            matches = re.findall(pattern, text_content)
            logger.info(f"Loose pattern {i+1} found {len(matches)} matches")
            for j, match in enumerate(matches):
                logger.info(f"Loose match {j+1}: {match[:100]}...")
                if len(match) > 10:
                    prefecture = self._extract_prefecture(match)
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        logger.warning("No valid address pattern found in search results")
        return None
    
    def _extract_search_links(self, soup: BeautifulSoup) -> List[str]:
        """検索結果からリンクを抽出"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('/url?q='):
                href = href.split('/url?q=')[1].split('&')[0]
            if href.startswith('http') and 'google.com' not in href and 'youtube.com' not in href:
                links.append(href)
        return links[:5]  # 上位5件
    
    async def _scrape_company_page(self, url: str, company_name: str) -> Optional[Dict[str, str]]:
        """企業ページから住所を抽出"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 住所関連のセクションを探す
                    address_sections = [
                        '会社概要', '企業情報', '会社案内', '会社データ', 
                        'お問い合わせ', 'アクセス', '所在地', '本社',
                        '会社情報', '企業概要', '会社プロフィール', '本社所在地'
                    ]
                    
                    for section in address_sections:
                        elements = soup.find_all(text=re.compile(section, re.IGNORECASE))
                        for element in elements:
                            parent = element.parent
                            if parent:
                                text_content = parent.get_text()
                                address_info = self._extract_address_from_text(text_content, company_name)
                                if address_info:
                                    return address_info
                    
                    # 全体のテキストから住所を探す
                    full_text = soup.get_text()
                    address_info = self._extract_address_from_text(full_text, company_name)
                    if address_info:
                        return address_info
                        
        except Exception as e:
            logger.debug(f"Failed to scrape {url}: {e}")
            
        return None
    
    def _extract_address_from_text(self, text: str, company_name: str) -> Optional[Dict[str, str]]:
        """テキストから住所を抽出"""
        # より厳密な住所パターン
        address_patterns = [
            r'〒\d{3}-\d{4}[^。]*[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if len(match) > 20:  # 十分な長さの住所
                    prefecture = self._extract_prefecture(match)
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        return None
    
    def _extract_prefecture(self, address: str) -> str:
        """住所から都道府県を抽出"""
        prefectures = [
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
        ]
        
        for prefecture in prefectures:
            if prefecture in address:
                return prefecture
        
        return "不明"
    
    def _validate_address(self, address: str, prefecture: str) -> bool:
        """住所の品質を検証"""
        if not address or not prefecture:
            return False
        
        # NGワードチェック
        ng_words = ["不明", "要確認", "推測", "本社所在地", "詳細住所は要確認", "内"]
        if any(word in address for word in ng_words):
            return False
        
        # 最小文字数チェック
        if len(address) < 20:
            return False
        
        # 都道府県が47都道府県のいずれかに一致するか
        valid_prefectures = [
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
        ]
        
        return prefecture in valid_prefectures
    
    def _clean_address(self, address: str) -> str:
        """住所を整形"""
        # 改行、余分な空白を削除
        address = re.sub(r'\s+', ' ', address.strip())
        
        # 電話番号、FAXなどの不要情報を除去
        address = re.sub(r'TEL[：:]\d+[-\d]*', '', address)
        address = re.sub(r'FAX[：:]\d+[-\d]*', '', address)
        address = re.sub(r'電話[：:]\d+[-\d]*', '', address)
        address = re.sub(r'フリーコール[：:]\d+[-\d]*', '', address)
        address = re.sub(r'Copyright.*', '', address)
        
        return address.strip()
    
    async def search_company_address(self, company_name: str, website: str = "") -> Optional[Dict[str, str]]:
        """企業の住所を検索・抽出"""
        logger.info(f"Searching accurate address for: {company_name}")
        
        # 検索クエリを生成
        queries = self._generate_search_queries(company_name, website)
        
        # 各クエリで検索を試行
        for query in queries:
            try:
                logger.info(f"Trying search query: {query}")
                # Google検索を実行
                soup = await self._search_google(query)
                if not soup:
                    logger.warning(f"No search results for query: {query}")
                    continue
                
                # 検索結果から住所を抽出
                address_info = self._extract_address_from_search_results(soup, company_name)
                logger.info(f"Extracted address info: {address_info}")
                if address_info and self._validate_address(address_info['address'], address_info['prefecture']):
                    address_info['address'] = self._clean_address(address_info['address'])
                    logger.info(f"Found accurate address via search: {address_info}")
                    return address_info
                else:
                    logger.warning(f"Address validation failed for: {address_info}")
                
                # 検索結果のリンクをスクレイピング
                links = self._extract_search_links(soup)
                for link in links:
                    try:
                        address_info = await self._scrape_company_page(link, company_name)
                        if address_info and self._validate_address(address_info['address'], address_info['prefecture']):
                            address_info['address'] = self._clean_address(address_info['address'])
                            logger.info(f"Found accurate address via scraping: {address_info}")
                            return address_info
                    except Exception as e:
                        logger.debug(f"Failed to scrape {link}: {e}")
                        continue
                        
            except Exception as e:
                logger.debug(f"Search query failed: {query} - {e}")
                continue
        
        logger.warning(f"No accurate address found for: {company_name}")
        return None
