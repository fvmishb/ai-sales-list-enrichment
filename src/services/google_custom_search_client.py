"""
Google Custom Search API client for accurate company information retrieval
"""
import asyncio
import logging
import aiohttp
import json
from typing import Dict, Any, List, Optional
from urllib.parse import quote

from ..config import settings

logger = logging.getLogger(__name__)

class GoogleCustomSearchClient:
    """Google Custom Search API client"""
    
    def __init__(self):
        self.api_key = settings.google_search_api_key
        self.cse_id = settings.google_cse_id
        self.base_url = "https://www.googleapis.com/customsearch/v1"
        
    async def search_company_info(self, company_name: str, website: str = "") -> Dict[str, Any]:
        """Search for comprehensive company information"""
        
        # 複数の検索クエリを試行
        queries = [
            f'"{company_name}" 会社概要',
            f'"{company_name}" 本社 住所',
            f'"{company_name}" 企業情報',
        ]
        
        if website:
            queries.insert(0, f'site:{website} 会社概要')
        
        all_results = []
        
        async with aiohttp.ClientSession() as session:
            for query in queries:
                results = await self._search(session, query)
                if results:
                    all_results.extend(results)
                await asyncio.sleep(0.5)  # Rate limiting
        
        # 検索結果から情報を抽出
        extracted_info = await self._extract_company_info(all_results, company_name)
        
        return extracted_info
    
    async def _search(self, session: aiohttp.ClientSession, query: str, num: int = 5) -> List[Dict[str, Any]]:
        """Execute a search query"""
        try:
            params = {
                'key': self.api_key,
                'cx': self.cse_id,
                'q': query,
                'num': num,
                'lr': 'lang_ja'
            }
            
            async with session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('items', [])
                else:
                    error_text = await response.text()
                    logger.error(f"Search API error: {response.status} - {error_text}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error searching: {e}")
            return []
    
    async def _extract_company_info(self, search_results: List[Dict[str, Any]], company_name: str) -> Dict[str, Any]:
        """Extract company information from search results"""
        
        info = {
            "name_legal": "",
            "hq_address_raw": "",
            "prefecture_name": "",
            "overview_text": "",
            "services_text": [],
            "products_text": [],
            "employee_count": None,
            "employee_count_source_url": "",
            "inquiry_url": "",
            "sources": []
        }
        
        # 各検索結果からコンテンツをスクレイピング
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            for result in search_results[:5]:  # 上位5件
                try:
                    url = result.get('link', '')
                    snippet = result.get('snippet', '')
                    
                    # スニペットから情報を抽出
                    self._extract_from_snippet(snippet, info)
                    
                    # ページをスクレイピング
                    page_content = await self._fetch_page(session, url)
                    if page_content:
                        self._extract_from_page(page_content, info, company_name)
                        info['sources'].append(url)
                    
                    await asyncio.sleep(0.3)  # Rate limiting
                    
                except Exception as e:
                    logger.debug(f"Error processing result: {e}")
                    continue
        
        return info
    
    async def _fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetch page content"""
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.debug(f"Error fetching {url}: {e}")
        return None
    
    def _extract_from_snippet(self, snippet: str, info: Dict[str, Any]):
        """Extract information from search snippet"""
        import re
        
        # 住所パターン
        address_patterns = [
            r'〒\d{3}-\d{4}[^。]*[都道府県][^。]*',
            r'[都道府県][^。]*[市区町村][^。]*\d+',
        ]
        
        for pattern in address_patterns:
            match = re.search(pattern, snippet)
            if match and not info['hq_address_raw']:
                info['hq_address_raw'] = match.group(0)
                info['prefecture_name'] = self._extract_prefecture(match.group(0))
                break
        
        # 従業員数パターン
        employee_pattern = r'従業員[：:]\s*(\d+)[名人]'
        match = re.search(employee_pattern, snippet)
        if match and not info['employee_count']:
            info['employee_count'] = int(match.group(1))
    
    def _extract_from_page(self, html: str, info: Dict[str, Any], company_name: str):
        """Extract information from page HTML"""
        from bs4 import BeautifulSoup
        import re
        
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        
        # 住所の抽出
        if not info['hq_address_raw']:
            address_patterns = [
                r'〒\d{3}-\d{4}[^。]*[都道府県][^。]*[市区町村][^。]*\d+[^。]*',
                r'本社所在地[：:]\s*([都道府県][^。]*)',
            ]
            
            for pattern in address_patterns:
                match = re.search(pattern, text)
                if match:
                    info['hq_address_raw'] = match.group(0) if '所在地' not in pattern else match.group(1)
                    info['prefecture_name'] = self._extract_prefecture(info['hq_address_raw'])
                    break
        
        # 従業員数の抽出
        if not info['employee_count']:
            employee_patterns = [
                r'従業員数[：:]\s*(\d+)[名人]',
                r'社員数[：:]\s*(\d+)[名人]',
            ]
            
            for pattern in employee_patterns:
                match = re.search(pattern, text)
                if match:
                    info['employee_count'] = int(match.group(1))
                    break
        
        # 概要テキストの抽出
        if not info['overview_text']:
            overview_sections = soup.find_all(text=re.compile('会社概要|企業概要|会社案内'))
            for section in overview_sections[:1]:
                parent = section.parent
                if parent:
                    text_content = parent.get_text()[:500]
                    if len(text_content) > 100:
                        info['overview_text'] = text_content.strip()
                        break
    
    def _extract_prefecture(self, address: str) -> str:
        """Extract prefecture from address"""
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

