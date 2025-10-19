"""Perplexity API client for enterprise data enrichment."""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp import ClientTimeout

from ..config import settings

logger = logging.getLogger(__name__)


class PerplexityClient:
    """Perplexity API client with Search API and Sonar models."""
    
    def __init__(self):
        self.api_key = settings.pplx_api_key
        self.search_url = "https://api.perplexity.ai/search"
        self.chat_url = "https://api.perplexity.ai/chat/completions"
        self.timeout = ClientTimeout(total=30)
        
    async def search(self, query: str, max_results: int = 20) -> Dict[str, Any]:
        """Search for candidate URLs using Perplexity Search API."""
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
                
                payload = {
                    "query": query,
                    "max_results": max_results,
                    "max_tokens_per_page": 1024
                }
                
                async with session.post(self.search_url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Search API success: {len(data.get('results', []))} results")
                        return data
                    else:
                        error_text = await response.text()
                        logger.error(f"Search API error {response.status}: {error_text}")
                        raise Exception(f"Search API error {response.status}: {error_text}")
                        
        except Exception as e:
            logger.error(f"Search API request failed: {e}")
            raise
    
    async def extract(self, urls: List[str], company_name: str) -> Dict[str, Any]:
        """Extract information from URLs using Sonar model."""
        try:
            # Create detailed extraction query
            query = f"""
以下のURL群から、企業「{company_name}」に関する詳細な情報を抽出してください：

URL群: {', '.join(urls[:5])}

抽出すべき情報：
1. 本社住所（必須）：
   - 都道府県名（例：東京都、大阪府、愛知県など）
   - 市区町村名（例：渋谷区、中央区、名古屋市など）
   - 番地・建物名（例：1-2-3、○○ビル3階など）
   - 郵便番号（見つかる場合）
   - 会社概要、アクセス、所在地、本社などのページから詳細な住所を探す
2. 従業員数（数値と単位）
3. 主要なサービスまたは製品のリスト（具体的なサービス名、製品名）
4. 事業内容の詳細（業界に応じた具体的な事業内容）
5. 使用技術・手法・ノウハウ（業界に応じた技術、手法、専門性など）
6. 企業の特徴や強み（技術的特徴、事業領域、設立年、資本金、独自性など）
7. 直近12〜18ヶ月の重要なニュース見出しまたはプレスリリース（3つまで）
8. 会社概要ページの詳細な事業説明

抽出した情報は以下のJSON形式で出力してください：
{{
  "address_lines": ["住所情報1", "住所情報2"],
  "employee_mentions": ["従業員数情報1", "従業員数情報2"],
  "service_heads": ["サービス1", "サービス2", "サービス3"],
  "product_heads": ["製品1", "製品2", "製品3"],
  "news_headlines": ["ニュース1", "ニュース2", "ニュース3"],
  "business_details": ["事業詳細1", "事業詳細2"],
  "company_features": ["特徴1", "特徴2", "特徴3"],
  "tech_stack": ["技術・手法1", "技術・手法2", "技術・手法3"],
  "company_description": "会社概要ページの詳細な事業説明文"
}}
"""
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
                
                payload = {
                    "model": "sonar",
                    "messages": [
                        {
                            "role": "system",
                            "content": "あなたは企業情報抽出の専門家です。与えられたURLから正確で詳細な企業情報を抽出し、指定されたJSON形式で出力します。情報が見つからない場合は空の配列を返してください。"
                        },
                        {
                            "role": "user",
                            "content": query
                        }
                    ],
                    "stream": False
                }
                
                async with session.post(self.chat_url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        content = data.get("choices", [{}])[0].get("message", {}).get("content", "{}")
                        
                        try:
                            # Try to parse as JSON
                            extracted_data = json.loads(content)
                            logger.info(f"Sonar extraction success for {company_name}")
                            return extracted_data
                        except json.JSONDecodeError:
                            # Fallback to basic text extraction
                            logger.warning(f"Sonar response not JSON, using fallback extraction for {company_name}")
                            return self._fallback_extraction(content)
                    else:
                        error_text = await response.text()
                        logger.error(f"Sonar API error {response.status}: {error_text}")
                        raise Exception(f"Sonar API error {response.status}: {error_text}")
                        
        except Exception as e:
            logger.error(f"Sonar extraction failed for {company_name}: {e}")
            raise
    
    def _fallback_extraction(self, content: str) -> Dict[str, Any]:
        """Fallback extraction when Sonar response is not JSON."""
        import re
        
        return {
            "address_lines": re.findall(r'〒\s*\d{3}-?\d{4}\s*.*?都道府県.*?', content),
            "employee_mentions": re.findall(r'従業員数\s*[:：]?\s*[\d,，\.]+\s*名?|Employees?\s*[:：]?\s*[\d,\.]+', content),
            "service_heads": re.findall(r'サービス[：:]\s*(.*?)(?:\n|$)', content),
            "product_heads": re.findall(r'製品[：:]\s*(.*?)(?:\n|$)', content),
            "news_heads": re.findall(r'ニュース[：:]\s*(.*?)(?:\n|$)', content)
        }
    
    async def search_and_extract(self, company_info: Dict[str, Any]) -> Dict[str, Any]:
        """Combined search and extraction for Phase A and B."""
        try:
            # Phase A: Search for candidate URLs
            domain = company_info.get('website', '').replace('https://', '').replace('http://', '').split('/')[0]
            search_query = f"site:{domain} (会社概要 OR 会社情報 OR 事業内容 OR サービス OR 製品 OR プロダクト OR 特定商取引 OR 採用 OR news OR press OR ir OR 会社案内 OR corporate OR about OR business OR services OR products) 企業名: {company_info.get('name', '')} Pref: {company_info.get('prefecture', 'unknown')}"
            
            search_results = await self.search(search_query, max_results=10)
            
            # Extract URLs from search results
            urls = []
            for result in search_results.get('results', []):
                urls.append(result.get('url', ''))
            
            if not urls:
                logger.warning(f"No URLs found for {company_info.get('name', '')}")
                return {"urls": [], "extracted_data": {}}
            
            # Phase B: Extract information from URLs
            extracted_data = await self.extract(urls, company_info.get('name', ''))
            
            return {
                "urls": urls,
                "extracted_data": extracted_data
            }
            
        except Exception as e:
            logger.error(f"Search and extract failed for {company_info.get('name', '')}: {e}")
            raise

    async def search_address(self, company_info: Dict[str, Any]) -> Dict[str, Any]:
        """Search specifically for company address information."""
        try:
            company_name = company_info.get('name', '')
            industry = company_info.get('industry', '')
            website = company_info.get('website', '')
            
            # Create specific address search queries
            address_queries = [
                f"{company_name} 本社 住所 所在地",
                f"{company_name} {industry} 本社 住所",
                f"{company_name} 会社概要 住所",
                f"{company_name} 会社情報 所在地"
            ]
            
            # Add website-specific search if available
            if website:
                domain = website.replace('https://', '').replace('http://', '').split('/')[0]
                address_queries.append(f"site:{domain} {company_name} 住所 本社")
            
            all_results = []
            
            # Search with multiple queries
            for query in address_queries[:3]:  # Limit to top 3 queries
                try:
                    logger.info(f"Address search query: {query}")
                    search_results = await self.search(query, max_results=5)
                    if search_results and search_results.get('results'):
                        all_results.extend(search_results['results'])
                except Exception as e:
                    logger.warning(f"Address search failed for query '{query}': {e}")
                    continue
            
            # Remove duplicates based on URL
            seen_urls = set()
            unique_results = []
            for result in all_results:
                if result['url'] not in seen_urls:
                    seen_urls.add(result['url'])
                    unique_results.append(result)
            
            if not unique_results:
                logger.warning(f"No address search results found for {company_name}")
                return {
                    "company_name": company_name,
                    "address_info": {},
                    "search_results": [],
                    "status": "no_results"
                }
            
            # Extract address information from top URLs
            urls = [result['url'] for result in unique_results[:3]]  # Top 3 URLs
            logger.info(f"Extracting address from {len(urls)} URLs")
            
            address_data = await self.extract(urls, company_name)
            
            return {
                "company_name": company_name,
                "address_info": address_data,
                "search_results": unique_results,
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in search_address: {e}")
            return {
                "company_name": company_info.get('name', ''),
                "address_info": {},
                "search_results": [],
                "status": "error",
                "error": str(e)
            }