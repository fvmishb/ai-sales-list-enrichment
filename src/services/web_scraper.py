"""Web scraper for extracting content from company websites."""

import asyncio
import logging
from typing import Dict, Any, Optional
import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import re

from ..config import settings

logger = logging.getLogger(__name__)


class WebScraper:
    """Lightweight web scraper for extracting company information."""
    
    def __init__(self):
        self.timeout = ClientTimeout(total=settings.scraper_timeout)
        self.max_content_length = settings.scraper_max_content_length
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
    
    async def fetch_page_content(
        self, 
        url: str, 
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """Fetch and parse HTML content from URL."""
        try:
            timeout = timeout or settings.scraper_timeout
            client_timeout = ClientTimeout(total=timeout)
            
            async with aiohttp.ClientSession(
                timeout=client_timeout,
                headers=self.headers
            ) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        
                        # Check content length
                        if len(content) > self.max_content_length:
                            content = content[:self.max_content_length]
                            logger.warning(f"Content truncated for {url}: {len(content)} chars")
                        
                        # Parse HTML
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        # Extract main content
                        main_content = self._extract_main_content(soup)
                        
                        return {
                            "url": url,
                            "title": self._extract_title(soup),
                            "content": main_content,
                            "status": "success",
                            "content_length": len(main_content)
                        }
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        return {
                            "url": url,
                            "title": "",
                            "content": "",
                            "status": f"http_error_{response.status}",
                            "content_length": 0
                        }
                        
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching {url}")
            return {
                "url": url,
                "title": "",
                "content": "",
                "status": "timeout",
                "content_length": 0
            }
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return {
                "url": url,
                "title": "",
                "content": "",
                "status": "error",
                "content_length": 0
            }
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title."""
        title_tag = soup.find("title")
        if title_tag:
            return title_tag.get_text().strip()
        return ""
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main content from HTML, excluding navigation and ads."""
        # Remove unwanted elements
        for element in soup(["script", "style", "nav", "header", "footer", "aside", "advertisement"]):
            element.decompose()
        
        # Try to find main content area
        main_content = None
        
        # Look for common main content selectors
        main_selectors = [
            "main",
            ".main-content",
            ".content",
            ".main",
            "#main",
            "#content",
            ".container",
            ".wrapper"
        ]
        
        for selector in main_selectors:
            main_content = soup.select_one(selector)
            if main_content:
                break
        
        # If no main content found, use body
        if not main_content:
            main_content = soup.find("body")
        
        if main_content:
            # Extract text content
            text_content = main_content.get_text(separator=" ", strip=True)
            
            # Clean up text
            text_content = re.sub(r'\s+', ' ', text_content)  # Normalize whitespace
            text_content = re.sub(r'\n+', '\n', text_content)  # Normalize newlines
            
            return text_content.strip()
        
        return ""
    
    async def fetch_multiple_pages(
        self, 
        urls: list, 
        max_concurrent: int = 5
    ) -> list:
        """Fetch multiple pages concurrently."""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(url):
            async with semaphore:
                return await self.fetch_page_content(url)
        
        tasks = [fetch_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching {urls[i]}: {result}")
                processed_results.append({
                    "url": urls[i],
                    "title": "",
                    "content": "",
                    "status": "error",
                    "content_length": 0
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    def extract_company_specific_info(self, content: str, company_name: str) -> Dict[str, Any]:
        """Extract company-specific information from content."""
        info = {
            "address_lines": [],
            "employee_count": None,
            "services": [],
            "products": [],
            "founded_year": None,
            "capital": None
        }
        
        # Extract address information
        info["address_lines"] = self._extract_address_lines(content)
        
        # Extract employee count
        info["employee_count"] = self._extract_employee_count(content)
        
        # Extract services
        info["services"] = self._extract_services(content)
        
        # Extract products
        info["products"] = self._extract_products(content)
        
        # Extract founded year
        info["founded_year"] = self._extract_founded_year(content)
        
        # Extract capital
        info["capital"] = self._extract_capital(content)
        
        return info
    
    def _extract_address_lines(self, content: str) -> list:
        """Extract address lines from content."""
        address_lines = []
        
        # Postal code pattern
        postal_pattern = r'〒\s*\d{3}-?\d{4}'
        postal_matches = re.findall(postal_pattern, content)
        address_lines.extend(postal_matches)
        
        # Address pattern (prefecture + city + address)
        address_pattern = r'([都道府県][^。\n\r]{0,50})'
        address_matches = re.findall(address_pattern, content)
        address_lines.extend(address_matches)
        
        # Remove duplicates and clean
        address_lines = list(set(address_lines))
        address_lines = [line.strip() for line in address_lines if line.strip()]
        
        return address_lines[:5]  # Limit to 5 lines
    
    def _extract_employee_count(self, content: str) -> Optional[int]:
        """Extract employee count from content."""
        # Employee count patterns
        patterns = [
            r'従業員数[：:]\s*(\d+(?:,\d+)*)',
            r'社員数[：:]\s*(\d+(?:,\d+)*)',
            r'(\d+(?:,\d+)*)\s*名の従業員',
            r'(\d+(?:,\d+)*)\s*人の社員',
            r'従業員\s*(\d+(?:,\d+)*)',
            r'社員\s*(\d+(?:,\d+)*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            if matches:
                try:
                    # Remove commas and convert to int
                    count_str = matches[0].replace(',', '')
                    return int(count_str)
                except ValueError:
                    continue
        
        return None
    
    def _extract_services(self, content: str) -> list:
        """Extract services from content."""
        services = []
        
        # Service-related keywords
        service_keywords = [
            "サービス", "事業", "業務", "提供", "開発", "販売", "運営",
            "コンサルティング", "支援", "代行", "管理", "設計", "施工"
        ]
        
        # Look for bullet points or list items
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if any(keyword in line for keyword in service_keywords):
                # Clean up the line
                line = re.sub(r'^[・\-\*\d+\.\)]\s*', '', line)  # Remove bullet points
                line = re.sub(r'\s+', ' ', line)  # Normalize whitespace
                if len(line) > 10 and len(line) < 100:  # Reasonable length
                    services.append(line)
        
        return services[:10]  # Limit to 10 services
    
    def _extract_products(self, content: str) -> list:
        """Extract products from content."""
        products = []
        
        # Product-related keywords
        product_keywords = [
            "製品", "商品", "ソフトウェア", "システム", "アプリ", "プラットフォーム",
            "ツール", "ソリューション", "パッケージ", "ライセンス"
        ]
        
        # Look for bullet points or list items
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if any(keyword in line for keyword in product_keywords):
                # Clean up the line
                line = re.sub(r'^[・\-\*\d+\.\)]\s*', '', line)  # Remove bullet points
                line = re.sub(r'\s+', ' ', line)  # Normalize whitespace
                if len(line) > 10 and len(line) < 100:  # Reasonable length
                    products.append(line)
        
        return products[:10]  # Limit to 10 products
    
    def _extract_founded_year(self, content: str) -> Optional[int]:
        """Extract founded year from content."""
        patterns = [
            r'設立[：:]\s*(\d{4})年',
            r'創業[：:]\s*(\d{4})年',
            r'(\d{4})年設立',
            r'(\d{4})年創業',
            r'設立\s*(\d{4})',
            r'創業\s*(\d{4})'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            if matches:
                try:
                    year = int(matches[0])
                    if 1800 <= year <= 2024:  # Reasonable year range
                        return year
                except ValueError:
                    continue
        
        return None
    
    def _extract_capital(self, content: str) -> Optional[str]:
        """Extract capital information from content."""
        patterns = [
            r'資本金[：:]\s*([^。\n\r]{0,50})',
            r'資本[：:]\s*([^。\n\r]{0,50})',
            r'資本金\s*([^。\n\r]{0,50})',
            r'資本\s*([^。\n\r]{0,50})'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            if matches:
                capital = matches[0].strip()
                if len(capital) > 0 and len(capital) < 50:
                    return capital
        
        return None

