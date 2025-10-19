import asyncio
import logging
from typing import List, Dict, Any, Optional
import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
import json

from ..config import settings

logger = logging.getLogger(__name__)

class EnhancedScraper:
    """Enhanced web scraper with news and press release support."""
    
    def __init__(self):
        self.timeout = ClientTimeout(total=settings.scraper_timeout)
        self.max_content_length = settings.scraper_max_content_length
        self.session = None
        
        # News sources configuration
        self.news_sources = {
            "prtimes": {
                "search_url": "https://prtimes.jp/main/html/search.php",
                "query_param": "q",
                "domain_filter": "prtimes.jp"
            },
            "google_news": {
                "search_url": "https://news.google.com/search",
                "query_param": "q",
                "domain_filter": "news.google.com"
            },
            "press_release": {
                "search_url": "https://www.google.com/search",
                "query_param": "q",
                "domain_filter": "prtimes.jp OR news.google.com OR pressrelease"
            }
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_company_comprehensive_info(
        self, 
        company_name: str, 
        website: str, 
        industry: str
    ) -> Dict[str, Any]:
        """Get comprehensive company information from multiple sources."""
        
        if not self.session:
            raise RuntimeError("EnhancedScraper must be used as async context manager")
        
        results = {
            "company_name": company_name,
            "website": website,
            "industry": industry,
            "sources": {
                "official_site": {},
                "news_articles": [],
                "press_releases": [],
                "social_media": []
            },
            "extracted_data": {},
            "last_updated": datetime.now().isoformat()
        }
        
        # 1. 公式サイトから基本情報を取得
        logger.info(f"Scraping official site for {company_name}")
        official_data = await self._scrape_official_site(website, company_name)
        results["sources"]["official_site"] = official_data
        
        # 2. ニュース記事を検索・取得
        logger.info(f"Searching news articles for {company_name}")
        news_articles = await self._search_news_articles(company_name, industry)
        results["sources"]["news_articles"] = news_articles
        
        # 3. プレスリリースを検索・取得
        logger.info(f"Searching press releases for {company_name}")
        press_releases = await self._search_press_releases(company_name, industry)
        results["sources"]["press_releases"] = press_releases
        
        # 4. 全情報を統合して抽出データを生成
        logger.info(f"Extracting comprehensive data for {company_name}")
        extracted_data = await self._extract_comprehensive_data(results)
        results["extracted_data"] = extracted_data
        
        return results
    
    async def _scrape_official_site(self, website: str, company_name: str) -> Dict[str, Any]:
        """Scrape official company website for basic information."""
        try:
            # 主要ページを特定
            target_pages = await self._identify_target_pages(website, company_name)
            
            all_content = []
            for page_url, page_type in target_pages:
                try:
                    content = await self._fetch_page_content(page_url)
                    if content:
                        all_content.append({
                            "url": page_url,
                            "type": page_type,
                            "content": content
                        })
                except Exception as e:
                    logger.warning(f"Failed to fetch {page_url}: {e}")
                    continue
            
            return {
                "pages": all_content,
                "total_pages": len(all_content),
                "success": len(all_content) > 0
            }
            
        except Exception as e:
            logger.error(f"Error scraping official site for {company_name}: {e}")
            return {"pages": [], "total_pages": 0, "success": False, "error": str(e)}
    
    async def _identify_target_pages(self, website: str, company_name: str) -> List[tuple]:
        """Identify target pages to scrape from company website."""
        target_pages = []
        
        # まずホームページのみを取得
        target_pages.append((website, "home"))
        
        # 基本的なページの候補（存在確認後に追加）
        base_pages = [
            ("/about", "about"),
            ("/company", "company"),
            ("/corporate", "corporate"),
            ("/overview", "overview")
        ]
        
        for path, page_type in base_pages:
            full_url = urljoin(website, path)
            target_pages.append((full_url, page_type))
        
        return target_pages
    
    async def _search_news_articles(self, company_name: str, industry: str) -> List[Dict[str, Any]]:
        """Search for news articles about the company."""
        news_articles = []
        
        # Google News検索
        try:
            google_news = await self._search_google_news(company_name, industry)
            news_articles.extend(google_news)
        except Exception as e:
            logger.warning(f"Google News search failed: {e}")
        
        # PRtimes検索
        try:
            prtimes_news = await self._search_prtimes(company_name, industry)
            news_articles.extend(prtimes_news)
        except Exception as e:
            logger.warning(f"PRtimes search failed: {e}")
        
        return news_articles
    
    async def _search_google_news(self, company_name: str, industry: str) -> List[Dict[str, Any]]:
        """Search Google News for company-related articles."""
        try:
            # Google News検索クエリ
            query = f'"{company_name}" {industry}'
            search_url = f"https://news.google.com/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
            
            async with self.session.get(search_url) as response:
                if response.status == 200:
                    html = await response.text()
                    return self._parse_google_news(html, company_name)
                else:
                    logger.warning(f"Google News request failed: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Google News search error: {e}")
            return []
    
    async def _search_prtimes(self, company_name: str, industry: str) -> List[Dict[str, Any]]:
        """Search PRtimes for company press releases."""
        try:
            query = f'"{company_name}" {industry}'
            search_url = f"https://prtimes.jp/main/html/search.php?q={query}"
            
            async with self.session.get(search_url) as response:
                if response.status == 200:
                    html = await response.text()
                    return self._parse_prtimes(html, company_name)
                else:
                    logger.warning(f"PRtimes request failed: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"PRtimes search error: {e}")
            return []
    
    async def _search_press_releases(self, company_name: str, industry: str) -> List[Dict[str, Any]]:
        """Search for press releases and announcements."""
        press_releases = []
        
        # プレスリリース検索クエリ
        queries = [
            f'"{company_name}" プレスリリース',
            f'"{company_name}" 発表',
            f'"{company_name}" ニュース',
            f'"{company_name}" {industry} プレスリリース'
        ]
        
        for query in queries[:2]:  # 最初の2つのクエリのみ
            try:
                # Google検索でプレスリリースを検索
                search_url = f"https://www.google.com/search?q={query}&tbm=nws&hl=ja&gl=JP"
                async with self.session.get(search_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        releases = self._parse_press_releases(html, company_name)
                        press_releases.extend(releases)
            except Exception as e:
                logger.warning(f"Press release search failed for query '{query}': {e}")
                continue
        
        return press_releases
    
    def _parse_google_news(self, html: str, company_name: str) -> List[Dict[str, Any]]:
        """Parse Google News search results."""
        soup = BeautifulSoup(html, 'lxml')
        articles = []
        
        # Google Newsの記事要素を検索
        article_elements = soup.find_all(['article', 'div'], class_=re.compile(r'Jt|Ww|W|X'))
        
        for element in article_elements[:5]:  # 最大5記事
            try:
                title_elem = element.find(['h3', 'h4', 'a'])
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                link_elem = element.find('a', href=True)
                url = link_elem['href'] if link_elem else ""
                
                # 相対URLを絶対URLに変換
                if url.startswith('/'):
                    url = f"https://news.google.com{url}"
                
                # 日付を抽出
                date_elem = element.find(['time', 'span'], class_=re.compile(r'date|time'))
                date_text = date_elem.get_text(strip=True) if date_elem else ""
                
                articles.append({
                    "title": title,
                    "url": url,
                    "date": date_text,
                    "source": "Google News",
                    "relevance_score": self._calculate_relevance_score(title, company_name)
                })
            except Exception as e:
                logger.warning(f"Error parsing Google News article: {e}")
                continue
        
        return articles
    
    def _parse_prtimes(self, html: str, company_name: str) -> List[Dict[str, Any]]:
        """Parse PRtimes search results."""
        soup = BeautifulSoup(html, 'lxml')
        articles = []
        
        # PRtimesの記事要素を検索
        article_elements = soup.find_all(['div', 'article'], class_=re.compile(r'list|item|article'))
        
        for element in article_elements[:5]:  # 最大5記事
            try:
                title_elem = element.find(['h3', 'h4', 'a'])
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                link_elem = element.find('a', href=True)
                url = link_elem['href'] if link_elem else ""
                
                # 相対URLを絶対URLに変換
                if not url.startswith('http'):
                    url = f"https://prtimes.jp{url}"
                
                # 日付を抽出
                date_elem = element.find(['time', 'span'], class_=re.compile(r'date|time'))
                date_text = date_elem.get_text(strip=True) if date_elem else ""
                
                articles.append({
                    "title": title,
                    "url": url,
                    "date": date_text,
                    "source": "PRtimes",
                    "relevance_score": self._calculate_relevance_score(title, company_name)
                })
            except Exception as e:
                logger.warning(f"Error parsing PRtimes article: {e}")
                continue
        
        return articles
    
    def _parse_press_releases(self, html: str, company_name: str) -> List[Dict[str, Any]]:
        """Parse press release search results."""
        soup = BeautifulSoup(html, 'lxml')
        releases = []
        
        # Google検索結果の記事要素を検索
        result_elements = soup.find_all(['div'], class_=re.compile(r'g|result'))
        
        for element in result_elements[:5]:  # 最大5件
            try:
                title_elem = element.find(['h3', 'a'])
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                link_elem = element.find('a', href=True)
                url = link_elem['href'] if link_elem else ""
                
                # スニペットを抽出
                snippet_elem = element.find(['span', 'div'], class_=re.compile(r'snippet|description'))
                snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                
                releases.append({
                    "title": title,
                    "url": url,
                    "snippet": snippet,
                    "source": "Press Release Search",
                    "relevance_score": self._calculate_relevance_score(title, company_name)
                })
            except Exception as e:
                logger.warning(f"Error parsing press release: {e}")
                continue
        
        return releases
    
    async def _fetch_page_content(self, url: str) -> Optional[str]:
        """Fetch and clean page content."""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # 不要な要素を削除
                    for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                        element.decompose()
                    
                    # メインコンテンツを抽出
                    main_content = soup.find('main') or soup.find('article') or soup.find('body')
                    if main_content:
                        text = main_content.get_text(separator=' ', strip=True)
                        return text[:self.max_content_length]
                    else:
                        return soup.get_text(separator=' ', strip=True)[:self.max_content_length]
                else:
                    logger.warning(f"Failed to fetch {url}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _calculate_relevance_score(self, text: str, company_name: str) -> float:
        """Calculate relevance score for news articles."""
        score = 0.0
        text_lower = text.lower()
        company_lower = company_name.lower()
        
        # 企業名の完全一致
        if company_lower in text_lower:
            score += 0.5
        
        # 企業名の部分一致
        company_words = company_lower.split()
        for word in company_words:
            if len(word) > 2 and word in text_lower:
                score += 0.1
        
        # 業界関連キーワード
        industry_keywords = ['事業', 'サービス', '製品', '技術', '開発', '提供', '解決', '課題']
        for keyword in industry_keywords:
            if keyword in text_lower:
                score += 0.05
        
        return min(score, 1.0)
    
    async def _extract_comprehensive_data(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comprehensive data from all sources."""
        # この部分は後でGemini Clientと統合
        return {
            "basic_info": {},
            "news_insights": [],
            "pain_hypotheses": [],
            "recent_developments": []
        }
