"""Google Custom Search API client for enterprise data enrichment."""

import asyncio
import logging
from typing import Dict, List, Any, Optional
import aiohttp
from aiohttp import ClientTimeout

from ..config import settings, get_secret

logger = logging.getLogger(__name__)


class GoogleSearchClient:
    """Google Custom Search API client for finding company pages."""
    
    def __init__(self):
        self.api_key = self._get_api_key()
        self.cse_id = self._get_cse_id()
        self.search_url = "https://www.googleapis.com/customsearch/v1"
        self.timeout = ClientTimeout(total=30)
        
    def _get_api_key(self) -> str:
        """Get Google Search API key from Secret Manager or environment."""
        if settings.google_search_api_key:
            return settings.google_search_api_key
        
        # Try to get from Secret Manager
        try:
            return get_secret("google-search-api-key")
        except Exception as e:
            logger.warning(f"Could not get Google Search API key: {e}")
            return ""
    
    def _get_cse_id(self) -> str:
        """Get Custom Search Engine ID from Secret Manager or environment."""
        if settings.google_cse_id:
            return settings.google_cse_id
        
        # Try to get from Secret Manager
        try:
            cse_id = get_secret("google-cse-id")
            if cse_id and cse_id != "YOUR_GOOGLE_CSE_ID":
                return cse_id
        except Exception as e:
            logger.warning(f"Could not get Google CSE ID: {e}")
        
        # Use default CSE ID for general web search
        return "017576662512468239146:omuauf_lfve"
    
    async def search_company_site(
        self, 
        domain: str, 
        company_name: str, 
        max_results: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for company pages using general web search and filter by domain."""
        try:
            if not self.api_key or not self.cse_id:
                logger.error("Google Search API key or CSE ID not configured")
                return []
            
            # Build search query for company site
            search_query = self._build_search_query(domain, company_name)
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                params = {
                    "key": self.api_key,
                    "cx": self.cse_id,
                    "q": search_query,
                    "num": min(max_results * 3, 30)  # Get more results to filter
                }
                
                async with session.get(self.search_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get("items", [])
                        
                        # Filter results by domain
                        filtered_results = self._filter_results_by_domain(results, domain)
                        
                        logger.info(f"Google Search found {len(filtered_results)} results for {domain} (from {len(results)} total)")
                        return self._process_search_results(filtered_results[:max_results])
                    else:
                        error_text = await response.text()
                        logger.error(f"Google Search API error {response.status}: {error_text}")
                        return []
                        
        except Exception as e:
            logger.error(f"Google Search request failed: {e}")
            return []
    
    def _build_search_query(self, domain: str, company_name: str) -> str:
        """Build optimized search query for company site."""
        # Use English keywords to avoid encoding issues
        query_parts = [
            "(about OR company OR corporate OR business OR services OR products OR",
            "news OR press OR ir OR career OR recruitment OR contact OR",
            "overview OR information OR profile)",
            f'"{company_name}"'
        ]
        
        return " ".join(query_parts)
    
    def _filter_results_by_domain(self, results: List[Dict[str, Any]], domain: str) -> List[Dict[str, Any]]:
        """Filter search results to only include URLs from the specified domain."""
        filtered_results = []
        clean_domain = domain.replace("https://", "").replace("http://", "").split("/")[0]
        
        for result in results:
            url = result.get("link", "")
            if clean_domain in url:
                filtered_results.append(result)
        
        return filtered_results
    
    def _process_search_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process and categorize search results."""
        processed_results = []
        
        for item in results:
            url = item.get("link", "")
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            
            # Categorize the page type
            page_type = self._categorize_page(url, title, snippet)
            
            processed_results.append({
                "url": url,
                "title": title,
                "snippet": snippet,
                "page_type": page_type,
                "relevance_score": self._calculate_relevance_score(url, title, snippet)
            })
        
        # Sort by relevance score
        processed_results.sort(key=lambda x: x["relevance_score"], reverse=True)
        return processed_results
    
    def _categorize_page(self, url: str, title: str, snippet: str) -> str:
        """Categorize page type based on URL, title, and snippet."""
        url_lower = url.lower()
        title_lower = title.lower()
        snippet_lower = snippet.lower()
        
        # Company overview pages
        if any(keyword in url_lower for keyword in ["about", "company", "corporate", "会社概要", "会社情報"]):
            return "about"
        
        # Business/Services pages
        if any(keyword in url_lower for keyword in ["business", "services", "事業", "サービス", "製品"]):
            return "business"
        
        # News/Press pages
        if any(keyword in url_lower for keyword in ["news", "press", "ir", "ニュース", "プレス"]):
            return "news"
        
        # Legal pages
        if any(keyword in url_lower for keyword in ["legal", "privacy", "terms", "特定商取引", "法的事項"]):
            return "legal"
        
        # Recruitment pages
        if any(keyword in url_lower for keyword in ["career", "recruit", "採用", "求人"]):
            return "recruitment"
        
        return "other"
    
    def _calculate_relevance_score(self, url: str, title: str, snippet: str) -> float:
        """Calculate relevance score for search result."""
        score = 0.0
        
        # URL-based scoring
        url_lower = url.lower()
        if "about" in url_lower or "company" in url_lower:
            score += 0.3
        if "corporate" in url_lower:
            score += 0.2
        if "会社概要" in url_lower or "会社情報" in url_lower:
            score += 0.4
        
        # Title-based scoring
        title_lower = title.lower()
        if "会社概要" in title_lower or "会社情報" in title_lower:
            score += 0.3
        if "事業内容" in title_lower or "サービス" in title_lower:
            score += 0.2
        if "企業情報" in title_lower:
            score += 0.2
        
        # Snippet-based scoring
        snippet_lower = snippet.lower()
        if "本社" in snippet_lower or "住所" in snippet_lower:
            score += 0.2
        if "従業員" in snippet_lower or "社員" in snippet_lower:
            score += 0.1
        if "設立" in snippet_lower or "創業" in snippet_lower:
            score += 0.1
        
        return min(score, 1.0)  # Cap at 1.0
    
    async def search_address_specific(
        self, 
        company_name: str, 
        industry: str = ""
    ) -> List[Dict[str, Any]]:
        """Search specifically for company address information."""
        try:
            if not self.api_key or not self.cse_id:
                logger.error("Google Search API key or CSE ID not configured")
                return []
            
            # Build address-specific search query
            address_query = f'"{company_name}" (address OR location OR headquarters OR office OR contact)'
            if industry:
                address_query += f" {industry}"
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                params = {
                    "key": self.api_key,
                    "cx": self.cse_id,
                    "q": address_query,
                    "num": 5,
                    "siteSearch": "",  # Search entire web
                    "siteSearchFilter": "e"  # Exclude no sites
                }
                
                async with session.get(self.search_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get("items", [])
                        logger.info(f"Address search found {len(results)} results for {company_name}")
                        return self._process_address_results(results)
                    else:
                        error_text = await response.text()
                        logger.error(f"Address search API error {response.status}: {error_text}")
                        return []
                        
        except Exception as e:
            logger.error(f"Address search request failed: {e}")
            return []
    
    def _process_address_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process address search results."""
        processed_results = []
        
        for item in results:
            url = item.get("link", "")
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            
            processed_results.append({
                "url": url,
                "title": title,
                "snippet": snippet,
                "address_relevance": self._calculate_address_relevance(snippet)
            })
        
        # Sort by address relevance
        processed_results.sort(key=lambda x: x["address_relevance"], reverse=True)
        return processed_results
    
    def _calculate_address_relevance(self, snippet: str) -> float:
        """Calculate address relevance score."""
        score = 0.0
        snippet_lower = snippet.lower()
        
        # Address indicators
        if "〒" in snippet or "郵便番号" in snippet_lower:
            score += 0.3
        if any(pref in snippet for pref in ["都", "道", "府", "県"]):
            score += 0.4
        if any(city in snippet_lower for city in ["区", "市", "町", "村"]):
            score += 0.2
        if "番地" in snippet or "丁目" in snippet:
            score += 0.1
        
        return min(score, 1.0)
