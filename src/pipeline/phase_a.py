"""Phase A: Candidate URL exploration using Perplexity Search API."""

import logging
from typing import Dict, Any
from ..services.perplexity import PerplexityClient
from ..utils.rate_limiter import global_rate_limiter

logger = logging.getLogger(__name__)


async def phase_a_search(company_info: dict, pplx_client: PerplexityClient) -> dict:
    """Phase A: Search for candidate URLs using Perplexity Search API."""
    try:
        domain = company_info.get('website', '').replace('https://', '').replace('http://', '').split('/')[0]
        
        # Build search query
        search_query = (
            f"site:{domain} "
            f"(会社概要 OR 会社情報 OR 事業内容 OR サービス OR 製品 OR プロダクト OR "
            f"特定商取引 OR 採用 OR news OR press OR ir OR 会社案内 OR corporate OR about OR "
            f"business OR services OR products) "
            f"企業名: {company_info.get('name', '')} "
            f"Pref: {company_info.get('prefecture', 'unknown')}"
        )
        
        # Apply rate limiting
        async with global_rate_limiter.global_limiter:
            async with await global_rate_limiter.get_domain_limiter(domain):
                search_results = await pplx_client.search(search_query, max_results=20)
        
        # Categorize URLs
        urls = {
            "about_pages": [],
            "business_pages": [],
            "product_pages": [],
            "news_pages": [],
            "legal_pages": []
        }
        
        for result in search_results.get('results', []):
            url = result.get('url', '')
            title = result.get('title', '').lower()
            
            # Categorize based on URL and title
            if any(keyword in url.lower() or keyword in title for keyword in ['about', 'company', '会社概要', '会社情報']):
                urls["about_pages"].append(url)
            elif any(keyword in url.lower() or keyword in title for keyword in ['business', 'service', '事業', 'サービス']):
                urls["business_pages"].append(url)
            elif any(keyword in url.lower() or keyword in title for keyword in ['product', '製品', 'プロダクト']):
                urls["product_pages"].append(url)
            elif any(keyword in url.lower() or keyword in title for keyword in ['news', 'press', 'ir', 'ニュース', 'プレス']):
                urls["news_pages"].append(url)
            elif any(keyword in url.lower() or keyword in title for keyword in ['legal', '特定商取引', 'privacy', 'terms']):
                urls["legal_pages"].append(url)
            else:
                # Default to about pages
                urls["about_pages"].append(url)
        
        # Limit to top 5 URLs per category
        for category in urls:
            urls[category] = urls[category][:5]
        
        logger.info(f"Phase A completed for {company_info.get('name', '')}. Found URLs: {sum(len(urls[cat]) for cat in urls)} total")
        return urls
        
    except Exception as e:
        logger.error(f"Phase A failed for {company_info.get('name', '')}: {e}")
        return {
            "about_pages": [],
            "business_pages": [],
            "product_pages": [],
            "news_pages": [],
            "legal_pages": []
        }