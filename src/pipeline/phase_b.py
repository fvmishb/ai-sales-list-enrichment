"""Phase B: Element extraction using Perplexity Sonar model."""

import logging
from typing import Dict, Any
from ..services.perplexity import PerplexityClient
from ..utils.rate_limiter import global_rate_limiter

logger = logging.getLogger(__name__)


async def phase_b_extract(company_info: dict, candidate_urls: dict, pplx_client: PerplexityClient) -> dict:
    """Phase B: Extract specific details from selected URLs using Sonar model."""
    try:
        # Collect all URLs
        all_urls = []
        for url_list in candidate_urls.values():
            all_urls.extend(url_list)
        
        if not all_urls:
            logger.warning(f"No URLs found for extraction for {company_info.get('name', '')}")
            return {}
        
        # Limit to top 5 URLs for extraction
        urls_to_extract = list(set(all_urls))[:5]
        
        domain = company_info.get('website', '').replace('https://', '').replace('http://', '').split('/')[0]
        
        # Apply rate limiting
        async with global_rate_limiter.global_limiter:
            async with await global_rate_limiter.get_domain_limiter(domain):
                extracted_data = await pplx_client.extract(urls_to_extract, company_info.get('name', ''))
        
        logger.info(f"Phase B completed for {company_info.get('name', '')}. Extracted data keys: {extracted_data.keys()}")
        return {"status": "success", "extracted_data": extracted_data}
        
    except Exception as e:
        logger.error(f"Phase B failed for {company_info.get('name', '')}: {e}")
        return {}