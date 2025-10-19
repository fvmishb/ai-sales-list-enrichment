"""Cloud Tasks handler for company processing."""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..pipeline.phase_a import phase_a_search
from ..pipeline.phase_b import phase_b_extract
from ..pipeline.phase_c import PhaseC
from ..services.bigquery import BigQueryClient
from ..services.perplexity import PerplexityClient
from ..utils.rate_limiter import global_rate_limiter
from ..utils.extractors import extract_apex_domain

logger = logging.getLogger(__name__)


class TaskHandler:
    """Handler for Cloud Tasks company processing."""
    
    def __init__(self):
        self.phase_c = PhaseC()
        self.bigquery = BigQueryClient()
        self.rate_limiter = global_rate_limiter
        self.pplx_client = PerplexityClient()
        
        # Processing statistics
        self.stats = {
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "by_status": {}
        }
    
    async def process_company(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single company through the enrichment pipeline."""
        # Handle both dict and TaskMessage objects
        if hasattr(company, 'website'):
            website = company.website
            company_data = {
                "website": company.website,
                "name": getattr(company, 'name', ''),
                "industry": getattr(company, 'industry', ''),
                "prefecture": getattr(company, 'prefecture', '')
            }
        else:
            website = company.get("website", "unknown")
            company_data = company
            
        logger.info(f"Starting processing for company: {website}")
        
        try:
            # Extract domain for rate limiting
            domain = extract_apex_domain(website)
            if not domain:
                logger.error(f"Invalid domain for website: {website}")
                return await self._handle_error(company_data, "invalid_domain")
            
            # Apply rate limiting
            await self.rate_limiter.acquire_both(domain)
            
            # Check if already processed (idempotency)
            existing_status = await self.bigquery.get_company_status(website)
            if existing_status == 'ok':
                logger.info(f"Company {company_data.get('name')} already processed successfully. Skipping.")
                self._update_stats("skipped_already_ok")
                return {"status": "skipped_already_ok", "website": website}
            
            # Phase A: Candidate URL Exploration
            phase_a_result = await phase_a_search(company_data, self.pplx_client)
            
            # Phase B: Element Extraction
            phase_b_result = await phase_b_extract(company_data, phase_a_result, self.pplx_client)
            
            # Phase C: Normalization, Summarization, Synthesis
            phase_c_output = await self.phase_c.format_and_synthesize(
                company_data, phase_a_result, phase_b_result
            )
            final_record = phase_c_output.get("enriched_data", {})
            final_record['status'] = phase_c_output.get("status", "ok")
            
            # Store result in BigQuery
            success = await self.bigquery.upsert_company(final_record)
            
            if success:
                logger.info(f"Successfully processed company: {company_data.get('name')}")
                self._update_stats("ok")
                return {"status": "ok", "website": website}
            else:
                self._update_stats("failed")
                logger.error(f"Failed to save company to BigQuery: {website}")
                return {"status": "failed", "website": website, "error": "bigquery_save_failed"}
                
        except Exception as e:
            logger.error(f"Error processing company {website}: {e}")
            self._update_stats("failed")
            return await self._handle_error(company_data, str(e))
    
    async def _handle_error(self, company_data: Dict[str, Any], error_message: str) -> Dict[str, Any]:
        """Handles errors during company processing."""
        website = company_data.get("website", "unknown")
        logger.error(f"Handling error for {website}: {error_message}")
        error_record = {
            "website": website,
            "name": company_data.get("name"),
            "industry": company_data.get("industry"),
            "status": "failed",
            "signals": {"error": error_message},
            "last_crawled_at": datetime.utcnow().isoformat() + "Z"
        }
        await self.bigquery.upsert_company(error_record)
        return {"status": "failed", "website": website, "error": error_message}
    
    def _update_stats(self, status: str):
        self.stats["total_processed"] += 1
        if status == "ok":
            self.stats["successful"] += 1
        else:
            self.stats["failed"] += 1
        self.stats["by_status"][status] = self.stats.get(status, 0) + 1


# Global instance
task_handler = TaskHandler()