"""BigQuery client for data storage and retrieval."""

import asyncio
import logging
from typing import Dict, List, Any, Optional, List
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..config import settings

logger = logging.getLogger(__name__)


class BigQueryClient:
    """BigQuery client for enterprise data storage."""
    
    def __init__(self):
        self.client = bigquery.Client(project=settings.gcp_project_id)
        self.dataset_id = settings.bq_dataset_id
        self.raw_table_id = settings.bq_raw_table_id
        self.enriched_table_id = settings.bq_enriched_table_id
        
    async def upsert_company(self, company_data: Dict[str, Any]) -> bool:
        """Upsert company data to enriched table."""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(self.enriched_table_id)
            
            # Add timestamp
            company_data["last_crawled_at"] = datetime.utcnow().isoformat()
            
            # Prepare row for BigQuery
            row = self._prepare_row_for_bq(company_data)
            logger.info(f"Prepared row for BigQuery: {row}")
            
            # Use MERGE for upsert
            query = f"""
            MERGE `{settings.gcp_project_id}.{self.dataset_id}.{self.enriched_table_id}` T
            USING (SELECT @website as website) S
            ON T.website = S.website
            WHEN MATCHED THEN
              UPDATE SET
                name = @name,
                name_legal = @name_legal,
                industry = @industry,
                hq_address_raw = @hq_address_raw,
                prefecture_name = @prefecture_name,
                overview_text = @overview_text,
                services_text = @services_text,
                products_text = @products_text,
                pain_hypotheses = @pain_hypotheses,
                personalization_notes = @personalization_notes,
                employee_count = @employee_count,
                employee_count_source_url = @employee_count_source_url,
                last_crawled_at = @last_crawled_at,
                status = @status,
                signals = @signals
            WHEN NOT MATCHED THEN
              INSERT (website, name, name_legal, industry, hq_address_raw, prefecture_name,
                     overview_text, services_text, products_text, pain_hypotheses,
                     personalization_notes, employee_count, employee_count_source_url,
                     last_crawled_at, status, signals)
              VALUES (@website, @name, @name_legal, @industry, @hq_address_raw, @prefecture_name,
                     @overview_text, @services_text, @products_text, @pain_hypotheses,
                     @personalization_notes, @employee_count, @employee_count_source_url,
                     @last_crawled_at, @status, @signals)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("website", "STRING", row["website"]),
                    bigquery.ScalarQueryParameter("name", "STRING", row["name"]),
                    bigquery.ScalarQueryParameter("name_legal", "STRING", row["name_legal"]),
                    bigquery.ScalarQueryParameter("industry", "STRING", row["industry"]),
                    bigquery.ScalarQueryParameter("hq_address_raw", "STRING", row["hq_address_raw"]),
                    bigquery.ScalarQueryParameter("prefecture_name", "STRING", row["prefecture_name"]),
                    bigquery.ScalarQueryParameter("overview_text", "STRING", row["overview_text"]),
                    bigquery.ScalarQueryParameter("services_text", "STRING", row["services_text"]),
                    bigquery.ScalarQueryParameter("products_text", "STRING", row["products_text"]),
                    bigquery.ArrayQueryParameter("pain_hypotheses", "STRING", row["pain_hypotheses"]),
                    bigquery.ScalarQueryParameter("personalization_notes", "STRING", row["personalization_notes"]),
                    bigquery.ScalarQueryParameter("employee_count", "INT64", row["employee_count"]),
                    bigquery.ScalarQueryParameter("employee_count_source_url", "STRING", row["employee_count_source_url"]),
                    bigquery.ScalarQueryParameter("last_crawled_at", "TIMESTAMP", row["last_crawled_at"]),
                    bigquery.ScalarQueryParameter("status", "STRING", row["status"]),
                    bigquery.ScalarQueryParameter("signals", "JSON", row["signals"])
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            
            logger.info(f"Successfully upserted company: {row['website']}")
            return True
            
        except Exception as e:
            logger.error(f"Error upserting company {company_data.get('website', 'unknown')}: {e}")
            return False

    async def get_company_status(self, website: str) -> Optional[str]:
        """Get the processing status of a company by its website."""
        try:
            query = f"""
            SELECT status FROM `{settings.gcp_project_id}.{self.dataset_id}.{self.enriched_table_id}`
            WHERE website = @website
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("website", "STRING", website),
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            for row in results:
                return row.status
            return None
        except Exception as e:
            logger.error(f"Error getting company status for {website}: {e}")
            return None
    
    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        try:
            query = f"""
            SELECT 
              industry,
              COUNT(*) as total,
              SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as completed,
              SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) / COUNT(*) * 100 as completion_rate,
              AVG(ARRAY_LENGTH(pain_hypotheses)) as avg_hypotheses,
              SUM(CASE WHEN employee_count IS NULL THEN 1 ELSE 0 END) as missing_employees
            FROM `{settings.gcp_project_id}.{self.dataset_id}.{self.enriched_table_id}`
            GROUP BY industry
            ORDER BY total DESC
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            stats = []
            for row in results:
                stats.append({
                    "industry": row.industry,
                    "total": row.total,
                    "completed": row.completed,
                    "completion_rate": round(row.completion_rate, 2),
                    "avg_hypotheses": round(row.avg_hypotheses, 2),
                    "missing_employees": row.missing_employees
                })
            
            return {"by_industry": stats}
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {"error": str(e)}
    
    async def get_companies_to_process(self, industry: str = None, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get companies that need processing."""
        try:
            where_clause = ""
            if industry:
                where_clause = f"WHERE industry = '{industry}'"
            
            query = f"""
            SELECT website, company_name, industry, prefecture
            FROM `{settings.gcp_project_id}.{self.dataset_id}.{self.raw_table_id}`
            {where_clause}
            LIMIT {limit}
            """
            
            logger.info(f"Executing query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            companies = []
            for row in results:
                companies.append({
                    "website": row.website,
                    "name": row.company_name,
                    "industry": row.industry,
                    "prefecture": row.prefecture,
                    "inquiry_url": ""  # Not available in raw data
                })
            
            logger.info(f"Found {len(companies)} companies for industry: {industry}")
            return companies
            
        except Exception as e:
            logger.error(f"Error getting companies to process: {e}")
            return []
    
    def _prepare_row_for_bq(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare company data for BigQuery insertion."""
        import json
        
        # Handle list fields properly
        services_text = company_data.get("services_text", "")
        if isinstance(services_text, list):
            services_text = "\n".join([f"・{item.strip()}" for item in services_text if isinstance(item, str)])
        
        products_text = company_data.get("products_text", "")
        if isinstance(products_text, list):
            products_text = "\n".join([f"・{item.strip()}" for item in products_text if isinstance(item, str)])
        
        row = {
            "website": company_data.get("website", ""),
            "name": company_data.get("name", ""),
            "name_legal": company_data.get("name_legal", ""),
            "industry": company_data.get("industry", ""),
            "hq_address_raw": company_data.get("hq_address_raw", ""),
            "prefecture_name": company_data.get("prefecture_name", ""),
            "overview_text": company_data.get("overview_text", ""),
            "services_text": services_text,
            "products_text": products_text,
            "pain_hypotheses": company_data.get("pain_hypotheses", []),
            "personalization_notes": company_data.get("personalization_notes", ""),
            "employee_count": company_data.get("employee_count"),
            "employee_count_source_url": company_data.get("employee_count_source_url", ""),
            "last_crawled_at": company_data.get("last_crawled_at", datetime.utcnow().isoformat()),
            "status": company_data.get("status", "ok"),
            "signals": json.dumps(company_data.get("signals", {}))  # JSON文字列に変換
        }
        
        return row
    
    async def run_query(self, query: str, query_params: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a BigQuery query and return results."""
        try:
            job_config = None
            if query_params:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            param["name"], 
                            param["parameterType"]["type"], 
                            param["parameterValue"]["value"]
                        ) for param in query_params
                    ]
                )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Convert results to list of dictionaries
            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    row_dict[key] = value
                rows.append(row_dict)
            
            return rows
            
        except Exception as e:
            logger.error(f"Error running query: {e}")
            return []


# Global client instance
bigquery_client = BigQueryClient()
