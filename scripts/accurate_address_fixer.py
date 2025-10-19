"""
正確な住所検索・修正スクリプト
Cloud Run環境でGoogle検索を実行し、実際の住所を取得してBigQueryを更新
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import aiohttp
import json

from src.services.bigquery import BigQueryClient
from src.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AccurateAddressFixer:
    """正確な住所修正クラス"""
    
    def __init__(self):
        self.bigquery_client = BigQueryClient()
        self.max_workers = 3  # 検索API制限を考慮して少なめに
        self.batch_size = 5   # バッチサイズも小さく
        self.delay_between_batches = 15  # バッチ間の待機時間を長めに
        self.cloud_run_url = "https://ai-sales-enrichment-905635292309.asia-northeast1.run.app"
        
    async def _fetch_companies_with_poor_addresses(self, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
        """住所が不正確な企業を取得"""
        query = f"""
            SELECT name, website, industry, hq_address_raw, prefecture_name
            FROM `ai-sales-list.companies.enriched`
            WHERE (
                hq_address_raw LIKE '%（要確認）%' OR
                hq_address_raw LIKE '%推測%' OR
                hq_address_raw LIKE '%本社所在地%' OR
                hq_address_raw LIKE '%詳細住所は要確認%' OR
                hq_address_raw LIKE '%不明%' OR
                hq_address_raw IS NULL OR
                hq_address_raw = '' OR
                prefecture_name = '不明' OR
                prefecture_name IS NULL OR
                prefecture_name = ''
            )
            ORDER BY name
        """
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        logger.info(f"Fetching companies with poor address quality. Query: {query}")
        companies = await self.bigquery_client.run_query(query)
        logger.info(f"Found {len(companies)} companies with poor address quality.")
        return companies
    
    async def _search_address_via_cloud_run(self, company: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Cloud Run API経由で住所を検索"""
        company_name = company.get('name', '')
        website = company.get('website', '')
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
                payload = {
                    "company_name": company_name,
                    "website": website
                }
                
                async with session.post(
                    f"{self.cloud_run_url}/search-address",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("status") == "success":
                            return {
                                "address": result["address"],
                                "prefecture": result["prefecture"]
                            }
                        else:
                            logger.warning(f"No address found for {company_name}: {result.get('message', 'Unknown error')}")
                            return None
                    else:
                        error_text = await response.text()
                        logger.error(f"Cloud Run API error for {company_name}: {response.status} - {error_text}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error calling Cloud Run API for {company_name}: {e}")
            return None
    
    async def _update_company_address(self, company: Dict[str, Any], address_info: Dict[str, str]) -> bool:
        """企業の住所をBigQueryで更新"""
        company_name = company.get('name', '')
        hq_address_raw = address_info['address']
        prefecture_name = address_info['prefecture']
        
        update_query = """
            UPDATE `ai-sales-list.companies.enriched`
            SET hq_address_raw = @hq_address_raw, prefecture_name = @prefecture_name
            WHERE name = @name
        """
        params = [
            {"name": "hq_address_raw", "parameterType": {"type": "STRING"}, "parameterValue": {"value": hq_address_raw}},
            {"name": "prefecture_name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": prefecture_name}},
            {"name": "name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": company_name}},
        ]
        
        try:
            await self.bigquery_client.run_query(update_query, query_params=params)
            logger.info(f"Successfully updated address for {company_name}: {hq_address_raw} ({prefecture_name})")
            return True
        except Exception as e:
            logger.error(f"Error updating BigQuery for {company_name}: {e}")
            return False
    
    async def _process_single_company(self, company: Dict[str, Any]) -> bool:
        """単一企業の住所検索・更新処理"""
        company_name = company.get('name', '')
        
        try:
            # Cloud Run API経由で住所を検索
            address_info = await self._search_address_via_cloud_run(company)
            
            if address_info:
                # BigQueryで住所を更新
                success = await self._update_company_address(company, address_info)
                return success
            else:
                logger.warning(f"Failed to find accurate address for: {company_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing {company_name}: {e}")
            return False
    
    def _process_single_company_sync(self, company: Dict[str, Any]) -> bool:
        """同期版の単一企業処理"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._process_single_company(company))
        finally:
            loop.close()
    
    async def fix_addresses_accurately(self, limit: int = None, offset: int = 0, test_mode: bool = False):
        """正確な住所修正を実行"""
        if test_mode:
            logger.info("Running in TEST mode")
            limit = limit if limit is not None else 5
            self.batch_size = 2
            self.delay_between_batches = 10
        
        companies_to_process = await self._fetch_companies_with_poor_addresses(limit=limit, offset=offset)
        if not companies_to_process:
            logger.info("No companies found with poor address quality to fix.")
            return
        
        logger.info(f"Starting accurate address fixing: batch_size={self.batch_size}, limit={limit}, offset={offset}")
        logger.info(f"Found {len(companies_to_process)} companies to process")
        
        total_companies = len(companies_to_process)
        num_batches = (total_companies + self.batch_size - 1) // self.batch_size
        
        success_count = 0
        failed_count = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in range(num_batches):
                start_index = i * self.batch_size
                end_index = min((i + 1) * self.batch_size, total_companies)
                batch_companies = companies_to_process[start_index:end_index]
                
                logger.info(f"Processing batch {i+1}/{num_batches} ({len(batch_companies)} companies)")
                
                futures = [executor.submit(self._process_single_company_sync, company) for company in batch_companies]
                
                batch_success = 0
                batch_failed = 0
                for future in as_completed(futures):
                    if future.result():
                        success_count += 1
                        batch_success += 1
                    else:
                        failed_count += 1
                        batch_failed += 1
                
                logger.info(f"Batch {i+1} completed: {batch_success} success, {batch_failed} failed")
                
                if i < num_batches - 1:
                    logger.info(f"Waiting {self.delay_between_batches} seconds before next batch...")
                    await asyncio.sleep(self.delay_between_batches)
        
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("ACCURATE ADDRESS FIXING COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total companies processed: {total_companies}")
        logger.info(f"Successfully updated: {success_count}")
        logger.info(f"Failed to update: {failed_count}")
        logger.info(f"Success rate: {success_count / total_companies * 100:.1f}%")
        logger.info(f"Total time: {elapsed_time:.1f} seconds")
        logger.info(f"Average time per company: {elapsed_time / total_companies:.1f} seconds")


async def main():
    """メイン実行関数"""
    fixer = AccurateAddressFixer()
    
    # テストモードで実行（5社）
    await fixer.fix_addresses_accurately(limit=5, test_mode=True)


if __name__ == "__main__":
    asyncio.run(main())

