"""
直接処理モード - Cloud Tasksをバイパスして高速処理
"""

import asyncio
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from ..services.perplexity import PerplexityClient
from ..services.openai_client import OpenAIClient
from ..services.bigquery import BigQueryClient
from ..config import settings

logger = logging.getLogger(__name__)

class DirectProcessor:
    """直接処理モードで企業データを高速処理"""
    
    def __init__(self):
        self.perplexity_client = PerplexityClient()
        self.openai_client = OpenAIClient()
        self.bigquery_client = BigQueryClient()
        
    async def process_companies_direct(self, companies: List[Dict[str, Any]], 
                                     max_workers: int = 20) -> Dict[str, Any]:
        """企業データを直接並列処理"""
        logger.info(f"Starting direct processing of {len(companies)} companies with {max_workers} workers")
        
        start_time = time.time()
        results = {
            "total": len(companies),
            "processed": 0,
            "success": 0,
            "errors": 0,
            "errors_detail": []
        }
        
        # 並列処理で企業データを処理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 各企業の処理タスクを作成
            future_to_company = {
                executor.submit(self._process_single_company, company): company 
                for company in companies
            }
            
            # 完了したタスクを処理
            for future in as_completed(future_to_company):
                company = future_to_company[future]
                results["processed"] += 1
                
                try:
                    success = future.result()
                    if success:
                        results["success"] += 1
                        logger.info(f"Successfully processed: {company.get('name', 'unknown')} ({results['success']}/{results['total']})")
                    else:
                        results["errors"] += 1
                        results["errors_detail"].append(f"Failed: {company.get('name', 'unknown')}")
                        logger.warning(f"Failed to process: {company.get('name', 'unknown')}")
                        
                except Exception as e:
                    results["errors"] += 1
                    results["errors_detail"].append(f"Error processing {company.get('name', 'unknown')}: {str(e)}")
                    logger.error(f"Error processing {company.get('name', 'unknown')}: {e}")
                
                # 進捗表示
                if results["processed"] % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = results["processed"] / elapsed * 60  # 社/分
                    remaining = results["total"] - results["processed"]
                    eta_minutes = remaining / (rate / 60) if rate > 0 else 0
                    
                    logger.info(f"Progress: {results['processed']}/{results['total']} "
                              f"({results['processed']/results['total']*100:.1f}%) "
                              f"Rate: {rate:.1f}社/分 ETA: {eta_minutes:.1f}分")
        
        elapsed = time.time() - start_time
        results["elapsed_time"] = elapsed
        results["rate"] = results["processed"] / elapsed * 60 if elapsed > 0 else 0
        
        logger.info(f"Direct processing completed: {results['success']}/{results['total']} "
                   f"successful in {elapsed:.1f}s ({results['rate']:.1f}社/分)")
        
        return results
    
    def _process_single_company(self, company: Dict[str, Any]) -> bool:
        """単一企業の処理（同期版）"""
        try:
            import asyncio
            from ..utils.extractors import extract_address_from_text
            
            # 新しいイベントループで実行
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Phase A: Perplexityで情報抽出
                extracted_data = loop.run_until_complete(
                    self.perplexity_client.search_and_extract(company)
                )
                
                # Phase A+: 住所情報の追加検索
                address_data = loop.run_until_complete(
                    self.perplexity_client.search_address(company)
                )
                
                # 住所情報を統合
                if address_data.get('address_info'):
                    address_info = address_data['address_info']
                    if 'address_lines' in address_info:
                        # 住所テキストから住所を抽出
                        address_text = ' '.join(address_info.get('address_lines', []))
                        extracted_address = extract_address_from_text(address_text, company.get('name', ''))
                        
                        # 抽出した住所情報を追加
                        extracted_data['extracted_data']['address_info'] = {
                            'address': extracted_address['address'],
                            'prefecture': extracted_address['prefecture']
                        }
                
                # Phase B: OpenAIで整形・統合
                enriched_data = loop.run_until_complete(
                    self.openai_client.format_and_synthesize(
                        company, extracted_data
                    )
                )
                
                # Phase C: BigQueryに保存
                success = loop.run_until_complete(
                    self.bigquery_client.upsert_company(enriched_data)
                )
                
                return success
                
            finally:
                loop.close()
            
        except Exception as e:
            logger.error(f"Error processing company {company.get('name', 'unknown')}: {e}")
            return False
