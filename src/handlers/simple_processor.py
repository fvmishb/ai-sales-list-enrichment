import asyncio
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import aiohttp
from bs4 import BeautifulSoup
import json

from ..services.simple_gemini_client import SimpleGeminiClient
from ..services.openai_client import OpenAIClient
from ..services.bigquery import BigQueryClient
from ..services.google_custom_search_client import GoogleCustomSearchClient
from ..services.perplexity import PerplexityClient
from ..config import settings

logger = logging.getLogger(__name__)

class SimpleProcessor:
    """シンプルで安定した企業データ処理"""
    
    def __init__(self):
        self.google_search_client = GoogleCustomSearchClient()
        self.gemini_client = SimpleGeminiClient()
        self.openai_client = OpenAIClient()
        self.bigquery_client = BigQueryClient()
        self.perplexity_client = PerplexityClient()
        
    async def process_companies_simple(self, companies: List[Dict[str, Any]], 
                                     max_workers: int = 10) -> Dict[str, Any]:
        """企業データをシンプルに処理"""
        logger.info(f"Starting simple processing of {len(companies)} companies with {max_workers} workers")
        
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
                        logger.info(f"Successfully processed: {company.get('name', 'Unknown')}")
                    else:
                        results["errors"] += 1
                        results["errors_detail"].append(f"Failed: {company.get('name', 'Unknown')}")
                        logger.warning(f"Failed to process: {company.get('name', 'Unknown')}")
                except Exception as e:
                    results["errors"] += 1
                    results["errors_detail"].append(f"Error: {company.get('name', 'Unknown')} - {str(e)}")
                    logger.error(f"Error processing {company.get('name', 'Unknown')}: {e}")
                
                # 進捗表示
                if results["processed"] % 10 == 0 or results["processed"] == results["total"]:
                    elapsed = time.time() - start_time
                    rate = results["processed"] / elapsed * 60 if elapsed > 0 else 0
                    eta_minutes = (results["total"] - results["processed"]) / rate if rate > 0 else 0
                    
                    logger.info(f"Progress: {results['processed']}/{results['total']} "
                              f"({results['processed']/results['total']*100:.1f}%) "
                              f"Rate: {rate:.1f}社/分 ETA: {eta_minutes:.1f}分")
        
        elapsed = time.time() - start_time
        results["elapsed_time"] = elapsed
        results["rate"] = results["processed"] / elapsed * 60 if elapsed > 0 else 0
        
        logger.info(f"Simple processing completed: {results['success']}/{results['total']} "
                   f"successful in {elapsed:.1f}s ({results['rate']:.1f}社/分)")
        
        return results
    
    def _process_single_company(self, company: Dict[str, Any]) -> bool:
        """単一企業の処理（シンプル版）"""
        try:
            import asyncio
            
            # 新しいイベントループで実行
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # 非同期処理を実行
                result = loop.run_until_complete(self._process_single_company_async(company))
                return result
            finally:
                loop.close()
            
        except Exception as e:
            logger.error(f"Error processing company {company.get('name', 'unknown')}: {e}")
            return False
    
    async def _process_single_company_async(self, company: Dict[str, Any]) -> bool:
        """単一企業の非同期処理（Perplexity Sonar統合版）"""
        try:
            company_name = company.get('name', '')
            website = company.get('website', '')
            industry = company.get('industry', '')
            
            logger.info(f"Processing company: {company_name}")
            
            try:
                logger.info(f"Attempting Perplexity Sonar API for {company_name}")
                sonar_result = await self.perplexity_client.search_company_structured(
                    company_name, website, industry
                )
                
                if sonar_result.get('status') == 'success':
                    sonar_data = sonar_result.get('data', {})
                    
                    enriched_data = {
                        'website': website,
                        'name': sonar_data.get('company_name', company_name),
                        'industry': industry,
                        'hq_address_raw': sonar_data.get('address'),
                        'prefecture_name': sonar_data.get('prefecture'),
                        'overview_text': sonar_data.get('company_overview'),
                        'employee_count': sonar_data.get('employees'),
                        'pain_hypotheses': [sonar_data.get('issues_hypothesis')] if sonar_data.get('issues_hypothesis') else [],
                        'sources': sonar_data.get('sources', []),
                        'services_text': [],
                        'products_text': [],
                        'personalization_notes': '',
                        'status': 'ok',
                        'signals': json.dumps({'source': 'perplexity_sonar', 'model': 'sonar-pro'})
                    }
                    
                    success = await self.bigquery_client.upsert_company(enriched_data)
                    
                    if success:
                        logger.info(f"Successfully processed {company_name} with Sonar API")
                        return True
                    else:
                        logger.error(f"Failed to save {company_name} to BigQuery")
                        return False
                else:
                    logger.warning(f"Sonar API failed for {company_name}, falling back to Google Custom Search")
                    
            except Exception as sonar_error:
                logger.warning(f"Sonar API error for {company_name}: {sonar_error}, falling back to Google Custom Search")
            
            logger.info(f"Using fallback: Google Custom Search for {company_name}")
            custom_search_data = await self.google_search_client.search_company_info(
                company_name, website
            )
            
            # Step 2: 公式サイトをスクレイピング（補完情報として）
            html_content = ""
            if website:
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.scraper_timeout)) as session:
                        async with session.get(website) as response:
                            response.raise_for_status()
                            html_content = await response.text()
                except Exception as e:
                    logger.warning(f"Failed to fetch {website}: {e}")
            
            # Step 3: Geminiで追加情報を抽出（スクレイピング結果から）
            gemini_data = {}
            if html_content:
                gemini_data = await self.gemini_client.extract_company_info(
                    html_content, company_name, industry
                )
            
            # Step 4: Custom SearchとGeminiの結果をマージ
            merged_data = self._merge_extraction_results(custom_search_data, gemini_data)
            
            # Step 5: フォールバック: 情報が不足している場合はnullのまま
            if not merged_data.get('hq_address_raw'):
                merged_data['hq_address_raw'] = None
                merged_data['prefecture_name'] = None
            
            # Step 6: GPT-5-miniで最終整形
            enriched_data = await self.openai_client.format_and_synthesize(
                company, {"extracted_data": merged_data}
            )
            
            if not enriched_data:
                logger.warning(f"OpenAI formatting failed for {company_name}")
                return False
            
            # Step 7: BigQueryに保存
            success = await self.bigquery_client.upsert_company(enriched_data)
            
            if success:
                logger.info(f"Successfully processed {company_name} with fallback method")
                return True
            else:
                logger.error(f"Failed to save {company_name} to BigQuery")
                return False
                
        except Exception as e:
            logger.error(f"Error processing {company.get('name', 'Unknown')}: {e}")
            return False
    
    def _merge_extraction_results(self, custom_search_data: Dict[str, Any], gemini_data: Dict[str, Any]) -> Dict[str, Any]:
        """Merge results from Custom Search and Gemini"""
        merged = {}
        
        # Custom Searchの結果を優先
        for key in ['hq_address_raw', 'prefecture_name', 'employee_count', 'overview_text']:
            merged[key] = custom_search_data.get(key) or gemini_data.get(key) or None
        
        # リスト型のフィールドはマージ
        for key in ['services_text', 'products_text']:
            merged[key] = list(set(
                (custom_search_data.get(key) or []) + 
                (gemini_data.get(key) or [])
            ))[:7]  # 最大7件
        
        return merged
    
    async def _fetch_website_content(self, website: str) -> str:
        """ウェブサイトからHTMLコンテンツを取得"""
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(website) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # BeautifulSoupでHTMLをパースしてテキストを抽出
                        soup = BeautifulSoup(html, 'lxml')
                        
                        # 不要な要素を削除
                        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                            element.decompose()
                        
                        # メインコンテンツを抽出
                        main_content = soup.find('main') or soup.find('article') or soup.find('body')
                        if main_content:
                            text = main_content.get_text(separator=' ', strip=True)
                            return text[:50000]  # 50KB制限
                        else:
                            return soup.get_text(separator=' ', strip=True)[:50000]
                    else:
                        logger.warning(f"Failed to fetch {website}: {response.status}")
                        return ""
        except Exception as e:
            logger.error(f"Error fetching {website}: {e}")
            return ""
