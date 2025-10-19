#!/usr/bin/env python3
"""
高速バッチ処理スクリプト - 直接処理モードで業界別に高速処理
"""

import asyncio
import sys
import argparse
import logging
from typing import List, Dict, Any

# プロジェクトルートをパスに追加
sys.path.append('/Users/mishb_/ai-sales-list')

from src.handlers.direct_processor import DirectProcessor
from src.services.bigquery import BigQueryClient

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def process_industry_fast(industry: str, limit: int = None, max_workers: int = 20):
    """業界別高速処理"""
    logger.info(f"Starting fast processing for industry: {industry}")
    
    # BigQueryから企業データを取得
    bq_client = BigQueryClient()
    companies = await bq_client.get_companies_to_process(industry, limit or 10000)
    
    if not companies:
        logger.warning(f"No companies found for industry: {industry}")
        return
    
    logger.info(f"Found {len(companies)} companies to process")
    
    # 直接処理モードで並列処理
    processor = DirectProcessor()
    results = await processor.process_companies_direct(companies, max_workers)
    
    # 結果を表示
    logger.info("=" * 50)
    logger.info(f"Processing completed for {industry}")
    logger.info(f"Total companies: {results['total']}")
    logger.info(f"Successfully processed: {results['success']}")
    logger.info(f"Errors: {results['errors']}")
    logger.info(f"Success rate: {results['success']/results['total']*100:.1f}%")
    logger.info(f"Processing time: {results['elapsed_time']:.1f} seconds")
    logger.info(f"Processing rate: {results['rate']:.1f} companies/minute")
    
    if results['errors_detail']:
        logger.warning(f"Error details: {results['errors_detail'][:5]}...")  # 最初の5件のみ表示

async def main():
    parser = argparse.ArgumentParser(description='Fast batch processing for company data')
    parser.add_argument('--industry', required=True, help='Industry to process')
    parser.add_argument('--limit', type=int, help='Maximum number of companies to process')
    parser.add_argument('--workers', type=int, default=20, help='Number of parallel workers')
    
    args = parser.parse_args()
    
    try:
        await process_industry_fast(args.industry, args.limit, args.workers)
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
