#!/usr/bin/env python3
"""Trigger batch processing for companies."""

import os
import sys
import asyncio
import argparse
import logging
import json
from google.cloud import pubsub_v1

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def trigger_batch_processing(industry: str = None, limit: int = 1000) -> bool:
    """Trigger batch processing for companies."""
    try:
        # Initialize Pub/Sub client
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(settings.gcp_project_id, settings.pubsub_topic_id)
        
        # Build message attributes
        attributes = {
            "industry": industry or "all",
            "limit": str(limit),
            "trigger_type": "batch_processing"
        }
        
        # Build message data
        message_data = {
            "trigger": "batch_processing",
            "industry": industry,
            "limit": limit
        }
        
        # Publish message
        message_bytes = json.dumps(message_data).encode('utf-8')
        future = publisher.publish(topic_path, message_bytes, **attributes)
        message_id = future.result()
        
        logger.info(f"Published batch trigger message: {message_id}")
        logger.info(f"Industry: {industry or 'all'}")
        logger.info(f"Limit: {limit}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error triggering batch processing: {e}")
        return False


async def trigger_industry_batch(industry: str, limit: int = 1000) -> bool:
    """Trigger batch processing for specific industry."""
    logger.info(f"Triggering batch processing for industry: {industry}")
    return await trigger_batch_processing(industry, limit)


async def trigger_all_industries(limit_per_industry: int = 1000) -> bool:
    """Trigger batch processing for all industries."""
    industries = [
        "IT・web",
        "製造業界",
        "小売・卸売業界",
        "金融業界",
        "医療・福祉業界",
        "教育・学習業界",
        "建設・建築",
        "運輸・物流業界",
        "飲食業界",
        "不動産業界",
        "人材業界",
        "コンサルティング業界",
        "広告・制作業界",
        "エネルギー業界",
        "通信業界",
        "自動車・輸送機器業界",
        "機械業界",
        "食品業界",
        "アパレル・美容業界",
        "ゲーム業界",
        "エンタメ・娯楽",
        "レジャー・観光・宿泊",
        "生活関連サービス業",
        "その他サービス業界",
        "その他業界"
    ]
    
    logger.info(f"Triggering batch processing for {len(industries)} industries")
    
    success_count = 0
    for industry in industries:
        if await trigger_industry_batch(industry, limit_per_industry):
            success_count += 1
        else:
            logger.error(f"Failed to trigger batch for industry: {industry}")
    
    logger.info(f"Successfully triggered {success_count}/{len(industries)} industries")
    return success_count == len(industries)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Trigger batch processing for companies')
    parser.add_argument('--industry', help='Specific industry to process')
    parser.add_argument('--limit', type=int, default=1000, help='Number of companies to process')
    parser.add_argument('--all-industries', action='store_true', help='Process all industries')
    parser.add_argument('--limit-per-industry', type=int, default=1000, help='Limit per industry when processing all')
    
    args = parser.parse_args()
    
    if args.all_industries:
        success = asyncio.run(trigger_all_industries(args.limit_per_industry))
    elif args.industry:
        success = asyncio.run(trigger_industry_batch(args.industry, args.limit))
    else:
        success = asyncio.run(trigger_batch_processing(limit=args.limit))
    
    if success:
        logger.info("Batch processing triggered successfully")
        sys.exit(0)
    else:
        logger.error("Failed to trigger batch processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
