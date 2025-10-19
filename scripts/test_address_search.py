#!/usr/bin/env python3
"""
住所検索機能のテストスクリプト
"""

import asyncio
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.perplexity import PerplexityClient
from src.utils.extractors import extract_address_from_text

async def test_address_search():
    """住所検索のテスト"""
    
    # テスト用の企業データ
    test_companies = [
        {
            "name": "株式会社CMU Holdings",
            "industry": "人材業界",
            "website": "https://example.com"
        },
        {
            "name": "ジークス株式会社（ZEAX, Inc.）",
            "industry": "人材業界", 
            "website": "https://www.zeax.co.jp/"
        }
    ]
    
    perplexity_client = PerplexityClient()
    
    for company in test_companies:
        print(f"\n=== {company['name']} の住所検索テスト ===")
        
        try:
            # 住所検索を実行
            address_result = await perplexity_client.search_address(company)
            
            print(f"検索結果: {address_result.get('status', 'unknown')}")
            print(f"検索URL数: {len(address_result.get('search_results', []))}")
            
            if address_result.get('address_info'):
                address_info = address_result['address_info']
                print(f"住所情報: {address_info}")
                
                # 住所テキストから住所を抽出
                if 'address_lines' in address_info:
                    address_text = ' '.join(address_info.get('address_lines', []))
                    print(f"住所テキスト: {address_text}")
                    
                    extracted_address = extract_address_from_text(address_text, company['name'])
                    print(f"抽出された住所: {extracted_address}")
            
        except Exception as e:
            print(f"エラー: {e}")

if __name__ == "__main__":
    asyncio.run(test_address_search())
