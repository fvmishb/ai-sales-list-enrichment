#!/usr/bin/env python3
"""
Google検索ベースの住所抽出スクリプト
会社名でGoogle検索を実行し、検索結果から住所情報を抽出してBigQueryに保存
"""

import asyncio
import logging
import aiohttp
import json
import re
import time
import argparse
from typing import Dict, Any, List, Optional, Tuple
from google.cloud import bigquery
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin, urlparse
import random

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressSearchExtractor:
    """住所検索・抽出クラス"""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "ai-sales-list"
        self.session = None
        self.processed_count = 0
        self.success_count = 0
        self.failed_count = 0
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def _generate_search_queries(self, company_name: str, website: str = "") -> List[str]:
        """検索クエリを生成"""
        queries = [
            f'"{company_name}" 本社 住所',
            f'"{company_name}" 会社概要',
            f'"{company_name}" 企業情報',
            f'"{company_name}" 所在地',
            f'"{company_name}" お問い合わせ',
            f'"{company_name}" アクセス'
        ]
        
        if website:
            domain = urlparse(website).netloc
            if domain:
                queries.extend([
                    f'site:{domain} 会社概要',
                    f'site:{domain} 企業情報',
                    f'site:{domain} お問い合わせ'
                ])
        
        return queries
    
    async def _search_google(self, query: str) -> Optional[BeautifulSoup]:
        """Google検索を実行"""
        try:
            search_url = f"https://www.google.com/search?q={quote(query)}&num=10"
            
            # ランダムな待機時間（API制限対策）
            await asyncio.sleep(random.uniform(2, 4))
            
            async with self.session.get(search_url) as response:
                if response.status == 200:
                    html = await response.text()
                    return BeautifulSoup(html, 'html.parser')
                else:
                    logger.warning(f"Google search failed with status {response.status}")
                    return None
                    
        except Exception as e:
            logger.debug(f"Google search error for query '{query}': {e}")
            return None
    
    def _extract_address_from_search_results(self, soup: BeautifulSoup, company_name: str) -> Optional[Dict[str, str]]:
        """検索結果から住所を抽出"""
        if not soup:
            return None
        
        text_content = soup.get_text()
        
        # 住所パターン
        address_patterns = [
            r'〒\d{3}-\d{4}[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                if len(match) > 15 and (company_name in text_content or len(match) > 20):
                    prefecture = self._extract_prefecture(match)
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        return None
    
    def _extract_search_links(self, soup: BeautifulSoup) -> List[str]:
        """検索結果からリンクを抽出"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('/url?q='):
                href = href.split('/url?q=')[1].split('&')[0]
            if href.startswith('http') and 'google.com' not in href and 'youtube.com' not in href:
                links.append(href)
        return links[:5]  # 上位5件
    
    async def _scrape_company_page(self, url: str, company_name: str) -> Optional[Dict[str, str]]:
        """企業ページから住所を抽出"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 住所関連のセクションを探す
                    address_sections = [
                        '会社概要', '企業情報', '会社案内', '会社データ', 
                        'お問い合わせ', 'アクセス', '所在地', '本社',
                        '会社情報', '企業概要', '会社プロフィール'
                    ]
                    
                    for section in address_sections:
                        elements = soup.find_all(text=re.compile(section, re.IGNORECASE))
                        for element in elements:
                            parent = element.parent
                            if parent:
                                text_content = parent.get_text()
                                address_info = self._extract_address_from_text(text_content, company_name)
                                if address_info:
                                    return address_info
                    
                    # 全体のテキストから住所を探す
                    full_text = soup.get_text()
                    address_info = self._extract_address_from_text(full_text, company_name)
                    if address_info:
                        return address_info
                        
        except Exception as e:
            logger.debug(f"Failed to scrape {url}: {e}")
            
        return None
    
    def _extract_address_from_text(self, text: str, company_name: str) -> Optional[Dict[str, str]]:
        """テキストから住所を抽出"""
        # 住所パターン
        address_patterns = [
            r'[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if len(match) > 15:  # 十分な長さの住所
                    prefecture = self._extract_prefecture(match)
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        return None
    
    def _extract_prefecture(self, address: str) -> str:
        """住所から都道府県を抽出"""
        prefectures = [
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
        ]
        
        for prefecture in prefectures:
            if prefecture in address:
                return prefecture
        
        return "不明"
    
    def _validate_address(self, address: str, prefecture: str) -> bool:
        """住所の品質を検証"""
        if not address or not prefecture:
            return False
        
        # NGワードチェック
        ng_words = ["不明", "要確認", "推測", "本社所在地", "詳細住所は要確認"]
        if any(word in address for word in ng_words):
            return False
        
        # 最小文字数チェック
        if len(address) < 15:
            return False
        
        # 都道府県が47都道府県のいずれかに一致するか
        valid_prefectures = [
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
        ]
        
        return prefecture in valid_prefectures
    
    def _clean_address(self, address: str) -> str:
        """住所を整形"""
        # 改行、余分な空白を削除
        address = re.sub(r'\s+', ' ', address.strip())
        
        # 電話番号、FAXなどの不要情報を除去
        address = re.sub(r'TEL[：:]\d+[-\d]*', '', address)
        address = re.sub(r'FAX[：:]\d+[-\d]*', '', address)
        address = re.sub(r'電話[：:]\d+[-\d]*', '', address)
        address = re.sub(r'フリーコール[：:]\d+[-\d]*', '', address)
        
        return address.strip()
    
    async def search_company_address(self, company_name: str, website: str = "") -> Optional[Dict[str, str]]:
        """企業の住所を検索・抽出"""
        logger.info(f"Searching address for: {company_name}")
        
        # 検索クエリを生成
        queries = self._generate_search_queries(company_name, website)
        
        # 各クエリで検索を試行
        for query in queries:
            try:
                # Google検索を実行
                soup = await self._search_google(query)
                if not soup:
                    continue
                
                # 検索結果から住所を抽出
                address_info = self._extract_address_from_search_results(soup, company_name)
                if address_info and self._validate_address(address_info['address'], address_info['prefecture']):
                    address_info['address'] = self._clean_address(address_info['address'])
                    logger.info(f"Found address via search: {address_info}")
                    return address_info
                
                # 検索結果のリンクをスクレイピング
                links = self._extract_search_links(soup)
                for link in links:
                    try:
                        address_info = await self._scrape_company_page(link, company_name)
                        if address_info and self._validate_address(address_info['address'], address_info['prefecture']):
                            address_info['address'] = self._clean_address(address_info['address'])
                            logger.info(f"Found address via scraping: {address_info}")
                            return address_info
                    except Exception as e:
                        logger.debug(f"Failed to scrape {link}: {e}")
                        continue
                        
            except Exception as e:
                logger.debug(f"Search query failed: {query} - {e}")
                continue
        
        logger.warning(f"No address found for: {company_name}")
        return None
    
    async def process_companies_batch(self, companies: List[Dict[str, Any]]) -> Dict[str, int]:
        """企業のバッチを処理"""
        batch_results = {"success": 0, "failed": 0}
        
        # 並列処理（最大5社同時）
        semaphore = asyncio.Semaphore(5)
        
        async def process_single_company(company_data: Dict[str, Any]) -> bool:
            async with semaphore:
                try:
                    company_name = company_data.get('name', '')
                    website = company_data.get('website', '')
                    
                    # 住所を検索・抽出
                    address_info = await self.search_company_address(company_name, website)
                    
                    if address_info:
                        # BigQueryに更新
                        await self._update_company_address(company_name, address_info)
                        batch_results["success"] += 1
                        self.success_count += 1
                        logger.info(f"Successfully updated: {company_name}")
                        return True
                    else:
                        batch_results["failed"] += 1
                        self.failed_count += 1
                        logger.warning(f"Failed to find address for: {company_name}")
                        return False
                        
                except Exception as e:
                    logger.error(f"Error processing {company_data.get('name', 'unknown')}: {e}")
                    batch_results["failed"] += 1
                    self.failed_count += 1
                    return False
        
        # バッチ内で並列実行
        tasks = [process_single_company(company) for company in companies]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        return batch_results
    
    async def _update_company_address(self, company_name: str, address_info: Dict[str, str]):
        """企業の住所情報をBigQueryに更新"""
        update_query = f"""
        UPDATE `{self.project_id}.companies.enriched`
        SET 
            hq_address_raw = @hq_address_raw,
            prefecture_name = @prefecture_name
        WHERE name = @company_name
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hq_address_raw", "STRING", address_info.get("address", "")),
                bigquery.ScalarQueryParameter("prefecture_name", "STRING", address_info.get("prefecture", "")),
                bigquery.ScalarQueryParameter("company_name", "STRING", company_name)
            ]
        )
        
        query_job = self.client.query(update_query, job_config=job_config)
        query_job.result()
    
    def get_companies_to_process(self, limit: int = None, offset: int = 0) -> List[Dict[str, Any]]:
        """処理対象の企業を取得"""
        query = f"""
        SELECT name, industry, website, hq_address_raw, prefecture_name
        FROM `{self.project_id}.companies.enriched`
        WHERE hq_address_raw LIKE '%（要確認）%'
           OR hq_address_raw LIKE '%推測%'
           OR hq_address_raw LIKE '%本社所在地%'
           OR hq_address_raw LIKE '%詳細住所は要確認%'
           OR hq_address_raw LIKE '%不明%'
           OR hq_address_raw IS NULL
           OR hq_address_raw = ''
        ORDER BY name
        """
        
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        query_job = self.client.query(query)
        results = list(query_job.result())
        
        logger.info(f"Found {len(results)} companies to process")
        return [dict(row) for row in results]
    
    async def run_batch_processing(self, batch_size: int = 100, offset: int = 0, limit: int = None):
        """バッチ処理を実行"""
        logger.info(f"Starting batch processing: batch_size={batch_size}, offset={offset}, limit={limit}")
        
        start_time = time.time()
        
        # 処理対象の企業を取得
        companies = self.get_companies_to_process(limit=limit, offset=offset)
        
        if not companies:
            logger.info("No companies to process")
            return
        
        # バッチに分割
        batches = [companies[i:i + batch_size] for i in range(0, len(companies), batch_size)]
        
        logger.info(f"Processing {len(companies)} companies in {len(batches)} batches")
        
        async with self:
            for i, batch in enumerate(batches):
                logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} companies)")
                
                # バッチを処理
                batch_results = await self.process_companies_batch(batch)
                
                self.processed_count += len(batch)
                
                logger.info(f"Batch {i+1} completed: {batch_results['success']} success, {batch_results['failed']} failed")
                
                # バッチ間で待機（DML制限対策）
                if i + 1 < len(batches):
                    logger.info("Waiting 10 seconds before next batch...")
                    await asyncio.sleep(10)
        
        elapsed_time = time.time() - start_time
        
        # 統計情報を出力
        logger.info("=" * 50)
        logger.info("PROCESSING COMPLETED")
        logger.info("=" * 50)
        logger.info(f"Total processed: {self.processed_count}")
        logger.info(f"Success: {self.success_count}")
        logger.info(f"Failed: {self.failed_count}")
        logger.info(f"Success rate: {self.success_count/self.processed_count*100:.1f}%" if self.processed_count > 0 else "N/A")
        logger.info(f"Total time: {elapsed_time:.1f} seconds")
        logger.info(f"Average time per company: {elapsed_time/self.processed_count:.1f} seconds" if self.processed_count > 0 else "N/A")

async def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='Address Search and Extraction')
    parser.add_argument('--test', action='store_true', help='Test mode (10 companies)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    parser.add_argument('--offset', type=int, default=0, help='Offset')
    parser.add_argument('--limit', type=int, help='Limit number of companies')
    
    args = parser.parse_args()
    
    if args.test:
        args.limit = 10
        args.batch_size = 5
        logger.info("Running in TEST mode")
    
    extractor = AddressSearchExtractor()
    await extractor.run_batch_processing(
        batch_size=args.batch_size,
        offset=args.offset,
        limit=args.limit
    )

if __name__ == "__main__":
    asyncio.run(main())

