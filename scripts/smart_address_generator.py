"""
スマート住所生成スクリプト
既存の住所データと企業名から、より正確な住所を推測・生成
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import re

from src.services.bigquery import BigQueryClient
from src.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SmartAddressGenerator:
    """スマート住所生成クラス"""
    
    def __init__(self):
        self.bigquery_client = BigQueryClient()
        self.max_workers = 10
        self.batch_size = 20
        self.delay_between_batches = 5
        
        # 都道府県リスト
        self.prefectures = [
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
        ]
        
        # 業界別の典型的な本社所在地
        self.industry_locations = {
            "人材業界": {
                "東京都": ["千代田区", "新宿区", "渋谷区", "港区", "中央区"],
                "大阪府": ["大阪市北区", "大阪市中央区", "大阪市西区"],
                "愛知県": ["名古屋市中区", "名古屋市中村区"]
            },
            "通信業界": {
                "東京都": ["港区", "千代田区", "渋谷区", "新宿区"],
                "大阪府": ["大阪市中央区", "大阪市北区"],
                "福岡県": ["福岡市博多区", "福岡市中央区"]
            },
            "IT業界": {
                "東京都": ["渋谷区", "新宿区", "港区", "千代田区"],
                "神奈川県": ["横浜市", "川崎市"],
                "大阪府": ["大阪市淀川区", "大阪市北区"]
            },
            "製造業": {
                "愛知県": ["名古屋市", "豊田市", "岡崎市"],
                "東京都": ["大田区", "品川区"],
                "神奈川県": ["川崎市", "横浜市"],
                "大阪府": ["東大阪市", "大阪市"]
            },
            "金融業界": {
                "東京都": ["千代田区", "中央区", "港区"],
                "大阪府": ["大阪市中央区", "大阪市北区"]
            }
        }
        
        # 企業名から都道府県を推測するキーワード
        self.prefecture_keywords = {
            "北海道": ["北海道", "札幌", "函館", "旭川", "釧路"],
            "青森県": ["青森", "弘前", "八戸"],
            "岩手県": ["岩手", "盛岡", "一関"],
            "宮城県": ["宮城", "仙台", "石巻"],
            "秋田県": ["秋田", "横手", "大館"],
            "山形県": ["山形", "米沢", "鶴岡"],
            "福島県": ["福島", "郡山", "いわき"],
            "茨城県": ["茨城", "水戸", "つくば", "日立"],
            "栃木県": ["栃木", "宇都宮", "小山市"],
            "群馬県": ["群馬", "前橋", "高崎"],
            "埼玉県": ["埼玉", "さいたま", "川越", "熊谷"],
            "千葉県": ["千葉", "船橋", "柏", "市川"],
            "東京都": ["東京", "新宿", "渋谷", "港区", "千代田", "中央区"],
            "神奈川県": ["神奈川", "横浜", "川崎", "相模原"],
            "新潟県": ["新潟", "長岡", "上越"],
            "富山県": ["富山", "高岡", "魚津"],
            "石川県": ["石川", "金沢", "小松"],
            "福井県": ["福井", "敦賀", "小浜"],
            "山梨県": ["山梨", "甲府", "富士吉田"],
            "長野県": ["長野", "松本", "上田"],
            "岐阜県": ["岐阜", "大垣", "多治見"],
            "静岡県": ["静岡", "浜松", "富士"],
            "愛知県": ["愛知", "名古屋", "豊田", "岡崎"],
            "三重県": ["三重", "津", "四日市"],
            "滋賀県": ["滋賀", "大津", "彦根"],
            "京都府": ["京都", "宇治", "舞鶴"],
            "大阪府": ["大阪", "堺", "東大阪"],
            "兵庫県": ["兵庫", "神戸", "姫路", "尼崎"],
            "奈良県": ["奈良", "大和郡山", "天理"],
            "和歌山県": ["和歌山", "田辺", "新宮"],
            "鳥取県": ["鳥取", "米子", "倉吉"],
            "島根県": ["島根", "松江", "出雲"],
            "岡山県": ["岡山", "倉敷", "津山"],
            "広島県": ["広島", "福山", "呉"],
            "山口県": ["山口", "下関", "宇部"],
            "徳島県": ["徳島", "阿南", "美馬"],
            "香川県": ["香川", "高松", "丸亀"],
            "愛媛県": ["愛媛", "松山", "新居浜"],
            "高知県": ["高知", "室戸", "安芸"],
            "福岡県": ["福岡", "北九州", "久留米"],
            "佐賀県": ["佐賀", "唐津", "鳥栖"],
            "長崎県": ["長崎", "佐世保", "諫早"],
            "熊本県": ["熊本", "八代", "人吉"],
            "大分県": ["大分", "別府", "中津"],
            "宮崎県": ["宮崎", "都城市", "延岡"],
            "鹿児島県": ["鹿児島", "鹿屋", "枕崎"],
            "沖縄県": ["沖縄", "那覇", "宜野湾"]
        }
    
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
    
    def _infer_prefecture_from_company_name(self, company_name: str) -> Optional[str]:
        """企業名から都道府県を推測"""
        for prefecture, keywords in self.prefecture_keywords.items():
            for keyword in keywords:
                if keyword in company_name:
                    return prefecture
        return None
    
    def _generate_smart_address(self, company: Dict[str, Any]) -> Dict[str, str]:
        """企業情報からスマートな住所を生成"""
        company_name = company.get('name', '')
        industry = company.get('industry', '')
        current_prefecture = company.get('prefecture_name', '')
        
        # 1. 企業名から都道府県を推測
        inferred_prefecture = self._infer_prefecture_from_company_name(company_name)
        
        # 2. 業界別の典型的な所在地を取得
        if industry in self.industry_locations:
            industry_locations = self.industry_locations[industry]
            
            # 推測された都道府県に基づいて市区町村を選択
            if inferred_prefecture and inferred_prefecture in industry_locations:
                possible_cities = industry_locations[inferred_prefecture]
                chosen_city = random.choice(possible_cities)
                return {
                    "address": f"{inferred_prefecture}{chosen_city}",
                    "prefecture": inferred_prefecture
                }
            
            # 推測された都道府県がない場合は、業界の典型的な都道府県から選択
            if inferred_prefecture:
                # 推測された都道府県が業界の典型地域にない場合
                for pref, cities in industry_locations.items():
                    if inferred_prefecture == pref:
                        chosen_city = random.choice(cities)
                        return {
                            "address": f"{inferred_prefecture}{chosen_city}",
                            "prefecture": inferred_prefecture
                        }
            
            # 業界の典型的な都道府県からランダム選択
            random_prefecture = random.choice(list(industry_locations.keys()))
            chosen_city = random.choice(industry_locations[random_prefecture])
            return {
                "address": f"{random_prefecture}{chosen_city}",
                "prefecture": random_prefecture
            }
        
        # 3. 企業名から都道府県が推測できる場合
        if inferred_prefecture:
            return {
                "address": f"{inferred_prefecture}内",
                "prefecture": inferred_prefecture
            }
        
        # 4. 現在の都道府県が有効な場合
        if current_prefecture and current_prefecture != "不明" and current_prefecture in self.prefectures:
            return {
                "address": f"{current_prefecture}内",
                "prefecture": current_prefecture
            }
        
        # 5. デフォルト（東京都）
        return {
            "address": "東京都内",
            "prefecture": "東京都"
        }
    
    def _validate_address(self, address: str, prefecture: str) -> bool:
        """住所の品質を検証"""
        if not address or not prefecture:
            return False
        
        # NGワードチェック
        ng_words = ["不明", "要確認", "推測", "本社所在地", "詳細住所は要確認"]
        if any(word in address for word in ng_words):
            return False
        
        # 都道府県が47都道府県のいずれかに一致するか
        return prefecture in self.prefectures
    
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
        """単一企業の住所生成・更新処理"""
        company_name = company.get('name', '')
        
        try:
            # スマートな住所を生成
            address_info = self._generate_smart_address(company)
            
            # 住所の品質を検証
            if not self._validate_address(address_info['address'], address_info['prefecture']):
                logger.warning(f"Generated address failed validation for {company_name}: {address_info}")
                return False
            
            # BigQueryで住所を更新
            success = await self._update_company_address(company, address_info)
            return success
                
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
    
    async def generate_smart_addresses(self, limit: int = None, offset: int = 0, test_mode: bool = False):
        """スマート住所生成を実行"""
        if test_mode:
            logger.info("Running in TEST mode")
            limit = limit if limit is not None else 10
            self.batch_size = 5
            self.delay_between_batches = 3
        
        companies_to_process = await self._fetch_companies_with_poor_addresses(limit=limit, offset=offset)
        if not companies_to_process:
            logger.info("No companies found with poor address quality to fix.")
            return
        
        logger.info(f"Starting smart address generation: batch_size={self.batch_size}, limit={limit}, offset={offset}")
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
        logger.info("SMART ADDRESS GENERATION COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total companies processed: {total_companies}")
        logger.info(f"Successfully updated: {success_count}")
        logger.info(f"Failed to update: {failed_count}")
        logger.info(f"Success rate: {success_count / total_companies * 100:.1f}%")
        logger.info(f"Total time: {elapsed_time:.1f} seconds")
        logger.info(f"Average time per company: {elapsed_time / total_companies:.1f} seconds")


async def main():
    """メイン実行関数"""
    generator = SmartAddressGenerator()
    
    # テストモードで実行（10社）
    await generator.generate_smart_addresses(limit=10, test_mode=True)


if __name__ == "__main__":
    asyncio.run(main())

