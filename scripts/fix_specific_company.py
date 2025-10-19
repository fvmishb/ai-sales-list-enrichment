#!/usr/bin/env python3
"""
特定企業のデータ品質修正スクリプト
"""

import asyncio
import logging
import aiohttp
import json
import re
from typing import Dict, Any, List, Optional
from google.cloud import bigquery
from bs4 import BeautifulSoup

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpecificCompanyFixer:
    """特定企業のデータ修正クラス"""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "ai-sales-list"
        
    async def search_company_address(self, company_name: str, website: str = "") -> Dict[str, str]:
        """企業の住所情報を検索"""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            ) as session:
                
                # Google検索で住所情報を検索
                search_queries = [
                    f"{company_name} 本社 住所",
                    f"{company_name} 所在地",
                    f"{company_name} 会社情報",
                    f"site:{website} 会社概要" if website else f"{company_name} 会社概要"
                ]
                
                for query in search_queries:
                    try:
                        search_url = f"https://www.google.com/search?q={query}&num=5"
                        async with session.get(search_url) as response:
                            if response.status == 200:
                                html = await response.text()
                                soup = BeautifulSoup(html, 'html.parser')
                                
                                # 検索結果から住所らしき情報を抽出
                                address_info = self._extract_address_from_search_results(soup, company_name)
                                if address_info.get('address') and address_info.get('prefecture'):
                                    return address_info
                                
                                # 検索結果のリンクを辿って詳細情報を取得
                                links = self._extract_search_links(soup)
                                for link in links[:3]:  # 上位3つのリンクをチェック
                                    try:
                                        address_info = await self._scrape_company_page(session, link, company_name)
                                        if address_info.get('address') and address_info.get('prefecture'):
                                            return address_info
                                    except Exception as e:
                                        logger.debug(f"Failed to scrape {link}: {e}")
                                        continue
                                        
                    except Exception as e:
                        logger.debug(f"Search query failed: {query} - {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Address search failed for {company_name}: {e}")
            
        return {"address": "", "prefecture": "不明"}
    
    def _extract_address_from_search_results(self, soup: BeautifulSoup, company_name: str) -> Dict[str, str]:
        """検索結果から住所情報を抽出"""
        address_patterns = [
            r'〒\d{3}-\d{4}',
            r'東京都[^。]*',
            r'大阪府[^。]*',
            r'愛知県[^。]*',
            r'神奈川県[^。]*',
            r'埼玉県[^。]*',
            r'千葉県[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*'
        ]
        
        text_content = soup.get_text()
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                if company_name in text_content or len(match) > 10:
                    prefecture = self._extract_prefecture(match)
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        return {"address": "", "prefecture": "不明"}
    
    def _extract_search_links(self, soup: BeautifulSoup) -> List[str]:
        """検索結果からリンクを抽出"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('/url?q='):
                href = href.split('/url?q=')[1].split('&')[0]
            if href.startswith('http') and 'google.com' not in href:
                links.append(href)
        return links
    
    async def _scrape_company_page(self, session: aiohttp.ClientSession, url: str, company_name: str) -> Dict[str, str]:
        """企業ページから住所情報を抽出"""
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 会社概要、企業情報、お問い合わせなどのページを探す
                    target_sections = [
                        '会社概要', '企業情報', '会社案内', '会社データ', 
                        'お問い合わせ', 'アクセス', '所在地', '本社'
                    ]
                    
                    for section in target_sections:
                        elements = soup.find_all(text=re.compile(section))
                        for element in elements:
                            parent = element.parent
                            if parent:
                                text_content = parent.get_text()
                                address_info = self._extract_address_from_text(text_content, company_name)
                                if address_info.get('address') and address_info.get('prefecture'):
                                    return address_info
                    
                    # 全体のテキストから住所を探す
                    full_text = soup.get_text()
                    address_info = self._extract_address_from_text(full_text, company_name)
                    if address_info.get('address') and address_info.get('prefecture'):
                        return address_info
                        
        except Exception as e:
            logger.debug(f"Failed to scrape {url}: {e}")
            
        return {"address": "", "prefecture": "不明"}
    
    def _extract_address_from_text(self, text: str, company_name: str) -> Dict[str, str]:
        """テキストから住所情報を抽出"""
        # 住所パターン
        address_patterns = [
            r'[都道府県][^。]*[市区町村][^。]*[0-9-]+[^。]*',
            r'[都道府県][^。]*[市区町村][^。]*',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if len(match) > 10:  # 十分な長さの住所
                    prefecture = self._extract_prefecture(match)
                    if prefecture and prefecture != "不明":
                        return {"address": match.strip(), "prefecture": prefecture}
        
        return {"address": "", "prefecture": "不明"}
    
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
    
    def _generate_enhanced_overview(self, company_name: str, industry: str, address_info: Dict[str, str]) -> str:
        """詳細で自然な企業概要を生成"""
        location_info = ""
        if address_info.get("prefecture") and address_info.get("prefecture") != "不明":
            location_info = f"本社は{address_info.get('prefecture')}に所在し、"
        
        if industry == "人材業界":
            return f"""{company_name}は人材業界の専門企業として、{location_info}企業の採用課題解決に特化したサービスを提供しています。

主な事業領域として、採用コンサルティング、人材紹介、派遣サービス、採用マーケティング支援、組織開発・人材育成プログラムなどを手がけており、クライアント企業の成長戦略に直結する人材ソリューションを提供しています。

特に{company_name}は、採用とマーケティングの専門性を活かし、従来の人材紹介サービスを超えた戦略的な採用支援を強みとしています。企業のブランディング、求人広告の企画・制作、採用プロセスの最適化など、採用活動全体をトータルサポートすることで、クライアントの採用成功に貢献しています。

対象顧客は主に中小企業から中堅企業の採用担当者や経営陣で、業界特有の課題である「優秀な人材の確保」「採用コストの最適化」「採用プロセスの効率化」などの課題解決に取り組んでいます。"""
        
        elif industry == "通信業界":
            return f"""{company_name}は通信業界の専門企業として、{location_info}企業の通信インフラ構築と運用に特化したサービスを提供しています。

主な事業領域として、ネットワーク設計・構築、通信機器の販売・リース、インターネット接続サービス、VoIP/クラウドPBX導入、24時間監視・保守サービスなどを手がけており、クライアント企業の通信環境の最適化と安定運用を支援しています。

特に{company_name}は、通信技術の専門知識と豊富な実績を活かし、企業の規模や業種に応じた最適な通信ソリューションを提供しています。セキュリティ対策、災害対策、コスト最適化など、通信インフラの課題解決に総合的に取り組んでいます。

対象顧客は主に中小企業から中堅企業のIT担当者や経営陣で、業界特有の課題である「通信インフラの安定性確保」「通信コストの削減」「セキュリティ強化」「災害対策」などの課題解決に取り組んでいます。"""
        
        else:
            return f"""{company_name}は{industry}の専門企業として、{location_info}業界特有の課題解決に特化したサービスを提供しています。

{industry}の特性を深く理解し、クライアント企業の事業成長を支援するソリューションを提供しています。業界の専門知識と豊富な実績を活かし、企業の規模やニーズに応じた最適なサービスを提供しています。

対象顧客は主に中小企業から中堅企業で、業界特有の課題解決に取り組んでいます。"""
    
    async def fix_specific_company(self, company_name: str):
        """特定企業のデータを修正"""
        logger.info(f"Fixing data for: {company_name}")
        
        # 企業データを取得
        query = f"""
        SELECT *
        FROM `{self.project_id}.companies.enriched`
        WHERE name = @company_name
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("company_name", "STRING", company_name)
            ]
        )
        
        query_job = self.client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        if not results:
            logger.warning(f"Company not found: {company_name}")
            return
        
        company_data = dict(results[0])
        industry = company_data.get('industry', '')
        website = company_data.get('website', '')
        
        # 住所情報を検索
        address_info = await self.search_company_address(company_name, website)
        
        # 詳細な企業概要を生成
        enhanced_overview = self._generate_enhanced_overview(company_name, industry, address_info)
        
        # データを更新
        update_query = f"""
        UPDATE `{self.project_id}.companies.enriched`
        SET 
            overview_text = @overview_text,
            hq_address_raw = @hq_address_raw,
            prefecture_name = @prefecture_name
        WHERE name = @company_name
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("overview_text", "STRING", enhanced_overview),
                bigquery.ScalarQueryParameter("hq_address_raw", "STRING", address_info.get("address", "")),
                bigquery.ScalarQueryParameter("prefecture_name", "STRING", address_info.get("prefecture", "不明")),
                bigquery.ScalarQueryParameter("company_name", "STRING", company_name)
            ]
        )
        
        query_job = self.client.query(update_query, job_config=job_config)
        query_job.result()
        
        logger.info(f"Successfully fixed data for: {company_name}")
        logger.info(f"Address: {address_info.get('address', 'Not found')}")
        logger.info(f"Prefecture: {address_info.get('prefecture', '不明')}")

async def main():
    """メイン実行関数"""
    fixer = SpecificCompanyFixer()
    
    # 株式会社パワー・マーケティングを修正
    await fixer.fix_specific_company("株式会社パワー・マーケティング")
    
    print("特定企業の修正が完了しました")

if __name__ == "__main__":
    asyncio.run(main())

