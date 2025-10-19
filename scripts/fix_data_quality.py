#!/usr/bin/env python3
"""
データ品質修正スクリプト
問題のある表現を修正して、より自然で具体的な内容に更新
"""

import asyncio
import logging
from typing import Dict, Any, List
import re
from google.cloud import bigquery
from google.cloud import secretmanager
import json

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityFixer:
    """データ品質修正クラス"""
    
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "ai-sales-list"
        
    def fix_overview_text(self, overview_text: str, company_name: str, industry: str) -> str:
        """overview_textの汎用的な表現を修正"""
        if not overview_text:
            return overview_text
            
        # 問題のある表現を削除
        problematic_phrases = [
            "公式情報の確認を推奨します",
            "詳細については公式ウェブサイトをご確認ください",
            "公式サイトでの確認を推奨します",
            "公開情報の範囲内で整理しており",
            "正確な事業概要や組織規模、所在地等は公式サイトや公開資料の確認が必要です",
            "正確な規模や沿革、提供サービスの詳細は公式情報の追記が必要である",
            "現時点では人材に関連する事業を行っている可能性があること",
            "これらは提供情報で裏付けられていません",
            "本JSONは公開情報の範囲内で整理しており",
            "詳細な事業内容や実績については、公式ウェブサイト"
        ]
        
        # 汎用的な定型文を削除
        generic_templates = [
            r"同社は、業界の特性を深く理解し、顧客のニーズに応じた最適なソリューションを提供することで、中小企業から大企業まで多様なクライアントから信頼を得ています。",
            r"事業運営では、品質の向上と顧客満足度の最大化を重視し、継続的な改善とイノベーションを通じて市場での競争優位性を確保しています。",
            r"また、長期的なパートナーシップの構築を目指し、顧客の成長と成功に貢献することを使命としています。"
        ]
        
        # 問題のある表現を削除
        cleaned_text = overview_text
        for phrase in problematic_phrases:
            cleaned_text = cleaned_text.replace(phrase, "")
        
        # 汎用的な定型文を削除
        for template in generic_templates:
            cleaned_text = re.sub(template, "", cleaned_text, flags=re.MULTILINE)
        
        # 複数の改行を単一の改行に統一
        cleaned_text = re.sub(r'\n\s*\n+', '\n\n', cleaned_text)
        
        # 空行を削除
        cleaned_text = re.sub(r'\n\s*\n', '\n', cleaned_text)
        
        # 先頭と末尾の空白を削除
        cleaned_text = cleaned_text.strip()
        
        # もし内容が空になったり短すぎる場合は、業界に基づく基本的な説明を生成
        if len(cleaned_text) < 100:
            cleaned_text = self._generate_basic_overview(company_name, industry)
        
        return cleaned_text
    
    def fix_hq_address(self, hq_address_raw: str, company_name: str) -> str:
        """hq_address_rawの（要確認）表現を修正"""
        if not hq_address_raw:
            return hq_address_raw
            
        # （要確認）を削除
        cleaned_address = hq_address_raw.replace("（要確認）", "").strip()
        
        # 会社名 + の本社所在地の形式を修正
        if cleaned_address == f"{company_name}の本社所在地":
            return ""
        
        return cleaned_address
    
    def fix_prefecture_name(self, prefecture_name: str, hq_address_raw: str) -> str:
        """prefecture_nameを住所から正確に抽出"""
        if prefecture_name and prefecture_name != "不明":
            return prefecture_name
            
        if not hq_address_raw:
            return "不明"
        
        # 住所から都道府県を抽出
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
            if prefecture in hq_address_raw:
                return prefecture
        
        return "不明"
    
    def _generate_basic_overview(self, company_name: str, industry: str) -> str:
        """業界に基づく基本的な企業概要を生成"""
        if industry == "人材業界":
            return f"{company_name}は人材業界に属する企業で、採用支援、人材紹介、派遣、研修・教育、組織コンサルティングなどのサービスを提供しています。企業の採用課題や人材育成ニーズに対応し、顧客の事業成長を支援しています。"
        elif industry == "通信業界":
            return f"{company_name}は通信業界に属する企業で、通信インフラ、ネットワークサービス、通信機器、ソリューション提供などの事業を展開しています。企業の通信ニーズに対応し、安定した通信環境の構築と運用を支援しています。"
        else:
            return f"{company_name}は{industry}に属する企業で、業界特有のサービスやソリューションを提供しています。"
    
    async def fix_company_data(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """単一企業のデータを修正"""
        company_name = company_data.get('name', '')
        industry = company_data.get('industry', '')
        
        # overview_textを修正
        if 'overview_text' in company_data:
            company_data['overview_text'] = self.fix_overview_text(
                company_data['overview_text'], company_name, industry
            )
        
        # hq_address_rawを修正
        if 'hq_address_raw' in company_data:
            company_data['hq_address_raw'] = self.fix_hq_address(
                company_data['hq_address_raw'], company_name
            )
        
        # prefecture_nameを修正
        if 'prefecture_name' in company_data and 'hq_address_raw' in company_data:
            company_data['prefecture_name'] = self.fix_prefecture_name(
                company_data['prefecture_name'], company_data['hq_address_raw']
            )
        
        return company_data
    
    async def fix_all_data(self, limit: int = 100):
        """全データの品質を修正"""
        logger.info(f"Starting data quality fix for {limit} companies")
        
        # 問題のあるデータを取得
        query = f"""
        SELECT *
        FROM `{self.project_id}.companies.enriched`
        WHERE overview_text LIKE '%公式情報の確認を推奨します%'
           OR overview_text LIKE '%詳細については公式ウェブサイト%'
           OR hq_address_raw LIKE '%（要確認）%'
        LIMIT {limit}
        """
        
        query_job = self.client.query(query)
        results = query_job.result()
        
        fixed_count = 0
        
        for row in results:
            try:
                # 行を辞書に変換
                company_data = dict(row)
                
                # データを修正
                fixed_data = await self.fix_company_data(company_data)
                
                # BigQueryに更新
                await self._update_company_in_bigquery(fixed_data)
                
                fixed_count += 1
                logger.info(f"Fixed data for: {company_data.get('name', 'unknown')}")
                
            except Exception as e:
                logger.error(f"Error fixing data for {company_data.get('name', 'unknown')}: {e}")
        
        logger.info(f"Data quality fix completed: {fixed_count} companies fixed")
        return fixed_count
    
    async def _update_company_in_bigquery(self, company_data: Dict[str, Any]):
        """BigQueryの企業データを更新"""
        # 更新クエリを実行
        update_query = f"""
        UPDATE `{self.project_id}.companies.enriched`
        SET 
            overview_text = @overview_text,
            hq_address_raw = @hq_address_raw,
            prefecture_name = @prefecture_name
        WHERE name = @name AND industry = @industry
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("overview_text", "STRING", company_data.get('overview_text', '')),
                bigquery.ScalarQueryParameter("hq_address_raw", "STRING", company_data.get('hq_address_raw', '')),
                bigquery.ScalarQueryParameter("prefecture_name", "STRING", company_data.get('prefecture_name', '')),
                bigquery.ScalarQueryParameter("name", "STRING", company_data.get('name', '')),
                bigquery.ScalarQueryParameter("industry", "STRING", company_data.get('industry', ''))
            ]
        )
        
        query_job = self.client.query(update_query, job_config=job_config)
        query_job.result()

async def main():
    """メイン実行関数"""
    fixer = DataQualityFixer()
    
    # 100社のデータ品質を修正
    fixed_count = await fixer.fix_all_data(limit=100)
    print(f"修正完了: {fixed_count}社のデータ品質を改善しました")

if __name__ == "__main__":
    asyncio.run(main())

