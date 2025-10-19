"""OpenAI API client for data formatting and synthesis."""

import asyncio
import json
import logging
from typing import Dict, Any, Optional, List, List
from openai import OpenAI

from ..config import settings

logger = logging.getLogger(__name__)


class OpenAIClient:
    """OpenAI API client for GPT-5 formatting."""
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = "gpt-5-mini"  # Using GPT-5-mini as requested
        
    async def format_and_synthesize(self, company: Dict[str, Any], extracted: Dict[str, Any]) -> Dict[str, Any]:
        """Format and synthesize company data using GPT-5-mini."""
        try:
            prompt = self._build_formatting_prompt(company, extracted)
            
            # Use new responses API for GPT-5-mini
            response = self.client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text",
                                "text": f"{self._get_system_prompt()}\n\n{prompt}"
                            }
                        ]
                    }
                ]
            )
            
            # Extract content from new API response
            if hasattr(response, 'output_text'):
                content = response.output_text
            elif hasattr(response, 'text'):
                content = response.text
            else:
                content = str(response)
            
            # Handle list response
            if isinstance(content, list):
                content = content[0] if content else ""
            
            # Ensure content is string
            if not isinstance(content, str):
                content = str(content)
            
            result = json.loads(content)
            
            # Post-process and validate
            return self._post_process_result(result, company)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return self._get_fallback_result(company)
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return self._get_fallback_result(company)
    
    def _get_system_prompt(self) -> str:
        """Get system prompt for formatting."""
        return """企業情報抽出の整形エージェント。与えられた箇条書き/短文を日本語で整え、厳格JSONのみを返す。文字数制約厳守。事実は与えられた根拠内に限定。

出力スキーマ（必ずこの形式で返してください）:
{
  "name": "企業名",
  "name_legal": "正式商号",
  "industry": "業界",
  "hq_address_raw": "本社住所（生のまま）",
  "prefecture_name": "都道府県名",
  "overview_text": "企業概要（300-500文字）",
  "services_text": "サービス一覧（・で始まる短文、1-7行）",
  "products_text": "製品一覧（・で始まる短文、0-7行）",
  "pain_hypotheses": ["課題仮説1", "課題仮説2", "課題仮説3"],
  "personalization_notes": "パーソナライゼーション用メモ（1-3行）",
  "employee_count": 数値,
  "employee_count_source_url": "従業員数出典URL"
}

重要ルール:
1. 必ず有効なJSON形式で返してください。他のテキストは含めないでください。
2. overview_textでは「与えられた抽出結果では」「公式情報の確認を推奨します」などの汎用的な表現は絶対に使用しないでください。
3. 情報が限定的な場合は、企業名と業界から推測できる具体的な事業内容を記載してください。
4. 推測に基づく場合は「可能性があります」ではなく、断定的に記載してください。
5. hq_address_rawでは「（要確認）」「本社所在地」などの汎用的な表現は避け、具体的な住所情報があれば記載してください。
6. prefecture_nameは住所から正確に抽出し、不明な場合は「不明」と記載してください。"""
    
    def _build_formatting_prompt(self, company: Dict[str, Any], extracted: Dict[str, Any]) -> str:
        """Build formatting prompt for GPT-5-mini."""
        prompt = f"""以下の企業情報を整形してください：

企業名: {company.get('name', '')}
ウェブサイト: {company.get('website', '')}
業界: {company.get('industry', '')}
都道府県ヒント: {company.get('prefecture', '')}

抽出された情報:
{json.dumps(extracted, ensure_ascii=False, indent=2)}

整形ルール:
- name: 企業名（必須）
- name_legal: 正式商号（推測可能な場合のみ）
- industry: 業界（必須）
- hq_address_raw: 本社住所（抽出された情報から）
- prefecture_name: 都道府県名（47都道府県のいずれかに正規化）
- overview_text: 300-500文字で企業概要をまとめる。以下の要素を含む：
  * 事業内容の詳細（業界に応じた具体的な事業内容）
  * 企業の特徴や強み（技術的特徴、ノウハウ、独自性など）
  * 従業員数や会社規模
  * 本社所在地
  * 設立年や会社の歴史（分かる場合）
  * 主要なサービスや製品
  * 対象顧客や市場でのポジション
- services_text: ・で始まる短文、1-7行
- products_text: ・で始まる短文、0-7行
- pain_hypotheses: 業界×規模×ニュースキーワードから3-5個生成（80-120文字）
- personalization_notes: 1-3行のテンプレートに当てはめる
- employee_count: 数値のみ（文字列は不可）
- employee_count_source_url: 従業員数出典URL

必ず有効なJSON形式で返してください。他のテキストは含めないでください。"""
        
        return prompt
    
    def _post_process_result(self, result: Dict[str, Any], company: Dict[str, Any]) -> Dict[str, Any]:
        """Post-process and validate the result."""
        # Ensure required fields
        result.setdefault("website", company.get("website", ""))
        result.setdefault("name", company.get("name", ""))
        result.setdefault("industry", company.get("industry", ""))
        result.setdefault("status", "ok")
        
        # Validate and clean pain_hypotheses
        if "pain_hypotheses" in result:
            if isinstance(result["pain_hypotheses"], list):
                result["pain_hypotheses"] = [h for h in result["pain_hypotheses"] if h and len(h.strip()) > 0]
            else:
                result["pain_hypotheses"] = []
        
        # Ensure pain_hypotheses has 3-5 items
        if len(result.get("pain_hypotheses", [])) < 3:
            result["pain_hypotheses"] = self._generate_fallback_hypotheses(company)
        
        # Clean text fields
        for field in ["overview_text", "personalization_notes"]:
            if field in result and result[field] and isinstance(result[field], str):
                result[field] = result[field].strip()
        
        # Handle list fields (services_text, products_text)
        for field in ["services_text", "products_text"]:
            if field in result and result[field]:
                if isinstance(result[field], list):
                    # Convert list to string with bullet points
                    result[field] = "\n".join([f"・{item.strip()}" for item in result[field] if isinstance(item, str)])
                elif isinstance(result[field], str):
                    result[field] = result[field].strip()
        
        return result
    
    def _get_fallback_result(self, company: Dict[str, Any], extracted: Dict[str, Any] = None) -> Dict[str, Any]:
        """フォールバック結果を生成"""
        industry = company.get('industry', '')
        company_name = company.get('name', '')
        
        # 業界に基づく基本的な概要を生成
        if industry == "人材業界":
            overview = f"{company_name}は人材業界に属する企業で、採用支援、人材紹介、研修・教育、組織コンサルティングなどのサービスを提供しています。"
        elif industry == "通信業界":
            overview = f"{company_name}は通信業界に属する企業で、通信インフラ、ネットワークサービス、通信機器、ソリューション提供などの事業を展開しています。"
        else:
            overview = f"{company_name}は{industry}に属する企業で、業界特有のサービスやソリューションを提供しています。"
        
        return {
            "name": company_name,
            "name_legal": extracted.get("name_legal", company_name) if extracted else company_name,
            "industry": industry,
            "hq_address_raw": extracted.get("hq_address_raw", "") if extracted else "",
            "prefecture_name": extracted.get("prefecture_name", "不明") if extracted else "不明",
            "overview_text": overview,
            "services_text": extracted.get("services_text", "") if extracted else "",
            "products_text": extracted.get("products_text", "") if extracted else "",
            "pain_hypotheses": extracted.get("pain_hypotheses", []) if extracted else [],
            "personalization_notes": f"{industry}の企業として、業界特有の課題やニーズに対応している可能性があります。",
            "employee_count": extracted.get("employee_count") if extracted else None,
            "employee_count_source_url": extracted.get("employee_count_source_url", "") if extracted else "",
            "website": company.get("website", ""),
            "status": "ok"
        }
