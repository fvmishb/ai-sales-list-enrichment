"""Gemini 2.5 Flash API client for lightweight information extraction."""

import asyncio
import json
import logging
from typing import Dict, List, Any, Optional
import aiohttp
from aiohttp import ClientTimeout
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from google.api_core.exceptions import GoogleAPIError

from ..config import settings, get_secret

logger = logging.getLogger(__name__)


class GeminiClient:
    """Gemini 2.5 Flash API client for extracting company information."""
    
    def __init__(self):
        self.api_key = self._get_api_key()
        self.use_vertex_ai = settings.use_vertex_ai
        self.timeout = ClientTimeout(total=60)
        
        if self.use_vertex_ai:
            self.base_url = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{settings.gcp_project_id}/locations/us-central1/publishers/google/models/gemini-2.0-flash-exp:generateContent"
        else:
            self.base_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent"
    
    def _get_api_key(self) -> str:
        """Get Gemini API key from Secret Manager or environment."""
        if settings.gemini_api_key:
            return settings.gemini_api_key
        
        # Try to get from Secret Manager
        try:
            return get_secret("gemini-api-key")
        except Exception as e:
            logger.warning(f"Could not get Gemini API key: {e}")
            return ""
    
    async def extract_comprehensive_data(self, comprehensive_data: Dict[str, Any], company_name: str, industry: str) -> Dict[str, Any]:
        """Extract structured information from comprehensive data (official site + news + press releases)."""
        
        # 公式サイトのコンテンツを結合
        official_content = ""
        for page in comprehensive_data.get("sources", {}).get("official_site", {}).get("pages", []):
            if page.get("content"):
                official_content += f"[{page['type']}] {page['url']}\n{page['content']}\n\n"
        
        # ニュース記事のコンテンツを結合
        news_content = ""
        for article in comprehensive_data.get("sources", {}).get("news_articles", []):
            news_content += f"[NEWS] {article['title']} ({article['source']})\n{article.get('snippet', '')}\n\n"
        
        # プレスリリースのコンテンツを結合
        press_content = ""
        for release in comprehensive_data.get("sources", {}).get("press_releases", []):
            press_content += f"[PRESS] {release['title']}\n{release.get('snippet', '')}\n\n"
        
        combined_content = f"""
        === 公式サイト情報 ===
        {official_content}
        
        === 最新ニュース ===
        {news_content}
        
        === プレスリリース ===
        {press_content}
        """
        
        if not combined_content.strip():
            logger.warning(f"No content to extract for {company_name}")
            return {}
        
        prompt = f"""
        以下の企業に関する包括的な情報を抽出してください。
        企業名: {company_name}
        業界: {industry}

        情報源:
        {combined_content}

        以下のJSON形式で情報を抽出してください。最新のニュースやプレスリリースから得られる課題や動向も含めて分析してください。
        {{
          "name_legal": "正式商号",
          "overview_text": "企業概要（300-500文字で、事業内容、強み、特徴、最新動向を具体的に記述）",
          "services_text": "主要サービス一覧（箇条書き、具体的なサービス名）",
          "products_text": "主要製品一覧（箇条書き、具体的な製品名）",
          "employee_count": 数値（従業員数、不明な場合はnull）,
          "employee_count_source_url": "従業員数出典URL（見つかる場合）",
          "hq_address_raw": "本社住所（生のテキスト情報）",
          "prefecture_name": "都道府県名（例: 東京都、大阪府）",
          "inquiry_url": "問い合わせページのURL（見つかる場合）",
          "pain_hypotheses": ["最新ニュースや業界動向から推測される課題仮説1", "課題仮説2", "課題仮説3"],
          "recent_developments": ["最新の動向や発表事項1", "動向2", "動向3"],
          "news_insights": ["ニュースから読み取れる企業の特徴や課題1", "インサイト2"]
        }}
        """
        
        try:
            # HTTP APIを使用してGeminiを呼び出し
            response_data = await self._call_gemini_api(prompt)
            
            if response_data:
                logger.info(f"Gemini comprehensive extraction successful for {company_name}")
                return response_data
            else:
                logger.warning(f"Gemini API returned empty response for {company_name}")
                return {}
                
        except Exception as e:
            logger.error(f"Error in Gemini comprehensive extraction for {company_name}: {e}")
            return {"error": str(e)}

    async def extract_company_info(
        self, 
        html_content: str, 
        company_name: str,
        industry: str = ""
    ) -> Dict[str, Any]:
        """Extract company information from HTML content using Gemini 2.5 Flash."""
        try:
            if not html_content or len(html_content) < 100:
                logger.warning(f"Insufficient content for {company_name}")
                return self._get_empty_result()
            
            # Truncate content if too long (Gemini has token limits)
            max_content_length = 50000  # ~12K tokens
            if len(html_content) > max_content_length:
                html_content = html_content[:max_content_length]
                logger.info(f"Content truncated for {company_name}: {len(html_content)} chars")
            
            # Create extraction prompt
            prompt = self._create_extraction_prompt(company_name, industry, html_content)
            
            # Call Gemini API
            response_data = await self._call_gemini_api(prompt)
            
            if response_data:
                return self._parse_gemini_response(response_data, company_name)
            else:
                return self._get_empty_result()
                
        except Exception as e:
            logger.error(f"Error extracting info for {company_name}: {e}")
            return self._get_empty_result()
    
    def _create_extraction_prompt(self, company_name: str, industry: str, html_content: str) -> str:
        """Create optimized prompt for Gemini 2.5 Flash."""
        return f"""以下のHTMLコンテンツから、企業「{company_name}」の情報を抽出してください。

業界: {industry}

抽出すべき情報:
1. 本社住所（都道府県、市区町村、番地、建物名）
2. 従業員数（数値のみ）
3. 設立年（年のみ）
4. 資本金（金額と単位）
5. 主要サービス（具体的なサービス名、最大5つ）
6. 主要製品（具体的な製品名、最大5つ）
7. 事業内容の詳細（200文字以内）
8. 企業の特徴や強み（100文字以内）

HTMLコンテンツ:
{html_content}

以下のJSON形式で回答してください（情報が見つからない場合はnullまたは空配列を返してください）:
{{
  "address_info": {{
    "prefecture": "都道府県名",
    "city": "市区町村名",
    "address": "番地・建物名",
    "postal_code": "郵便番号（見つかる場合）"
  }},
  "employee_count": 数値,
  "founded_year": 年,
  "capital": "資本金の文字列",
  "services": ["サービス1", "サービス2", "サービス3"],
  "products": ["製品1", "製品2", "製品3"],
  "business_description": "事業内容の詳細",
  "company_features": "企業の特徴や強み"
}}"""
    
    async def _call_gemini_api(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call Gemini API with the given prompt."""
        try:
            if self.use_vertex_ai:
                return await self._call_vertex_ai(prompt)
            else:
                return await self._call_generative_ai(prompt)
        except Exception as e:
            logger.error(f"Gemini API call failed: {e}")
            return None
    
    async def _call_vertex_ai(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call Vertex AI Gemini API."""
        try:
            # Get access token for Vertex AI
            access_token = await self._get_vertex_ai_token()
            if not access_token:
                logger.error("Could not get Vertex AI access token")
                return None
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "contents": [{
                    "parts": [{
                        "text": prompt
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 2048,
                    "topP": 0.8,
                    "topK": 40
                }
            }
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(self.base_url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        error_text = await response.text()
                        logger.error(f"Vertex AI API error {response.status}: {error_text}")
                        return None
                        
        except Exception as e:
            logger.error(f"Vertex AI API call failed: {e}")
            return None
    
    async def _call_generative_ai(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call Generative AI Gemini API."""
        try:
            if not self.api_key:
                logger.error("Gemini API key not configured")
                return None
            
            headers = {
                "Content-Type": "application/json"
            }
            
            payload = {
                "contents": [{
                    "parts": [{
                        "text": prompt
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 2048,
                    "topP": 0.8,
                    "topK": 40
                }
            }
            
            url = f"{self.base_url}?key={self.api_key}"
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        error_text = await response.text()
                        logger.error(f"Generative AI API error {response.status}: {error_text}")
                        return None
                        
        except Exception as e:
            logger.error(f"Generative AI API call failed: {e}")
            return None
    
    async def _get_vertex_ai_token(self) -> Optional[str]:
        """Get access token for Vertex AI."""
        try:
            # Use gcloud to get access token
            import subprocess
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                logger.error(f"Failed to get access token: {result.stderr}")
                return None
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None
    
    def _parse_gemini_response(self, response_data: Dict[str, Any], company_name: str) -> Dict[str, Any]:
        """Parse Gemini API response."""
        try:
            # Extract text from response
            if "candidates" in response_data and response_data["candidates"]:
                candidate = response_data["candidates"][0]
                if "content" in candidate and "parts" in candidate["content"]:
                    text = candidate["content"]["parts"][0].get("text", "")
                    
                    # Try to parse as JSON
                    try:
                        # Find JSON in the response
                        json_start = text.find("{")
                        json_end = text.rfind("}") + 1
                        
                        if json_start >= 0 and json_end > json_start:
                            json_text = text[json_start:json_end]
                            parsed_data = json.loads(json_text)
                            
                            # Validate and clean the data
                            return self._validate_and_clean_data(parsed_data, company_name)
                        else:
                            logger.warning(f"No JSON found in Gemini response for {company_name}")
                            return self._get_empty_result()
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON from Gemini response for {company_name}: {e}")
                        return self._get_empty_result()
            
            logger.warning(f"No valid response from Gemini for {company_name}")
            return self._get_empty_result()
            
        except Exception as e:
            logger.error(f"Error parsing Gemini response for {company_name}: {e}")
            return self._get_empty_result()
    
    def _validate_and_clean_data(self, data: Dict[str, Any], company_name: str) -> Dict[str, Any]:
        """Validate and clean extracted data."""
        cleaned_data = {
            "company_name": company_name,
            "address_info": {},
            "employee_count": None,
            "founded_year": None,
            "capital": None,
            "services": [],
            "products": [],
            "business_description": "",
            "company_features": "",
            "extraction_status": "success"
        }
        
        # Address info
        if "address_info" in data and isinstance(data["address_info"], dict):
            address_info = data["address_info"]
            cleaned_data["address_info"] = {
                "prefecture": address_info.get("prefecture", ""),
                "city": address_info.get("city", ""),
                "address": address_info.get("address", ""),
                "postal_code": address_info.get("postal_code", "")
            }
        
        # Employee count
        if "employee_count" in data and isinstance(data["employee_count"], (int, str)):
            try:
                cleaned_data["employee_count"] = int(str(data["employee_count"]).replace(",", ""))
            except (ValueError, TypeError):
                pass
        
        # Founded year
        if "founded_year" in data and isinstance(data["founded_year"], (int, str)):
            try:
                year = int(str(data["founded_year"]))
                if 1800 <= year <= 2024:
                    cleaned_data["founded_year"] = year
            except (ValueError, TypeError):
                pass
        
        # Capital
        if "capital" in data and isinstance(data["capital"], str):
            cleaned_data["capital"] = data["capital"].strip()
        
        # Services
        if "services" in data and isinstance(data["services"], list):
            cleaned_data["services"] = [
                str(service).strip() for service in data["services"][:5]
                if isinstance(service, str) and service.strip()
            ]
        
        # Products
        if "products" in data and isinstance(data["products"], list):
            cleaned_data["products"] = [
                str(product).strip() for product in data["products"][:5]
                if isinstance(product, str) and product.strip()
            ]
        
        # Business description
        if "business_description" in data and isinstance(data["business_description"], str):
            cleaned_data["business_description"] = data["business_description"].strip()[:500]
        
        # Company features
        if "company_features" in data and isinstance(data["company_features"], str):
            cleaned_data["company_features"] = data["company_features"].strip()[:200]
        
        return cleaned_data
    
    def _get_empty_result(self) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "company_name": "",
            "address_info": {},
            "employee_count": None,
            "founded_year": None,
            "capital": None,
            "services": [],
            "products": [],
            "business_description": "",
            "company_features": "",
            "extraction_status": "failed"
        }
    
    async def extract_from_multiple_pages(
        self, 
        pages: List[Dict[str, Any]], 
        company_name: str,
        industry: str = ""
    ) -> Dict[str, Any]:
        """Extract information from multiple pages and combine results."""
        try:
            if not pages:
                return self._get_empty_result()
            
            # Combine content from all pages
            combined_content = ""
            for page in pages:
                if page.get("content"):
                    combined_content += f"\n\n--- {page.get('title', '')} ---\n\n"
                    combined_content += page["content"]
            
            if not combined_content.strip():
                return self._get_empty_result()
            
            # Extract information from combined content
            return await self.extract_company_info(combined_content, company_name, industry)
            
        except Exception as e:
            logger.error(f"Error extracting from multiple pages for {company_name}: {e}")
            return self._get_empty_result()
