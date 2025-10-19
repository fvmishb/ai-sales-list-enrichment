import asyncio
import json
import logging
from typing import Dict, Any
import aiohttp
from aiohttp import ClientTimeout

from ..config import settings, get_secret

logger = logging.getLogger(__name__)

class SimpleGeminiClient:
    """シンプルなGemini 2.5 Flash APIクライアント"""
    
    def __init__(self):
        self.api_key = self._get_api_key()
        self.timeout = ClientTimeout(total=60)
        
        if settings.use_vertex_ai:
            self.base_url = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{settings.gcp_project_id}/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent"
        else:
            self.base_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    
    def _get_api_key(self) -> str:
        """Get access token for Vertex AI authentication."""
        if not settings.use_vertex_ai:
            # For direct API key authentication
            if settings.gemini_api_key:
                logger.info(f"Using Gemini API key from settings: {settings.gemini_api_key[:10]}...")
                return settings.gemini_api_key
            
            # Try to get from Secret Manager
            try:
                api_key = get_secret("gemini-api-key")
                # Remove any newline characters from the API key
                api_key = api_key.strip()
                logger.info(f"Retrieved Gemini API key from Secret Manager: {api_key[:10]}...")
                return api_key
            except Exception as e:
                logger.error(f"Could not get Gemini API key: {e}")
                return ""
        else:
            # For Vertex AI, we need to get an access token
            try:
                import google.auth
                from google.auth.transport.requests import Request
                
                credentials, project = google.auth.default()
                credentials.refresh(Request())
                access_token = credentials.token
                logger.info(f"Retrieved Vertex AI access token: {access_token[:10]}...")
                return access_token
            except Exception as e:
                logger.error(f"Could not get Vertex AI access token: {e}")
                return ""
    
    async def extract_company_info(self, html_content: str, company_name: str, industry: str = "") -> Dict[str, Any]:
        """HTMLから企業情報を抽出"""
        try:
            if not html_content or len(html_content) < 100:
                logger.warning(f"Insufficient content for {company_name}")
                return self._get_empty_result()
            
            # コンテンツを制限（Geminiのトークン制限）
            max_content_length = 30000  # ~7.5K tokens
            if len(html_content) > max_content_length:
                html_content = html_content[:max_content_length]
                logger.info(f"Content truncated for {company_name}: {len(html_content)} chars")
            
            # プロンプトを作成
            prompt = self._create_extraction_prompt(company_name, industry, html_content)
            
            # Gemini APIを呼び出し
            response_data = await self._call_gemini_api(prompt)
            
            if response_data:
                return self._parse_gemini_response(response_data, company_name)
            else:
                return self._get_empty_result()
                
        except Exception as e:
            logger.error(f"Error extracting info for {company_name}: {e}")
            return self._get_empty_result()
    
    def _create_extraction_prompt(self, company_name: str, industry: str, html_content: str) -> str:
        """抽出用のプロンプトを作成"""
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

以下のJSON形式で情報を抽出してください。情報が見つからない場合は、そのフィールドを空の文字列またはnullにしてください。
{{
  "name_legal": "正式商号",
  "overview_text": "企業概要（200-400文字程度で、事業内容、強み、特徴を具体的に記述）",
  "services_text": "主要サービス一覧（箇条書き、具体的なサービス名）",
  "products_text": "主要製品一覧（箇条書き、具体的な製品名）",
  "employee_count": 数値（従業員数、不明な場合はnull）,
  "employee_count_source_url": "従業員数出典URL（見つかる場合）",
  "hq_address_raw": "本社住所（生のテキスト情報）",
  "prefecture_name": "都道府県名（例: 東京都、大阪府）",
  "inquiry_url": "問い合わせページのURL（見つかる場合）",
  "pain_hypotheses": ["企業が抱える可能性のある課題仮説1", "課題仮説2", "課題仮説3"]
}}"""
    
    async def _call_gemini_api(self, prompt: str) -> Dict[str, Any]:
        """Gemini APIを呼び出し"""
        try:
            if not self.api_key:
                logger.error("Gemini API key not configured")
                return None
            
            logger.info(f"Calling Gemini API with key: {self.api_key[:10]}...")
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # For Vertex AI, we use the project-based URL with Bearer token
            url = self.base_url
            
            payload = {
                "contents": [{
                    "parts": [{
                        "text": prompt
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.1,
                    "topK": 1,
                    "topP": 0.8,
                    "maxOutputTokens": 2048
                }
            }
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        error_text = await response.text()
                        logger.error(f"Gemini API error {response.status}: {error_text}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            return None
    
    def _parse_gemini_response(self, response_data: Dict[str, Any], company_name: str) -> Dict[str, Any]:
        """Geminiのレスポンスをパース"""
        try:
            if "candidates" not in response_data or not response_data["candidates"]:
                logger.warning(f"No candidates in Gemini response for {company_name}")
                return self._get_empty_result()
            
            candidate = response_data["candidates"][0]
            if "content" not in candidate or "parts" not in candidate["content"]:
                logger.warning(f"No content in Gemini response for {company_name}")
                return self._get_empty_result()
            
            content = candidate["content"]["parts"][0]["text"]
            
            # JSONを抽出
            if content.startswith("```json") and content.endswith("```"):
                json_str = content[7:-3].strip()
            elif content.startswith("```") and content.endswith("```"):
                json_str = content[3:-3].strip()
            else:
                json_str = content.strip()
            
            # JSONをパース
            extracted_data = json.loads(json_str)
            
            # 必須フィールドをチェック
            if not extracted_data.get("name_legal"):
                extracted_data["name_legal"] = company_name
            
            logger.info(f"Successfully extracted data for {company_name}")
            return extracted_data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {company_name}: {e}")
            logger.error(f"Response content: {content}")
            return self._get_empty_result()
        except Exception as e:
            logger.error(f"Error parsing Gemini response for {company_name}: {e}")
            return self._get_empty_result()
    
    def _get_empty_result(self) -> Dict[str, Any]:
        """空の結果を返す"""
        return {
            "name_legal": "",
            "overview_text": "",
            "services_text": "",
            "products_text": "",
            "employee_count": None,
            "employee_count_source_url": "",
            "hq_address_raw": "",
            "prefecture_name": "",
            "inquiry_url": "",
            "pain_hypotheses": []
        }
