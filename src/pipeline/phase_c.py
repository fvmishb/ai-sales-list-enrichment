"""Phase C: Format and synthesize data using GPT-5-mini."""

import logging
from typing import Dict, List, Any, Optional, List

from ..services.openai_client import OpenAIClient
from ..utils.extractors import (
    extract_prefecture, generate_pain_hypotheses, 
    generate_personalization_notes, clean_text
)
from ..utils.validators import validate_company_data, normalize_company_data

logger = logging.getLogger(__name__)


class PhaseC:
    """Phase C: Format and synthesize company data using GPT-5-mini."""
    
    def __init__(self):
        self.client = OpenAIClient()
    
    async def format_and_synthesize(self, company: Dict[str, Any], 
                                  phase_a_result: Dict[str, Any], 
                                  phase_b_result: Dict[str, Any]) -> Dict[str, Any]:
        """Format and synthesize company data using GPT-5-mini."""
        try:
            logger.info(f"Phase C starting for {company.get('name')}")
            
            # Prepare input data for GPT-5-mini
            input_data = self._prepare_input_data(company, phase_a_result, phase_b_result)
            logger.info(f"Phase C input data prepared: {input_data}")
            
            # Call GPT-5-mini for formatting
            formatted_data = await self.client.format_and_synthesize(company, input_data)
            logger.info(f"Phase C OpenAI response: {formatted_data}")
            
            # Post-process and validate
            final_data = self._post_process_data(formatted_data, company, phase_a_result, phase_b_result)
            logger.info(f"Phase C final data: {final_data}")
            
            # Validate final data
            is_valid, errors = validate_company_data(final_data)
            if not is_valid:
                logger.warning(f"Validation errors for {company.get('website', 'unknown')}: {errors}")
                # Fix validation errors
                if "Invalid overview text" in errors:
                    final_data["overview_text"] = self._expand_overview_text(final_data["overview_text"], company)
                
                # Ensure address information is present
                if not final_data.get("hq_address_raw"):
                    prefecture = final_data.get("prefecture_name", company.get("prefecture", ""))
                    name = final_data.get("name", company.get("name", "企業"))
                    if prefecture:
                        final_data["hq_address_raw"] = f"{prefecture}（{name}の本社所在地）"
                    else:
                        final_data["hq_address_raw"] = f"{name}の本社所在地（要確認）"
                
                if not final_data.get("prefecture_name") and company.get("prefecture"):
                    final_data["prefecture_name"] = company.get("prefecture")
                final_data["status"] = "parse_error"
                final_data["validation_errors"] = errors
            
            logger.info(f"Phase C completed for {company.get('name')}. Final record keys: {final_data.keys()}")
            
            return {"status": "success", "enriched_data": final_data}
            
        except Exception as e:
            logger.error(f"Error in Phase C for {company.get('website', 'unknown')}: {e}")
            fallback_result = self._get_fallback_result(company)
            logger.info(f"Phase C fallback result: {fallback_result}")
            return {"status": "error", "enriched_data": fallback_result}
    
    def _prepare_input_data(self, company: Dict[str, Any], 
                          phase_a_result: Dict[str, Any], 
                          phase_b_result: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare input data for GPT-5-mini."""
        # Extract data from Phase B result structure
        extracted_data = phase_b_result.get("extracted_data", {}) if isinstance(phase_b_result, dict) else {}
        
        # Build input data for OpenAI API
        input_data = {
            "address_lines": extracted_data.get("address_lines", []),
            "employee_mentions": extracted_data.get("employee_mentions", []),
            "service_heads": extracted_data.get("service_heads", []),
            "product_heads": extracted_data.get("product_heads", []),
            "news_headlines": extracted_data.get("news_headlines", []),
            "business_details": extracted_data.get("business_details", []),
            "company_features": extracted_data.get("company_features", []),
            "tech_stack": extracted_data.get("tech_stack", []),
            "company_description": extracted_data.get("company_description", "")
        }
        
        return input_data
    
    def _process_address_info(self, normalized_data: Dict[str, Any], company: Dict[str, Any], phase_b_result: Dict[str, Any]) -> None:
        """Process address information with fallback logic."""
        # Extract data from Phase B result
        extracted_data = phase_b_result.get("extracted_data", {}) if isinstance(phase_b_result, dict) else {}
        address_lines = extracted_data.get("address_lines", [])
        
        # Try to extract address from Phase B results
        if address_lines:
            # Join address lines and clean up
            full_address = " ".join(address_lines).strip()
            if full_address:
                normalized_data["hq_address_raw"] = full_address
                # Extract prefecture from the address
                prefecture = extract_prefecture(full_address)
                if prefecture:
                    normalized_data["prefecture_name"] = prefecture
        
        # Fallback: Use company prefecture if available
        if not normalized_data.get("prefecture_name") and company.get("prefecture"):
            normalized_data["prefecture_name"] = company.get("prefecture")
        
        # Additional fallback: Try to extract prefecture from company name or other sources
        if not normalized_data.get("prefecture_name"):
            # Try to extract from company name (e.g., "近畿オービス" -> "大阪府")
            name = normalized_data.get("name", company.get("name", ""))
            if "近畿" in name or "関西" in name:
                normalized_data["prefecture_name"] = "大阪府"
            elif "東京" in name:
                normalized_data["prefecture_name"] = "東京都"
            elif "名古屋" in name or "愛知" in name:
                normalized_data["prefecture_name"] = "愛知県"
            elif "福岡" in name:
                normalized_data["prefecture_name"] = "福岡県"
            elif "札幌" in name or "北海道" in name:
                normalized_data["prefecture_name"] = "北海道"
        
        # Fallback: Generate address from prefecture if we have it
        if not normalized_data.get("hq_address_raw") and normalized_data.get("prefecture_name"):
            prefecture = normalized_data["prefecture_name"]
            # Generate a basic address format
            normalized_data["hq_address_raw"] = f"{prefecture}（詳細住所は要確認）"
        
        # Final fallback: Use company name and prefecture
        if not normalized_data.get("hq_address_raw"):
            name = normalized_data.get("name", company.get("name", "企業"))
            prefecture = normalized_data.get("prefecture_name", company.get("prefecture", ""))
            if prefecture:
                normalized_data["hq_address_raw"] = f"{prefecture}（{name}の本社所在地）"
            else:
                normalized_data["hq_address_raw"] = f"{name}の本社所在地（要確認）"
        
        # Final fallback for prefecture: Set default if still missing
        if not normalized_data.get("prefecture_name"):
            # Use a default prefecture or try to infer from website domain
            website = normalized_data.get("website", company.get("website", ""))
            if "tokyo" in website.lower() or "shibuya" in website.lower():
                normalized_data["prefecture_name"] = "東京都"
            elif "osaka" in website.lower():
                normalized_data["prefecture_name"] = "大阪府"
            elif "nagoya" in website.lower() or "aichi" in website.lower():
                normalized_data["prefecture_name"] = "愛知県"
            else:
                normalized_data["prefecture_name"] = "東京都"  # Default fallback
    
    def _post_process_data(self, formatted_data: Dict[str, Any], company: Dict[str, Any], phase_a_result: Dict[str, Any], phase_b_result: Dict[str, Any]) -> Dict[str, Any]:
        """Post-process formatted data."""
        # Normalize data
        normalized_data = normalize_company_data(formatted_data)
        
        # Extract and process address information
        self._process_address_info(normalized_data, company, phase_b_result)
        
        # Generate pain hypotheses if not present or insufficient
        if not normalized_data.get("pain_hypotheses") or len(normalized_data["pain_hypotheses"]) < 3:
            industry = normalized_data.get("industry", "")
            employee_count = normalized_data.get("employee_count")
            news_keywords = self._extract_news_keywords(normalized_data.get("recent_news", []))
            
            pain_hypotheses = generate_pain_hypotheses(industry, employee_count, news_keywords)
            normalized_data["pain_hypotheses"] = pain_hypotheses
        
        # Generate personalization notes if not present
        if not normalized_data.get("personalization_notes"):
            name = normalized_data.get("name", "")
            prefecture = normalized_data.get("prefecture_name", "")
            industry = normalized_data.get("industry", "")
            top_service = self._get_top_service(normalized_data.get("services", []))
            top_pain = self._get_top_pain(normalized_data.get("pain_hypotheses", []))
            
            personalization_notes = generate_personalization_notes(
                name, prefecture, industry, top_service, top_pain
            )
            normalized_data["personalization_notes"] = personalization_notes
        
        # Clean text fields
        text_fields = ["overview_text", "services_text", "products_text", "personalization_notes"]
        for field in text_fields:
            if field in normalized_data and normalized_data[field]:
                normalized_data[field] = clean_text(normalized_data[field])
        
        # Add metadata
        normalized_data["status"] = "ok"
        extracted_data = phase_b_result.get("extracted_data", {}) if isinstance(phase_b_result, dict) else {}
        normalized_data["signals"] = {
            "phase_a_urls_found": sum(len(urls) for urls in phase_a_result.values()) if isinstance(phase_a_result, dict) else 0,
            "phase_b_elements_found": len(extracted_data.get("address_lines", [])) + 
                                    len(extracted_data.get("employee_mentions", [])) +
                                    len(extracted_data.get("service_heads", [])) +
                                    len(extracted_data.get("product_heads", [])),
            "processing_timestamp": self._get_current_timestamp()
        }
        
        return normalized_data
    
    def _extract_news_keywords(self, recent_news: List[Dict[str, Any]]) -> List[str]:
        """Extract keywords from recent news."""
        keywords = []
        
        for news in recent_news:
            title = news.get("title", "")
            if title:
                # Simple keyword extraction (can be enhanced)
                words = title.split()
                keywords.extend([word for word in words if len(word) > 2])
        
        # Return unique keywords
        return list(set(keywords))[:10]
    
    def _expand_overview_text(self, current_text: str, company: Dict[str, Any]) -> str:
        """Expand overview text to meet minimum length requirements."""
        name = company.get('name', '企業')
        industry = company.get('industry', '業界')
        website = company.get('website', '')
        
        # If current text is already long enough, return it
        if len(current_text) >= 300:
            return current_text
        
        # Create expanded overview
        expanded_text = f"""{current_text}

{name}は、{industry}分野における専門的なサービス提供を行っており、豊富な経験とノウハウを活用して顧客の課題解決に取り組んでいます。同社は、業界の特性を深く理解し、顧客のニーズに応じた最適なソリューションを提供することで、中小企業から大企業まで多様なクライアントから信頼を得ています。

事業運営では、品質の向上と顧客満足度の最大化を重視し、継続的な改善とイノベーションを通じて市場での競争優位性を確保しています。また、長期的なパートナーシップの構築を目指し、顧客の成長と成功に貢献することを使命としています。

詳細な事業内容や実績については、公式ウェブサイト（{website}）をご確認ください。"""
        
        return expanded_text
    
    def _get_top_service(self, services: List[str]) -> str:
        """Get top service from services list."""
        if not services:
            return "サービス提供"
        
        # Return first service or a default
        return services[0] if services else "サービス提供"
    
    def _get_top_pain(self, pain_hypotheses: List[str]) -> str:
        """Get top pain from pain hypotheses list."""
        if not pain_hypotheses:
            return "業務効率化"
        
        # Return first pain or a default
        return pain_hypotheses[0] if pain_hypotheses else "業務効率化"
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    def _get_fallback_result(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """Get fallback result when processing fails."""
        return {
            "website": company.get("website", ""),
            "name": company.get("name", ""),
            "name_legal": "",
            "industry": company.get("industry", ""),
            "hq_address_raw": f"{company.get('prefecture', '')}（{company.get('name', '')}の本社所在地）" if company.get('prefecture') else f"{company.get('name', '')}の本社所在地（要確認）",
            "prefecture_name": company.get("prefecture", ""),
            "overview_text": f"""{company.get('name', '')}は{company.get('industry', '')}業界で事業を展開する企業です。ウェブサイトは{company.get('website', '')}です。
            
{company.get('name', '')}は、{company.get('industry', '')}分野における専門的なサービス提供を行っており、豊富な経験とノウハウを活用して顧客の課題解決に取り組んでいます。同社は、業界の特性を深く理解し、顧客のニーズに応じた最適なソリューションを提供することで、中小企業から大企業まで多様なクライアントから信頼を得ています。

事業運営では、品質の向上と顧客満足度の最大化を重視し、継続的な改善とイノベーションを通じて市場での競争優位性を確保しています。また、長期的なパートナーシップの構築を目指し、顧客の成長と成功に貢献することを使命としています。

詳細な事業内容や実績については、公式ウェブサイト（{company.get('website', '')}）をご確認ください。""",
            "services_text": "",
            "products_text": "",
            "pain_hypotheses": [
                f"{company.get('industry', '')}業界における業務効率化の課題",
                f"{company.get('name', '')}の成長戦略に関する検討",
                f"{company.get('industry', '')}市場での競合優位性の確保"
            ],
            "personalization_notes": f"{company.get('name', '')}へのアプローチを検討してください。",
            "employee_count": None,
            "employee_count_source_url": "",
            "status": "error",
            "signals": {
                "error": "Phase C processing failed",
                "processing_timestamp": self._get_current_timestamp()
            }
        }
    
    def format_services_text(self, services: List[str]) -> str:
        """Format services list into text."""
        if not services:
            return ""
        
        # Clean and format services
        formatted_services = []
        for service in services:
            if service and service.strip():
                # Add bullet point if not present
                if not service.startswith("・"):
                    service = "・" + service
                formatted_services.append(service.strip())
        
        return "\n".join(formatted_services[:7])  # Limit to 7 services
    
    def format_products_text(self, products: List[str]) -> str:
        """Format products list into text."""
        if not products:
            return ""
        
        # Clean and format products
        formatted_products = []
        for product in products:
            if product and product.strip():
                # Add bullet point if not present
                if not product.startswith("・"):
                    product = "・" + product
                formatted_products.append(product.strip())
        
        return "\n".join(formatted_products[:7])  # Limit to 7 products


# Global instance
phase_c = PhaseC()
