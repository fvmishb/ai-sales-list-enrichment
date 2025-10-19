"""Data validation utilities for enterprise information."""

import re
from typing import Dict, Any, List, Optional, Tuple, List
from urllib.parse import urlparse

from .extractors import PREFECTURES, extract_domain, extract_apex_domain


def validate_website(url: str) -> bool:
    """ウェブサイトURLの妥当性を検証する。"""
    if not url or not isinstance(url, str):
        return False
    
    # 基本的なURL形式チェック
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        
        # 有効なスキームかチェック
        if parsed.scheme not in ['http', 'https']:
            return False
        
        # ドメインが存在するかチェック
        if not parsed.netloc or '.' not in parsed.netloc:
            return False
        
        return True
    except Exception:
        return False


def validate_company_name(name: str) -> bool:
    """企業名の妥当性を検証する。"""
    if not name or not isinstance(name, str):
        return False
    
    # 空文字列や空白のみでないかチェック
    if not name.strip():
        return False
    
    # 長すぎないかチェック（200文字以内）
    if len(name) > 200:
        return False
    
    # 無効な文字が含まれていないかチェック
    invalid_chars = ['<', '>', '"', "'", '&', '\n', '\r', '\t']
    if any(char in name for char in invalid_chars):
        return False
    
    return True


def validate_industry(industry: str) -> bool:
    """業界の妥当性を検証する。"""
    if not industry or not isinstance(industry, str):
        return False
    
    # 空文字列でないかチェック
    if not industry.strip():
        return False
    
    # 長すぎないかチェック（100文字以内）
    if len(industry) > 100:
        return False
    
    return True


def validate_prefecture(prefecture: str) -> bool:
    """都道府県の妥当性を検証する。"""
    if not prefecture or not isinstance(prefecture, str):
        return False
    
    # 47都道府県に含まれているかチェック
    return prefecture in PREFECTURES


def validate_employee_count(count: Any) -> bool:
    """従業員数の妥当性を検証する。"""
    if count is None:
        return True  # NULLは許可
    
    # 数値かチェック
    if not isinstance(count, (int, float)):
        return False
    
    # 正の整数かチェック
    if count < 0 or count != int(count):
        return False
    
    # 現実的な範囲かチェック（1億人以下）
    if count > 100_000_000:
        return False
    
    return True


def validate_overview_text(text: str) -> bool:
    """企業概要テキストの妥当性を検証する。"""
    if not text or not isinstance(text, str):
        return False
    
    # 文字数チェック（300-500文字）
    if len(text) < 300 or len(text) > 500:
        return False
    
    # 空文字列でないかチェック
    if not text.strip():
        return False
    
    return True


def validate_services_text(text: str) -> bool:
    """サービステキストの妥当性を検証する。"""
    if not text or not isinstance(text, str):
        return True  # 空は許可
    
    # 長すぎないかチェック（1000文字以内）
    if len(text) > 1000:
        return False
    
    return True


def validate_products_text(text: str) -> bool:
    """製品テキストの妥当性を検証する。"""
    if not text or not isinstance(text, str):
        return True  # 空は許可
    
    # 長すぎないかチェック（1000文字以内）
    if len(text) > 1000:
        return False
    
    return True


def validate_pain_hypotheses(hypotheses: List[str]) -> bool:
    """課題仮説の妥当性を検証する。"""
    if not isinstance(hypotheses, list):
        return False
    
    # 3-5個の仮説があるかチェック
    if len(hypotheses) < 3 or len(hypotheses) > 5:
        return False
    
    # 各仮説が有効かチェック
    for hypothesis in hypotheses:
        if not isinstance(hypothesis, str) or not hypothesis.strip():
            return False
        
        # 長すぎないかチェック（120文字以内）
        if len(hypothesis) > 120:
            return False
    
    return True


def validate_personalization_notes(notes: str) -> bool:
    """パーソナライゼーションノートの妥当性を検証する。"""
    if not notes or not isinstance(notes, str):
        return True  # 空は許可
    
    # 長すぎないかチェック（500文字以内）
    if len(notes) > 500:
        return False
    
    return True


def validate_company_data(company: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """企業データ全体の妥当性を検証する。"""
    errors = []
    
    # 必須フィールドの検証
    if not validate_website(company.get('website', '')):
        errors.append("Invalid website URL")
    
    if not validate_company_name(company.get('name', '')):
        errors.append("Invalid company name")
    
    if not validate_industry(company.get('industry', '')):
        errors.append("Invalid industry")
    
    # オプションフィールドの検証
    if 'prefecture' in company and not validate_prefecture(company['prefecture']):
        errors.append("Invalid prefecture")
    
    if 'employee_count' in company and not validate_employee_count(company['employee_count']):
        errors.append("Invalid employee count")
    
    if 'overview_text' in company and not validate_overview_text(company['overview_text']):
        errors.append("Invalid overview text")
    
    if 'services_text' in company and not validate_services_text(company['services_text']):
        errors.append("Invalid services text")
    
    if 'products_text' in company and not validate_products_text(company['products_text']):
        errors.append("Invalid products text")
    
    if 'pain_hypotheses' in company and not validate_pain_hypotheses(company['pain_hypotheses']):
        errors.append("Invalid pain hypotheses")
    
    if 'personalization_notes' in company and not validate_personalization_notes(company['personalization_notes']):
        errors.append("Invalid personalization notes")
    
    return len(errors) == 0, errors


def sanitize_text(text: str) -> str:
    """テキストをサニタイズする。"""
    if not text or not isinstance(text, str):
        return ""
    
    # HTMLタグを除去
    text = re.sub(r'<[^>]+>', '', text)
    
    # 制御文字を除去
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    
    # 余分な空白を正規化
    text = re.sub(r'\s+', ' ', text)
    
    # 前後の空白を除去
    text = text.strip()
    
    return text


def normalize_company_data(company: Dict[str, Any]) -> Dict[str, Any]:
    """企業データを正規化する。"""
    normalized = company.copy()
    
    # テキストフィールドをサニタイズ
    text_fields = ['name', 'name_legal', 'industry', 'hq_address_raw', 
                   'prefecture_name', 'overview_text', 'services_text', 
                   'products_text', 'personalization_notes']
    
    for field in text_fields:
        if field in normalized and normalized[field]:
            normalized[field] = sanitize_text(normalized[field])
    
    # ウェブサイトを正規化
    if 'website' in normalized and normalized['website']:
        normalized['website'] = normalize_url(normalized['website'])
    
    # 従業員数を正規化
    if 'employee_count' in normalized and normalized['employee_count'] is not None:
        try:
            normalized['employee_count'] = int(normalized['employee_count'])
        except (ValueError, TypeError):
            normalized['employee_count'] = None
    
    # 課題仮説を正規化
    if 'pain_hypotheses' in normalized and normalized['pain_hypotheses']:
        normalized['pain_hypotheses'] = [
            sanitize_text(h) for h in normalized['pain_hypotheses'] 
            if h and sanitize_text(h)
        ]
    
    return normalized


def normalize_url(url: str) -> str:
    """URLを正規化する。"""
    if not url:
        return ""
    
    # プロトコルを追加（ない場合）
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # 末尾のスラッシュを除去
    if url.endswith('/'):
        url = url[:-1]
    
    return url.lower()


def extract_validation_errors(company: Dict[str, Any]) -> Dict[str, List[str]]:
    """企業データから検証エラーを抽出する。"""
    errors = {}
    
    # 各フィールドの検証
    if not validate_website(company.get('website', '')):
        errors['website'] = ['Invalid URL format']
    
    if not validate_company_name(company.get('name', '')):
        errors['name'] = ['Invalid company name']
    
    if not validate_industry(company.get('industry', '')):
        errors['industry'] = ['Invalid industry']
    
    if 'prefecture' in company and not validate_prefecture(company['prefecture']):
        errors['prefecture'] = ['Invalid prefecture']
    
    if 'employee_count' in company and not validate_employee_count(company['employee_count']):
        errors['employee_count'] = ['Invalid employee count']
    
    if 'overview_text' in company and not validate_overview_text(company['overview_text']):
        errors['overview_text'] = ['Invalid overview text length']
    
    if 'pain_hypotheses' in company and not validate_pain_hypotheses(company['pain_hypotheses']):
        errors['pain_hypotheses'] = ['Invalid pain hypotheses count or format']
    
    return errors
