"""Data extraction utilities for enterprise information."""

import re
from typing import List, Optional, Dict, Any, Tuple, List

# 47都道府県リスト
PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
]

# 英語都道府県マッピング
ENGLISH_PREFECTURES = {
    "Tokyo": "東京都",
    "Osaka": "大阪府",
    "Kyoto": "京都府",
    "Hokkaido": "北海道",
    "Aichi": "愛知県",
    "Kanagawa": "神奈川県",
    "Saitama": "埼玉県",
    "Chiba": "千葉県",
    "Hyogo": "兵庫県",
    "Fukuoka": "福岡県"
}

# 住所抽出パターン
ADDRESS_PATTERN = r'(〒\s*\d{3}-?\d{4}\s*)?([^\n\r]{6,120}?[都道府県].*)'

# 従業員数抽出パターン
EMPLOYEE_PATTERNS = [
    r'従業員数\s*[:：]?\s*([\d,，\.]+)\s*名?',
    r'Employees?\s*[:：]?\s*([\d,，\.]+)',
    r'社員数\s*[:：]?\s*([\d,，\.]+)',
    r'スタッフ数\s*[:：]?\s*([\d,，\.]+)',
    r'従業者数\s*[:：]?\s*([\d,，\.]+)'
]


def extract_address(text: str) -> Optional[str]:
    """住所を抽出する。"""
    if not text:
        return None
    
    # 郵便番号付き住所パターン
    match = re.search(ADDRESS_PATTERN, text)
    if match:
        return match.group(2).strip()
    
    # 都道府県を含む住所パターン（郵便番号なし）
    for prefecture in PREFECTURES:
        pattern = rf'([^\n\r]{{6,120}}?{re.escape(prefecture)}[^\n\r]*)'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    
    return None


def extract_prefecture(address: str) -> Optional[str]:
    """住所から都道府県を抽出する。"""
    if not address:
        return None
    
    # 日本語都道府県を検索
    for prefecture in PREFECTURES:
        if prefecture in address:
            return prefecture
    
    # 英語都道府県を検索
    for eng_pref, jp_pref in ENGLISH_PREFECTURES.items():
        if eng_pref in address:
            return jp_pref
    
    return None

def extract_address_from_text(text: str, company_name: str = "") -> Dict[str, Any]:
    """テキストから住所情報を抽出する。"""
    if not text:
        return {"address": "", "prefecture": None}
    
    import re
    
    # 郵便番号パターン
    postal_pattern = r'〒\s*\d{3}-?\d{4}'
    postal_match = re.search(postal_pattern, text)
    
    # 住所パターン（都道府県から始まる）
    address_patterns = [
        r'([都道府県][^。\n\r]{0,50})',  # 都道府県から始まる住所
        r'(〒\s*\d{3}-?\d{4}\s*[^。\n\r]{0,50})',  # 郵便番号から始まる住所
        r'([東京都|大阪府|京都府|神奈川県|埼玉県|千葉県|愛知県|福岡県|北海道][^。\n\r]{0,50})',  # 主要都道府県
    ]
    
    address = ""
    prefecture = None
    
    # 住所を抽出
    for pattern in address_patterns:
        matches = re.findall(pattern, text)
        if matches:
            address = matches[0].strip()
            break
    
    # 都道府県を抽出
    prefecture = extract_prefecture(address)
    
    # 住所が見つからない場合、企業名から推測
    if not address and company_name:
        prefecture = extract_prefecture(company_name)
        if prefecture:
            address = f"{prefecture}に本社を置く{company_name}"
    
    return {
        "address": address,
        "prefecture": prefecture
    }


def extract_employee_count(text: str) -> Tuple[Optional[int], Optional[str]]:
    """従業員数を抽出する。"""
    if not text:
        return None, None
    
    # 各パターンで検索
    for pattern in EMPLOYEE_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            # 最大値を取得（連結/単体併記時）
            max_count = 0
            for match in matches:
                try:
                    # カンマや小数点を除去して整数化
                    count_str = match.replace(',', '').replace('，', '').replace('.', '')
                    count = int(count_str)
                    max_count = max(max_count, count)
                except ValueError:
                    continue
            
            if max_count > 0:
                return max_count, text  # 出典テキストも返す
    
    return None, None


def extract_legal_name(text: str) -> Optional[str]:
    """正式商号を抽出する。"""
    if not text:
        return None
    
    # 株式会社、有限会社、合同会社などのパターン
    patterns = [
        r'(株式会社[^\s\n\r]+)',
        r'(有限会社[^\s\n\r]+)',
        r'(合同会社[^\s\n\r]+)',
        r'(合資会社[^\s\n\r]+)',
        r'(合名会社[^\s\n\r]+)',
        r'(一般社団法人[^\s\n\r]+)',
        r'(一般財団法人[^\s\n\r]+)',
        r'(公益社団法人[^\s\n\r]+)',
        r'(公益財団法人[^\s\n\r]+)',
        r'(NPO法人[^\s\n\r]+)',
        r'(特定非営利活動法人[^\s\n\r]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    
    return None


def generate_pain_hypotheses(industry: str, employee_count: Optional[int], news_keywords: List[str]) -> List[str]:
    """業界・規模・ニュースキーワードから課題仮説を生成する。"""
    hypotheses = []
    
    # 業界別の基本課題
    industry_pains = {
        "IT・web": ["技術人材不足", "セキュリティ強化", "DX推進", "クラウド移行"],
        "製造業界": ["品質管理", "コスト削減", "自動化推進", "サプライチェーン最適化"],
        "小売・卸売業界": ["EC化対応", "在庫管理", "顧客満足度向上", "物流効率化"],
        "金融業界": ["コンプライアンス強化", "デジタル化", "リスク管理", "顧客体験向上"],
        "医療・福祉業界": ["人手不足", "デジタル化", "コスト削減", "サービス品質向上"],
        "教育・学習業界": ["オンライン化", "個別最適化", "学習効果向上", "運営効率化"],
        "建設・建築": ["人手不足", "安全対策", "工期短縮", "コスト管理"],
        "運輸・物流業界": ["ドライバー不足", "燃料費高騰", "配送効率化", "環境対応"],
        "飲食業界": ["人手不足", "食材コスト", "衛生管理", "顧客獲得"],
        "不動産業界": ["デジタル化", "顧客獲得", "物件管理", "法規制対応"]
    }
    
    # 規模別の課題
    size_pains = {
        "small": ["資金調達", "人材確保", "ブランド認知", "業務効率化"],
        "medium": ["組織拡大", "システム統合", "品質管理", "競合対策"],
        "large": ["イノベーション", "グローバル展開", "コンプライアンス", "持続的成長"]
    }
    
    # ニュースキーワード別の課題
    keyword_pains = {
        "AI": ["AI活用", "データ活用", "自動化", "競合優位性"],
        "DX": ["デジタル化", "業務効率化", "顧客体験向上", "競合対策"],
        "環境": ["環境対応", "ESG", "持続可能性", "コスト削減"],
        "人材": ["人材確保", "育成", "定着率向上", "働き方改革"],
        "コスト": ["コスト削減", "効率化", "収益性向上", "競争力強化"]
    }
    
    # 業界課題を追加
    if industry in industry_pains:
        hypotheses.extend(industry_pains[industry][:2])
    
    # 規模課題を追加
    if employee_count:
        if employee_count < 50:
            size_category = "small"
        elif employee_count < 500:
            size_category = "medium"
        else:
            size_category = "large"
        
        if size_category in size_pains:
            hypotheses.extend(size_pains[size_category][:1])
    
    # ニュースキーワード課題を追加
    for keyword in news_keywords:
        for key, pains in keyword_pains.items():
            if key in keyword:
                hypotheses.extend(pains[:1])
                break
    
    # 重複除去と制限
    hypotheses = list(dict.fromkeys(hypotheses))[:5]
    
    # 不足分は汎用課題で補完
    generic_pains = ["業務効率化", "コスト削減", "顧客満足度向上", "競合優位性確保", "成長戦略"]
    while len(hypotheses) < 3:
        for pain in generic_pains:
            if pain not in hypotheses:
                hypotheses.append(pain)
                break
    
    return hypotheses[:5]


def generate_personalization_notes(name: str, prefecture: str, industry: str, 
                                 top_service: str, top_pain: str) -> str:
    """パーソナライゼーション用メモを生成する。"""
    notes = []
    
    # 基本情報
    if prefecture:
        notes.append(f"{name}（{prefecture}）は{industry}領域で「{top_service}」に注力")
    else:
        notes.append(f"{name}は{industry}領域で「{top_service}」に注力")
    
    # 課題情報
    if top_pain:
        notes.append(f"直近トピックから、{top_pain}の検討余地")
    
    # アプローチ提案
    notes.append(f"初回は{industry}向けに具体的なソリューション提案を検討")
    
    return "。".join(notes) + "。"


def clean_text(text: str) -> str:
    """テキストをクリーニングする。"""
    if not text:
        return ""
    
    # 余分な空白を除去
    text = re.sub(r'\s+', ' ', text)
    
    # 改行を統一
    text = re.sub(r'\n+', '\n', text)
    
    # 前後の空白を除去
    text = text.strip()
    
    return text


def extract_domain(url: str) -> str:
    """URLからドメインを抽出する。"""
    if not url:
        return ""
    
    # プロトコルを除去
    if '://' in url:
        url = url.split('://', 1)[1]
    
    # パスを除去
    if '/' in url:
        url = url.split('/', 1)[0]
    
    # ポート番号を除去
    if ':' in url:
        url = url.split(':', 1)[0]
    
    return url.lower()


def extract_apex_domain(domain: str) -> str:
    """ドメインからapexドメインを抽出する。"""
    if not domain:
        return ""
    
    # サブドメインを除去
    parts = domain.split('.')
    if len(parts) >= 2:
        return '.'.join(parts[-2:])
    
    return domain
