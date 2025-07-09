"""
XML処理共通ユーティリティ

PMDAパーサーで共通して使用されるXML処理機能を提供します。
"""

import xml.etree.ElementTree as ET
import re
from typing import Optional, Dict, List


# PMDA XML名前空間の定義
PMDA_NAMESPACE = {
    'pmda': 'http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0',
    'xml': 'http://www.w3.org/XML/1998/namespace'
}


def register_xml_namespaces() -> None:
    """
    XMLの名前空間を登録する
    """
    for prefix, uri in PMDA_NAMESPACE.items():
        ET.register_namespace(prefix, uri)


def safe_find_text(element: ET.Element, xpath: str, namespaces: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    XPathで要素を検索し、テキストを安全に取得する

    Args:
        element: 検索を開始するXML要素
        xpath: 検索するXPath
        namespaces: 名前空間辞書（デフォルトはPMDA_NAMESPACE）

    Returns:
        Optional[str]: 要素のテキスト。見つからない場合はNone
    """
    if namespaces is None:
        namespaces = PMDA_NAMESPACE
    
    found_element = element.find(xpath, namespaces=namespaces)
    return found_element.text.strip() if found_element is not None and found_element.text else None


def extract_clean_text(element: Optional[ET.Element]) -> str:
    """
    XML要素からクリーンなテキストを抽出する
    
    Args:
        element: XML要素
        
    Returns:
        str: クリーンアップされたテキスト
    """
    if element is None:
        return ""
    
    # 要素内のすべてのテキストを結合
    full_text = "".join(element.itertext())
    
    # XMLマーカーを削除
    full_text = full_text.replace('<?enter?>', '\n')
    
    # HTMLタグを適切に処理
    # <Italic>タグの処理
    full_text = re.sub(r'<Italic>(.*?)</Italic>', r'\1', full_text)
    
    # <Sub>タグの処理（下付き文字）
    full_text = re.sub(r'<Sub>(.*?)</Sub>', r'\1', full_text)
    
    # <Sup>タグの処理（上付き文字）
    full_text = re.sub(r'<Sup>(.*?)</Sup>', r'\1', full_text)
    
    # その他のHTMLタグを削除
    full_text = re.sub(r'<[^>]+>', '', full_text)
    
    # 改行を空白に変換し、余分な空白を削除
    full_text = ' '.join(full_text.split())
    
    return full_text.strip()


def extract_lang_text_list(parent_element: ET.Element, xpath: str, namespaces: Optional[Dict[str, str]] = None) -> List[str]:
    """
    指定されたXPathから日本語テキストのリストを抽出する
    
    Args:
        parent_element: 親要素
        xpath: 検索するXPath
        namespaces: 名前空間辞書（デフォルトはPMDA_NAMESPACE）
        
    Returns:
        List[str]: 抽出されたテキストのリスト
    """
    if namespaces is None:
        namespaces = PMDA_NAMESPACE
    
    texts = []
    elements = parent_element.findall(xpath, namespaces=namespaces)
    
    for element in elements:
        if element.text and element.text.strip():
            texts.append(element.text.strip())
    
    return texts


def extract_condition_header(element: ET.Element, namespaces: Optional[Dict[str, str]] = None) -> str:
    """
    要素から条件ヘッダー（疾患名、副作用カテゴリなど）を抽出する
    
    Args:
        element: 対象のXML要素
        namespaces: 名前空間辞書（デフォルトはPMDA_NAMESPACE）
        
    Returns:
        str: 条件ヘッダー文字列、見つからない場合は空文字列
    """
    if namespaces is None:
        namespaces = PMDA_NAMESPACE
    
    # 直接の子要素のHeaderタグ内のLang要素を検索（ネストした要素は除外）
    header_elements = element.findall('./pmda:Header/pmda:Lang[@xml:lang="ja"]', namespaces=namespaces)
    
    for header in header_elements:
        # 内部のテキストを結合して取得（XML参照なども含める）
        full_text = "".join(header.itertext()).strip()
        if full_text:
            # 「〈...〉」の形式の条件を検索
            if '〈' in full_text and '〉' in full_text:
                # 〈〉を除去して条件名を抽出
                condition = full_text.replace('〈', '').replace('〉', '')
                # 空文字や無意味な文字列でない場合のみ返す
                if condition and len(condition.strip()) > 2:
                    return condition.strip()
            # その他のヘッダーも取得（短いものに限定、かつ意味のあるもの）
            elif 1 < len(full_text) < 200:
                return full_text
    
    return ""


def remove_duplicates_by_key(items: List[Dict[str, str]], key: str) -> List[Dict[str, str]]:
    """
    指定されたキーによる重複を除去する
    
    Args:
        items: 辞書のリスト
        key: 重複チェックに使用するキー
        
    Returns:
        List[Dict[str, str]]: 重複除去されたリスト
    """
    seen = set()
    result = []
    
    for item in items:
        if key in item and item[key] not in seen:
            seen.add(item[key])
            result.append(item)
    
    return result


def is_valid_medical_text(text: str, min_length: int = 2, max_length: int = 1000) -> bool:
    """
    医療テキストとして有効かチェックする
    
    Args:
        text: チェックするテキスト
        min_length: 最小文字数
        max_length: 最大文字数
        
    Returns:
        bool: 有効な場合True
    """
    if not text or not text.strip():
        return False
    
    cleaned_text = text.strip()
    
    # 長さチェック
    if len(cleaned_text) < min_length or len(cleaned_text) > max_length:
        return False
    
    # 意味のないテキストを除外
    meaningless_patterns = [
        r'^[0-9\s\-\.]+$',  # 数字と記号のみ
        r'^[a-zA-Z\s]+$',   # アルファベットのみ（短い場合）
        r'^[\s\n\r\t]+$',   # 空白文字のみ
    ]
    
    for pattern in meaningless_patterns:
        if re.match(pattern, cleaned_text):
            return False
    
    return True