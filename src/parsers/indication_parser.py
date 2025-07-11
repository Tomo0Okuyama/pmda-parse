import xml.etree.ElementTree as ET
from typing import List, Dict
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE, remove_duplicates_by_key

class IndicationParser:
    """
    医薬品の効能・効果（適応）をパースするクラス
    """
    def __init__(self, root: ET.Element):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
        """
        self.root = root
        self.namespace = PMDA_NAMESPACE

    def extract_indications(self) -> List[Dict[str, str]]:
        """
        効能・効果（適応）を抽出する
        HeaderとDetailの構造を適切に処理し、Header:Detail形式で結合する

        Returns:
            List[Dict[str, str]]: 効能・効果のリスト
        """
        indications = []
        
        # IndicationsOrEfficacyタグから効能・効果を抽出
        indication_elements = self.root.findall('.//pmda:IndicationsOrEfficacy', namespaces=self.namespace)
        
        for indication_element in indication_elements:
            # まず直下のDetail要素をチェック（Structure 4: IndicationsOrEfficacy/Detail）
            direct_indication_detail = indication_element.find('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if direct_indication_detail is not None and direct_indication_detail.text:
                detail_text = direct_indication_detail.text.strip()
                indications.append({
                    'text': detail_text,
                })
                continue  # 直下のDetailがある場合は、リスト構造の処理をスキップ
            
            # UnorderedList/Item要素とSimpleList/Item要素の両方から効能・効果情報を取得
            item_elements = []
            # UnorderedList構造をチェック
            unordered_items = indication_element.findall('.//pmda:UnorderedList/pmda:Item', namespaces=self.namespace)
            item_elements.extend(unordered_items)
            
            # SimpleList構造もチェック（直下のSimpleList/Item）
            simple_items = indication_element.findall('./pmda:SimpleList/pmda:Item', namespaces=self.namespace)
            item_elements.extend(simple_items)
            
            for item in item_elements:
                # Header要素を取得
                header_element = item.find('./pmda:Header/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                header_text = ""
                if header_element is not None and header_element.text:
                    header_text = header_element.text.strip()
                
                # 直下のDetail要素を取得（Item直下のDetail）
                direct_detail_element = item.find('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                # ネストしたDetail要素（SimpleList内のItem/Detail）を取得
                nested_detail_elements = item.findall('.//pmda:SimpleList/pmda:Item/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                if direct_detail_element is not None and direct_detail_element.text:
                    # 直下のDetailがある場合（Structure 3: UnorderedList/Item/Detail）
                    detail_text = direct_detail_element.text.strip()
                    if header_text:
                        # HeaderとDetailの両方がある場合、コロンで結合
                        combined_text = f"{header_text}:{detail_text}"
                    else:
                        # Detailのみの場合はそのまま追加
                        combined_text = detail_text
                    indications.append({
                        'text': combined_text,
                    })
                elif nested_detail_elements:
                    # ネストしたDetailがある場合（Structure 1: UnorderedList/Item/Header + SimpleList/Item/Detail）
                    for detail_element in nested_detail_elements:
                        if detail_element.text and detail_element.text.strip():
                            detail_text = detail_element.text.strip()
                            if header_text:
                                combined_text = f"{header_text}:{detail_text}"
                            else:
                                combined_text = detail_text
                            indications.append({
                                'text': combined_text,
                            })
                elif header_text:
                    # Headerのみの場合（Structure 2: SimpleList/Item/Header）
                    indications.append({
                        'text': header_text,
                    })
        
        # TherapeuticClassificationは薬効分類名であり効能・効果ではないため除外
        # 効能・効果は IndicationsOrEfficacy セクションにのみ記載される
        
        # 重複除去（同一テキストの重複を除去）
        return remove_duplicates_by_key(indications, 'text')

def parse_indications(file_path: str) -> List[Dict[str, str]]:
    """
    XMLファイルから効能・効果をパースする

    Args:
        file_path (str): パースするXMLファイルのパス

    Returns:
        List[Dict[str, str]]: 効能・効果のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルをパースして効能・効果を抽出
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = IndicationParser(root)
        return parser.extract_indications()
    except Exception:
        return []