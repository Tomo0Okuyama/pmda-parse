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

        Returns:
            List[Dict[str, str]]: 効能・効果のリスト
        """
        indications = []
        
        # IndicationsOrEfficacyタグから効能・効果を抽出
        indication_elements = self.root.findall('.//pmda:IndicationsOrEfficacy', namespaces=self.namespace)
        
        for indication_element in indication_elements:
            # 各Item要素から効能・効果情報を取得
            item_elements = indication_element.findall('.//pmda:Item', namespaces=self.namespace)
            
            for item in item_elements:
                # Detail/Langタグから日本語テキストを取得
                lang_elements = item.findall('.//pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for lang in lang_elements:
                    if lang.text and lang.text.strip():
                        text = lang.text.strip()
                        indications.append({
                            'text': text,
                        })
        
        # TherapeuticClassificationからも薬効分類名を取得
        therapeutic_elements = self.root.findall('.//pmda:TherapeuticClassification/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        
        for element in therapeutic_elements:
            if element is not None and element.text:
                text = element.text.strip()
                indications.append({
                    'text': text,
                })
        
        # GenericNameは薬剤の一般名であり、効能・効果ではないため除外
        # （効能・効果は主にIndicationsOrEfficacyセクションに記載される）
        
        # 重複除去
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
        
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = IndicationParser(root)
        return parser.extract_indications()
    except Exception as e:
        print(f"Error parsing indications in {file_path}: {e}")
        return []