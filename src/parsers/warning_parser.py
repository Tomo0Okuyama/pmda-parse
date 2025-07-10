import xml.etree.ElementTree as ET
from typing import List, Dict
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE, remove_duplicates_by_key

class WarningParser:
    """
    医薬品の警告・注意事項をパースするクラス
    """
    def __init__(self, root: ET.Element):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
        """
        self.root = root
        self.namespace = PMDA_NAMESPACE

    def extract_warnings(self) -> List[Dict[str, str]]:
        """
        警告・注意事項を抽出する

        Returns:
            List[Dict[str, str]]: 警告・注意事項のリスト
        """
        warnings = []
        
        # Warningsタグから警告を抽出
        warnings_elements = self.root.findall('.//pmda:Warnings', namespaces=self.namespace)
        
        for warnings_element in warnings_elements:
            # 各Item要素から警告情報を取得
            item_elements = warnings_element.findall('.//pmda:Item', namespaces=self.namespace)
            
            for item in item_elements:
                # Headerタグは除外し、Detail/Langタグのみから日本語テキストを取得
                detail_elements = item.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for detail in detail_elements:
                    if detail.text and detail.text.strip():
                        text = detail.text.strip()
                        # 適切な警告文の判定
                        if text and len(text.strip()) > 0:
                            warnings.append({
                                'text': text,
                            })
        
        # ImportantPrecautionsタグから重要な基本的注意を抽出
        precautions_elements = self.root.findall('.//pmda:ImportantPrecautions', namespaces=self.namespace)
        
        for precautions_element in precautions_elements:
            # 各Item要素から注意事項を取得
            item_elements = precautions_element.findall('.//pmda:Item', namespaces=self.namespace)
            
            for item in item_elements:
                # Headerタグは除外し、Detail/Langタグのみから日本語テキストを取得
                detail_elements = item.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for detail in detail_elements:
                    if detail.text and detail.text.strip():
                        text = detail.text.strip()
                        if text and len(text.strip()) > 0:
                            warnings.append({
                                'text': text,
                            })
        
        # PrecautionsForApplicationタグから適用上の注意を抽出
        application_precautions_elements = self.root.findall('.//pmda:PrecautionsForApplication', namespaces=self.namespace)
        
        for application_element in application_precautions_elements:
            # OtherInformation要素から注意事項を取得
            other_info_elements = application_element.findall('.//pmda:OtherInformation', namespaces=self.namespace)
            
            for other_info in other_info_elements:
                # Detail/Langタグから日本語テキストを取得
                detail_elements = other_info.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for detail in detail_elements:
                    if detail.text and detail.text.strip():
                        text = detail.text.strip()
                        if text and len(text.strip()) > 0:
                            warnings.append({
                                'text': text,
                            })
        
        # PrecautionsForHandlingタグから取扱い上の注意を抽出
        handling_precautions_elements = self.root.findall('.//pmda:PrecautionsForHandling', namespaces=self.namespace)
        
        for handling_element in handling_precautions_elements:
            # Detail/Langタグから日本語テキストを取得
            detail_elements = handling_element.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            
            for detail in detail_elements:
                if detail.text and detail.text.strip():
                    text = detail.text.strip()
                    if text and len(text.strip()) > 0:
                        warnings.append({
                            'text': text,
                        })
        
        # UseInSpecificPopulationsタグから特定の背景を有する患者に関する注意を抽出
        specific_populations_elements = self.root.findall('.//pmda:UseInSpecificPopulations', namespaces=self.namespace)
        
        for specific_element in specific_populations_elements:
            # 各Item要素から注意事項を取得
            item_elements = specific_element.findall('.//pmda:Item', namespaces=self.namespace)
            
            for item in item_elements:
                # Headerタグは除外し、Detail/Langタグのみから日本語テキストを取得
                detail_elements = item.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for detail in detail_elements:
                    if detail.text and detail.text.strip():
                        text = detail.text.strip()
                        if text and len(text.strip()) > 0:
                            warnings.append({
                                'text': text,
                            })
        
        # 重複除去して返す
        return remove_duplicates_by_key(warnings, 'text')

def parse_warnings(file_path: str) -> List[Dict[str, str]]:
    """
    XMLファイルから警告・注意事項をパースする

    Args:
        file_path (str): パースするXMLファイルのパス

    Returns:
        List[Dict[str, str]]: 警告・注意事項のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルをパースして警告・注意事項を抽出
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = WarningParser(root)
        return parser.extract_warnings()
    except Exception as e:
        print(f"Error parsing warnings in {file_path}: {e}")
        return []