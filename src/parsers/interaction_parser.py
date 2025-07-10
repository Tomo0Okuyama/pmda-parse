import xml.etree.ElementTree as ET
from typing import List, Dict
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE, remove_duplicates_by_key

class InteractionParser:
    """
    医薬品の相互作用をパースするクラス
    """
    def __init__(self, root: ET.Element):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
        """
        self.root = root
        self.namespace = PMDA_NAMESPACE

    def extract_interactions(self) -> List[Dict[str, str]]:
        """
        相互作用情報を抽出する

        Returns:
            List[Dict[str, str]]: 相互作用情報のリスト
        """
        interactions = []
        
        # PrecautionsForCombinationsタグから併用注意を抽出
        combination_elements = self.root.findall('.//pmda:PrecautionsForCombinations', namespaces=self.namespace)
        
        for combination_element in combination_elements:
            # PrecautionsForCombination内の各Drug要素から薬剤名と詳細情報を取得
            drug_elements = combination_element.findall('.//pmda:PrecautionsForCombination//pmda:Drug', namespaces=self.namespace)
            
            for drug in drug_elements:
                # 薬剤名を取得
                drug_name_element = drug.find('.//pmda:DrugName/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                drug_name = drug_name_element.text.strip() if drug_name_element is not None and drug_name_element.text else ""
                
                # 機序・危険因子を取得
                mechanism_element = drug.find('.//pmda:MechanismAndRiskFactors/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                mechanism = mechanism_element.text.strip() if mechanism_element is not None and mechanism_element.text else ""
                
                # 薬剤名と機序を組み合わせた相互作用情報を作成
                if drug_name:
                    if mechanism:
                        interaction_text = f"薬物:{drug_name} - 機序:{mechanism}"
                    else:
                        interaction_text = f"薬物:{drug_name}"
                    
                    interactions.append({
                        'text': interaction_text,
                    })
        
        # DrugInteractionsタグから相互作用を抽出
        interaction_elements = self.root.findall('.//pmda:DrugInteractions', namespaces=self.namespace)
        
        for interaction_element in interaction_elements:
            # 各Item要素から相互作用情報を取得
            item_elements = interaction_element.findall('.//pmda:Item', namespaces=self.namespace)
            
            for item in item_elements:
                # Detail/Langタグから日本語テキストを取得
                lang_elements = item.findall('.//pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for lang in lang_elements:
                    if lang.text and lang.text.strip():
                        interactions.append({
                            'text': lang.text.strip(),
                        })
        
        # 重複除去して返す
        return remove_duplicates_by_key(interactions, 'text')

def parse_interactions(file_path: str) -> List[Dict[str, str]]:
    """
    XMLファイルから相互作用情報をパースする

    Args:
        file_path (str): パースするXMLファイルのパス

    Returns:
        List[Dict[str, str]]: 相互作用情報のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルをパースして相互作用情報を抽出
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = InteractionParser(root)
        return parser.extract_interactions()
    except Exception:
        return []