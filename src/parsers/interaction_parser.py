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
        
        # PrecautionsCombinationsタグから併用注意を抽出
        combination_elements = self.root.findall('.//pmda:PrecautionsCombinations', namespaces=self.namespace)
        
        for combination_element in combination_elements:
            # 各Drug要素から薬剤名と詳細情報を取得
            drug_elements = combination_element.findall('.//pmda:Drug', namespaces=self.namespace)
            
            for drug in drug_elements:
                # 薬剤名を取得
                drug_name_elements = drug.findall('.//pmda:DrugName/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for drug_name in drug_name_elements:
                    if drug_name.text and drug_name.text.strip():
                        interactions.append({
                            'text': drug_name.text.strip(),
                        })
                
                # 臨床症状・措置方法を取得
                symptoms_elements = drug.findall('.//pmda:ClinSymptomsAndMeasures/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for symptoms in symptoms_elements:
                    if symptoms.text and symptoms.text.strip():
                        interactions.append({
                            'text': f"臨床症状・措置: {symptoms.text.strip()}",
                        })
                
                # 機序・危険因子を取得
                mechanism_elements = drug.findall('.//pmda:MechanismAndRiskFactors/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for mechanism in mechanism_elements:
                    if mechanism.text and mechanism.text.strip():
                        interactions.append({
                            'text': f"機序・危険因子: {mechanism.text.strip()}",
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
        
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = InteractionParser(root)
        return parser.extract_interactions()
    except Exception as e:
        print(f"Error parsing interactions in {file_path}: {e}")
        return []