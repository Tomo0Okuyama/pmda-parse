import xml.etree.ElementTree as ET
from typing import List, Dict
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE, remove_duplicates_by_key

class ContraindicationParser:
    """
    医薬品の禁忌をパースするクラス
    """
    def __init__(self, root: ET.Element):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
        """
        self.root = root
        self.namespace = PMDA_NAMESPACE

    def extract_contraindications(self) -> List[Dict[str, str]]:
        """
        禁忌情報を抽出する

        Returns:
            List[Dict[str, str]]: 禁忌情報のリスト
        """
        contraindications = []
        
        # ContraIndicationsタグから一般的な禁忌を抽出
        contraindications_elements = self.root.findall('.//pmda:ContraIndications', namespaces=self.namespace)
        
        for contraindications_element in contraindications_elements:
            # まず直下のDetail要素から情報を取得（Item要素がない場合）
            direct_detail_elements = contraindications_element.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            
            for lang in direct_detail_elements:
                if lang.text and lang.text.strip():
                    contraindications.append({
                        'text': lang.text.strip(),
                    })
            
            # 次にItem要素から禁忌情報を取得
            item_elements = contraindications_element.findall('.//pmda:Item', namespaces=self.namespace)
            
            for item in item_elements:
                # Detail/Langタグから日本語テキストを取得
                lang_elements = item.findall('.//pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for lang in lang_elements:
                    if lang.text and lang.text.strip():
                        contraindications.append({
                            'text': lang.text.strip(),
                        })
        
        # ContraIndicatedCombinationsタグから併用禁忌を抽出
        combination_elements = self.root.findall('.//pmda:ContraIndicatedCombinations', namespaces=self.namespace)
        
        for combination_element in combination_elements:
            # 各Drug要素から薬剤名と詳細情報を取得
            drug_elements = combination_element.findall('.//pmda:Drug', namespaces=self.namespace)
            
            for drug in drug_elements:
                # 薬剤名を取得（併用禁忌の対象薬剤）
                drug_name_elements = drug.findall('.//pmda:DrugName/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for drug_name in drug_name_elements:
                    if drug_name.text and drug_name.text.strip():
                        contraindications.append({
                            'text': drug_name.text.strip(),
                        })
                
                # 臨床症状・措置方法を取得（併用時の問題）
                symptoms_elements = drug.findall('.//pmda:ClinSymptomsAndMeasures/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for symptoms in symptoms_elements:
                    if symptoms.text and symptoms.text.strip():
                        contraindications.append({
                            'text': symptoms.text.strip(),
                        })
                
                # 機序・危険因子を取得（併用禁忌の理由）
                mechanism_elements = drug.findall('.//pmda:MechanismAndRiskFactors/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                for mechanism in mechanism_elements:
                    if mechanism.text and mechanism.text.strip():
                        contraindications.append({
                            'text': mechanism.text.strip(),
                        })
        
        # 重複除去して返す
        return remove_duplicates_by_key(contraindications, 'text')

def parse_contraindications(file_path: str) -> List[Dict[str, str]]:
    """
    XMLファイルから禁忌情報をパースする

    Args:
        file_path (str): パースするXMLファイルのパス

    Returns:
        List[Dict[str, str]]: 禁忌情報のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルをパースして禁忌情報を抽出
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = ContraindicationParser(root)
        return parser.extract_contraindications()
    except Exception:
        return []