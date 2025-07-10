import xml.etree.ElementTree as ET
from typing import List, Dict
from parsers.xml_utils import (
    register_xml_namespaces, 
    PMDA_NAMESPACE, 
    extract_clean_text, 
    remove_duplicates_by_key
)

class ActiveIngredientParser:
    """
    医薬品の有効成分情報をパースするクラス
    """
    def __init__(self, root: ET.Element, brand_id: str | None = None):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
            brand_id (str): 特定のブランドID（BRD_Drug1など）
        """
        self.root = root
        self.brand_id = brand_id
        self.namespace = PMDA_NAMESPACE
    

    def extract_active_ingredients(self) -> List[Dict[str, str]]:
        """
        有効成分情報を抽出する

        Returns:
            List[Dict[str, str]]: 有効成分情報のリスト
        """
        active_ingredients = []
        
        # PhyschemOfActIngredientsセクションから詳細な有効成分情報を抽出
        physchem_sections = self.root.findall('.//pmda:PhyschemOfActIngredientsSection', namespaces=self.namespace)
        
        for section in physchem_sections:
            ingredient_info = {}
            
            # GeneralName（一般名）を取得
            general_name_elem = section.find('.//pmda:GeneralName/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if general_name_elem is not None:
                general_name_text = extract_clean_text(general_name_elem)
                if general_name_text:
                    ingredient_info['general_name'] = general_name_text
            
            # ChemicalName（化学名）を取得
            chemical_name_elem = section.find('.//pmda:ChemicalName/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if chemical_name_elem is not None:
                chemical_name_text = extract_clean_text(chemical_name_elem)
                if chemical_name_text:
                    ingredient_info['chemical_name'] = chemical_name_text
            
            # MolecularFormula（分子式）を取得
            molecular_formula_elem = section.find('.//pmda:MolecularFormula/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if molecular_formula_elem is not None:
                molecular_formula_text = extract_clean_text(molecular_formula_elem)
                if molecular_formula_text:
                    ingredient_info['molecular_formula'] = molecular_formula_text
            
            # MolecularWeight（分子量）を取得
            molecular_weight_elem = section.find('.//pmda:MolecularWeight/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if molecular_weight_elem is not None:
                molecular_weight_text = extract_clean_text(molecular_weight_elem)
                if molecular_weight_text:
                    ingredient_info['molecular_weight'] = molecular_weight_text
            
            # Nature（性状）を取得
            nature_elem = section.find('.//pmda:Nature/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if nature_elem is not None:
                nature_text = extract_clean_text(nature_elem)
                if nature_text:
                    ingredient_info['nature'] = nature_text
            
            # DescriptionOfActiveIngredients（有効成分の説明）を取得
            description_elem = section.find('.//pmda:DescriptionOfActiveIngredients/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if description_elem is not None:
                description_text = extract_clean_text(description_elem)
                if description_text:
                    ingredient_info['description'] = description_text
            
            # Solubility（溶解性）を取得
            solubility_elem = section.find('.//pmda:Solubility/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if solubility_elem is not None:
                solubility_text = extract_clean_text(solubility_elem)
                if solubility_text:
                    ingredient_info['solubility'] = solubility_text
            
            # DistributionCoefficient（分配係数）を取得
            dist_coeff_elem = section.find('.//pmda:DistributionCoefficient/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if dist_coeff_elem is not None:
                dist_coeff_text = extract_clean_text(dist_coeff_elem)
                if dist_coeff_text:
                    ingredient_info['distribution_coefficient'] = dist_coeff_text
            
            # pKa（酸解離定数）を取得
            pka_elem = section.find('.//pmda:pKa/pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if pka_elem is not None:
                pka_text = extract_clean_text(pka_elem)
                if pka_text:
                    ingredient_info['pka'] = pka_text
            
            
            # 有効成分情報が含まれている場合のみ追加
            if ingredient_info:
                active_ingredients.append(ingredient_info)
        
        # CompositionAndPropertyセクションからActiveIngredientNameを取得
        if self.brand_id:
            # 特定のブランドIDに対応するCompositionForBrandを検索
            composition_sections = self.root.findall(f'.//pmda:CompositionForBrand[@ref="{self.brand_id}"]//pmda:CompositionTable', namespaces=self.namespace)
        else:
            # 全てのCompositionTableを検索
            composition_sections = self.root.findall('.//pmda:CompositionAndProperty//pmda:CompositionTable', namespaces=self.namespace)
        
        for section in composition_sections:
            # ActiveIngredientNameとその含量情報を取得
            ingredient_names = section.findall('.//pmda:ActiveIngredientName/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            value_units = section.findall('.//pmda:ValueAndUnit/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            
            for i, ingredient_name in enumerate(ingredient_names):
                if ingredient_name.text:
                    name_text = ingredient_name.text
                    if name_text is not None and name_text.strip():
                        ingredient_info = {
                            'ingredient_name': name_text.strip()
                        }
                        
                        # 対応する含量情報があれば追加
                        if i < len(value_units) and value_units[i].text:
                            content_text = value_units[i].text
                            if content_text is not None and content_text.strip():
                                ingredient_info['content_amount'] = content_text.strip()
                        
                        # 重複チェック（既存の有効成分情報と同じ名前でないか確認）
                        if not any(ai.get('ingredient_name') == ingredient_info['ingredient_name'] for ai in active_ingredients):
                            active_ingredients.append(ingredient_info)
        
        return active_ingredients


def parse_active_ingredients(file_path: str, brand_id: str | None = None) -> List[Dict[str, str]]:
    """
    XMLファイルから有効成分情報をパースする

    Args:
        file_path (str): パースするXMLファイルのパス
        brand_id (str): 特定のブランドID（BRD_Drug1など）

    Returns:
        List[Dict[str, str]]: 有効成分情報のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルをパースして有効成分情報を抽出
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = ActiveIngredientParser(root, brand_id)
        
        return parser.extract_active_ingredients()
    except Exception:
        return []