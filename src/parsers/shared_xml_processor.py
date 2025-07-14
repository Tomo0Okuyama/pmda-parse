import xml.etree.ElementTree as ET
import os
from typing import Dict, List, Any, Optional
from .xml_utils import register_xml_namespaces, PMDA_NAMESPACE
from .indication_parser import IndicationParser
from .dosage_parser import DosageParser
from .contraindication_parser import ContraindicationParser
from .warning_parser import WarningParser
from .side_effect_parser import SideEffectParser
from .interaction_parser import InteractionParser
from .base_parser import MedicineParser

class SharedXMLProcessor:
    """
    XMLファイルを一度だけパースして、全ての医療情報を効率的に抽出するクラス
    従来の複数パーサーによる重複XMLパースを解決し、大幅な高速化を実現
    """
    
    def __init__(self, file_path: str):
        """
        初期化メソッド - XMLファイルを一度だけパースして共有
        
        Args:
            file_path (str): パースするXMLファイルのパス
        """
        self.file_path = file_path
        
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルを一度だけパースして共有
        self.tree = ET.parse(file_path)
        self.root = self.tree.getroot()
        self.namespace = PMDA_NAMESPACE
        
        # 各パーサーを初期化（XMLツリーを共有）
        self.base_parser = MedicineParser(file_path)
        self.indication_parser = IndicationParser(self.root)
        self.dosage_parser = DosageParser(self.root, file_path)
        self.contraindication_parser = ContraindicationParser(self.root)
        self.warning_parser = WarningParser(self.root)
        self.side_effect_parser = SideEffectParser(self.root)
        self.interaction_parser = InteractionParser(self.root)
    
    def extract_basic_info(self) -> Dict[str, Any]:
        """
        基本医薬品情報を抽出する
        
        Returns:
            Dict[str, Any]: 基本医薬品情報
        """
        return {
            'therapeutic_classification': self.base_parser.extract_therapeutic_classification() or '',
            'form': self.base_parser.extract_form() or '',
            'manufacturer_code': self.base_parser.extract_manufacturer_code() or '',
            'manufacturer_name': self.base_parser.extract_manufacturer_name() or '',
            'source_filename': os.path.basename(self.file_path)
        }
    
    def extract_all_brands(self) -> List[Dict[str, str]]:
        """
        全ての医薬品ブランド情報を抽出する
        
        Returns:
            List[Dict[str, str]]: 各医薬品の製品名とYJコードのリスト
        """
        return self.base_parser.extract_all_brands()
    
    def extract_clinical_info(self, brand_info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        全ての臨床情報を一度に抽出する（最適化版）
        
        Args:
            brand_info (Dict[str, str], optional): ブランド情報（product_name, yj_code）
            
        Returns:
            Dict[str, Any]: 全ての臨床情報
        """
        clinical_info = {}
        
        # 効能・効果を抽出
        indications = self.indication_parser.extract_indications()
        if indications:
            clinical_info['indications'] = [item['text'] for item in indications if item['text']]
        
        # 用法・用量を抽出
        dosages = self.dosage_parser.extract_dosages()
        if dosages:
            clinical_info['dosage'] = [item['text'] for item in dosages if item['text']]
        
        # 禁忌を抽出
        contraindications = self.contraindication_parser.extract_contraindications()
        if contraindications:
            clinical_info['contraindications'] = [item['text'] for item in contraindications if item['text']]
        
        # 警告・注意事項を抽出
        warnings = self.warning_parser.extract_warnings()
        if warnings:
            clinical_info['warnings'] = [item['text'] for item in warnings if item['text']]
        
        # 副作用を抽出
        side_effects = self.side_effect_parser.extract_side_effects()
        if side_effects:
            clinical_info['side_effects'] = [item['text'] for item in side_effects if item['text']]
        
        # 相互作用を抽出
        interactions = self.interaction_parser.extract_interactions()
        if interactions:
            clinical_info['interactions'] = [item['text'] for item in interactions if item['text']]
        
        # BRD_DrugのIDを推定（複数医薬品対応）
        brand_id = self._extract_brand_id(brand_info)
        
        # 成分・含量を抽出
        from .composition_parser import parse_compositions
        compositions = parse_compositions(self.file_path, brand_id)
        if compositions:
            clinical_info['compositions'] = [item['text'] for item in compositions if item['text']]
        
        # 有効成分詳細情報を抽出
        from .active_ingredient_parser import parse_active_ingredients
        active_ingredients = parse_active_ingredients(self.file_path, brand_id)
        if active_ingredients:
            # 物理化学的情報のみを抽出（組成データと重複するため）
            filtered_ingredients = []
            for ingredient in active_ingredients:
                filtered_ingredient = {k: v for k, v in ingredient.items() 
                                     if k not in ['ingredient_name', 'content_amount']}
                if any(filtered_ingredient.values()):
                    filtered_ingredients.append(filtered_ingredient)
            
            if filtered_ingredients:
                clinical_info['active_ingredients'] = filtered_ingredients
        
        return clinical_info
    
    def _extract_brand_id(self, brand_info: Optional[Dict[str, str]]) -> Optional[str]:
        """
        YJコードからBRD_DrugのIDを推定する
        
        Args:
            brand_info (Dict[str, str], optional): ブランド情報
            
        Returns:
            Optional[str]: BRD_DrugのID
        """
        if not brand_info or not brand_info.get('yj_code'):
            return None
        
        try:
            # DetailBrandNameからYJコードに対応するIDを検索
            brand_elements = self.root.findall('.//pmda:DetailBrandName', self.namespace)
            for brand_element in brand_elements:
                yj_element = brand_element.find('.//pmda:YJCode', self.namespace)
                if yj_element is not None and yj_element.text == brand_info['yj_code']:
                    return brand_element.get('id')
        except Exception:
            pass
        
        return None
    
    def process_all_brands(self) -> List[Dict[str, Any]]:
        """
        全ての医薬品ブランドを処理して完全な医薬品データを返す
        
        Returns:
            List[Dict[str, Any]]: 処理された医薬品データのリスト
        """
        # 基本情報を取得
        basic_info = self.extract_basic_info()
        
        # 全ての医薬品ブランド情報を取得
        all_brands = self.extract_all_brands()
        
        if not all_brands:
            # フォールバック：従来の方法で単一医薬品として処理
            medicine_data = self.base_parser.to_json()
            medicine_data['source_filename'] = basic_info['source_filename']
            
            # 臨床情報を追加
            clinical_info = self.extract_clinical_info()
            medicine_data['clinical_info'] = clinical_info
            
            return [medicine_data]
        
        medicines_list = []
        for brand in all_brands:
            # 各医薬品に対してJSONエントリを作成
            medicine_data = {
                'yj_code': brand['yj_code'],
                'therapeutic_classification': basic_info['therapeutic_classification'],
                'product_name': brand['product_name'],
                'form': basic_info['form'],
                'manufacturer_code': basic_info['manufacturer_code'],
                'manufacturer_name': basic_info['manufacturer_name'],
                'source_filename': basic_info['source_filename']
            }
            
            # 臨床情報を追加（brand情報を渡す）
            clinical_info = self.extract_clinical_info(brand)
            medicine_data['clinical_info'] = clinical_info
            
            medicines_list.append(medicine_data)
        
        return medicines_list