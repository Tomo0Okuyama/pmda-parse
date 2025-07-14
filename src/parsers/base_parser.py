import os
import xml.etree.ElementTree as ET
import json
from typing import Dict, List, Optional, Union

class MedicineParser:
    """
    医薬品情報をXMLからパースするための基本クラス
    """
    def __init__(self, file_path: str):
        """
        初期化メソッド

        Args:
            file_path (str): パースするXMLファイルのパス
        """
        self.file_path = file_path
        self.tree = ET.parse(file_path)
        self.root = self.tree.getroot()
        
        # XML名前空間の定義
        self.namespace = {
            'pmda': 'http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0',
            'xml': 'http://www.w3.org/XML/1998/namespace'
        }

    def _safe_find_text(self, xpath: str, root: Optional[ET.Element] = None) -> Optional[str]:
        """
        XPathで要素を検索し、テキストを安全に取得する

        Args:
            xpath (str): 検索するXPath
            root (ET.Element, optional): 検索を開始するルート要素。デフォルトはself.root

        Returns:
            Optional[str]: 要素のテキスト。見つからない場合はNone
        """
        if root is None:
            root = self.root

        element = root.find(xpath, namespaces=self.namespace)
        return element.text.strip() if element is not None and element.text else None

    def _extract_formatted_text(self, xpath: str, root: Optional[ET.Element] = None) -> Optional[str]:
        """
        XMLフォーマットタグ（Sub, Sup, Italic等）を含むテキストを抽出する
        
        Args:
            xpath (str): 検索するXPath
            root (ET.Element, optional): 検索を開始するルート要素。デフォルトはself.root
            
        Returns:
            Optional[str]: フォーマットタグを処理したテキスト。見つからない場合はNone
        """
        if root is None:
            root = self.root
            
        try:
            found_element = root.find(xpath, namespaces=self.namespace)
            if found_element is None:
                return None
            
            # 要素内の全テキスト（子要素のテキストも含む）を再帰的に取得
            def extract_all_text(elem):
                text = elem.text or ''
                for child in elem:
                    if child.tag.endswith('Sub'):
                        # 下付き文字：そのままテキストとして追加
                        text += child.text or ''
                    elif child.tag.endswith('Sup'):
                        # 上付き文字：そのままテキストとして追加
                        text += child.text or ''
                    elif child.tag.endswith('Italic'):
                        # イタリック：そのままテキストとして追加
                        text += child.text or ''
                    else:
                        # その他のタグ：再帰的にテキストを取得
                        text += extract_all_text(child)
                    # 子要素の後のテキスト（tail）も追加
                    text += child.tail or ''
                return text
            
            result = extract_all_text(found_element)
            return result.strip() if result else None
        except Exception:
            return None

    def extract_product_id(self) -> Optional[str]:
        """
        製品IDを抽出する

        Returns:
            Optional[str]: 製品ID。見つからない場合はNone
        """
        return self._safe_find_text('.//pmda:PackageInsertNo', self.root)

    def extract_product_name(self) -> Optional[str]:
        """
        製品名称を抽出する（最初のDetailBrandNameから）

        Returns:
            Optional[str]: 製品名称。見つからない場合はNone
        """
        return self._safe_find_text('.//pmda:DetailBrandName[1]/pmda:ApprovalBrandName/pmda:Lang[@xml:lang="ja"]', self.root)

    def extract_yj_code(self) -> Optional[str]:
        """
        YJコードを抽出する（最初のDetailBrandNameから）

        Returns:
            Optional[str]: YJコード。見つからない場合はNone
        """
        return self._safe_find_text('.//pmda:DetailBrandName[1]/pmda:BrandCode/pmda:YJCode', self.root)
    
    def extract_all_brands(self) -> List[Dict[str, str]]:
        """
        全てのDetailBrandNameから製品名とYJコードを抽出する

        Returns:
            List[Dict[str, str]]: 各医薬品の製品名とYJコードのリスト
        """
        brands = []
        brand_elements = self.root.findall('.//pmda:DetailBrandName', self.namespace)
        
        for brand_element in brand_elements:
            product_name = self._safe_find_text('./pmda:ApprovalBrandName/pmda:Lang[@xml:lang="ja"]', brand_element)
            yj_code = self._safe_find_text('./pmda:BrandCode/pmda:YJCode', brand_element)
            
            if product_name or yj_code:
                brands.append({
                    'product_name': product_name or '',
                    'yj_code': yj_code or ''
                })
        
        return brands

    def extract_form(self) -> Optional[str]:
        """
        剤形を抽出する（剤形と色調を組み合わせ）

        Returns:
            Optional[str]: 剤形。見つからない場合はNone
        """
        # PropertyTableからFormulationとColorToneを組み合わせて取得
        property_tables = self.root.findall('.//pmda:Property//pmda:PropertyTable', self.namespace)
        for property_table in property_tables:
            formulation = self._safe_find_text('./pmda:Formulation/pmda:Lang[@xml:lang="ja"]', property_table)
            color_tone = self._safe_find_text('./pmda:ColorTone/pmda:Lang[@xml:lang="ja"]', property_table)
            
            # 剤形と色調の組み合わせが最も詳細な情報
            if formulation and color_tone:
                return f"{formulation}:{color_tone}"
            elif formulation:
                return formulation
            elif color_tone:
                return color_tone
        
        # PropertyForConstituentUnitsから外観・性状情報を取得
        constituent_units = self.root.findall('.//pmda:PropertyForConstituentUnits', self.namespace)
        for unit in constituent_units:
            # OtherPropertyから外観・性状を検索
            other_properties = unit.findall('.//pmda:OtherProperty', self.namespace)
            for other_prop in other_properties:
                category = self._safe_find_text('./pmda:CategoryName/pmda:Lang[@xml:lang="ja"]', other_prop)
                if category and ('外観' in category or '性状' in category):
                    content_detail = self._safe_find_text('./pmda:Content/pmda:ContentDetail/pmda:Lang[@xml:lang="ja"]', other_prop)
                    if content_detail:
                        return content_detail
        
        # PropertyTableのOtherPropertyから色・剤形情報を取得（フォールバック）
        other_properties = self.root.findall('.//pmda:Property//pmda:PropertyTable/pmda:OtherProperty', self.namespace)
        for other_prop in other_properties:
            category = self._safe_find_text('./pmda:CategoryName/pmda:Lang[@xml:lang="ja"]', other_prop)
            if category and '剤形' in category:
                content_detail = self._safe_find_text('./pmda:Content/pmda:ContentDetail/pmda:Lang[@xml:lang="ja"]', other_prop)
                if content_detail:
                    return content_detail
        
        # DosageFormから剤形を取得（代替）
        dosage_form = self._safe_find_text('.//pmda:DetailBrandName[1]/pmda:DosageForm/pmda:Lang[@xml:lang="ja"]', self.root)
        if dosage_form:
            return dosage_form
            
        # TherapeuticClassificationから薬効分類を取得（最後の手段）
        return self._safe_find_text('.//pmda:TherapeuticClassification/pmda:Detail/pmda:Lang[@xml:lang="ja"]', self.root)

    def extract_therapeutic_classification(self) -> Optional[str]:
        """
        薬効分類名を抽出する

        Returns:
            Optional[str]: 薬効分類名。見つからない場合はNone
        """
        return self._extract_formatted_text('.//pmda:TherapeuticClassification/pmda:Detail/pmda:Lang[@xml:lang="ja"]', self.root)

    def extract_manufacturer_code(self) -> Optional[str]:
        """
        製造元企業コードを抽出する

        Returns:
            Optional[str]: 製造元企業コード。見つからない場合はNone
        """
        return self._safe_find_text('.//pmda:CompanyIdentifier', self.root)
    
    def extract_manufacturer_name(self) -> Optional[str]:
        """
        製造元企業名を抽出する（企業名のみ）

        Returns:
            Optional[str]: 製造元企業名。見つからない場合はNone
        """
        # NameAddressManufact セクションから製造元情報を取得
        manufacturers = self.root.findall('.//pmda:NameAddressManufact/pmda:Manufacturer', self.namespace)
        
        if not manufacturers:
            return None
        
        # 複数の製造元がある場合、製造販売元または製造販売を優先
        primary_manufacturer = None
        fallback_manufacturer = None
        
        for manufacturer in manufacturers:
            # 企業名を取得（役割プレフィックスを除去して企業名のみ）
            name_element = manufacturer.find('.//pmda:Name/pmda:Lang[@xml:lang="ja"]', self.namespace)
            name = name_element.text.strip() if name_element is not None and name_element.text else ""
            
            if name:
                # XML構造での順序を優先（最初に見つかった製造元を使用）
                if primary_manufacturer is None:
                    primary_manufacturer = name
                    break
        
        return primary_manufacturer or fallback_manufacturer
    
    def extract_manufacturer(self) -> Optional[str]:
        """
        製造元を抽出する（後方互換性のため、企業コードを返す）

        Returns:
            Optional[str]: 製造元企業コード。見つからない場合はNone
        """
        return self.extract_manufacturer_code()

    def extract_vectors(self) -> List[Dict[str, str]]:
        """
        医薬品の各ベクター（効能・効果、用法・用量など）を抽出する
        
        Note:
            現在は基本実装のみ。将来的にベクトル検索機能が必要になった際に拡張予定。

        Returns:
            List[Dict[str, str]]: ベクターのリスト（現在は空リスト）
        """
        return []

    def to_json(self) -> Dict[str, Union[str, List[Dict[str, str]]]]:
        """
        医薬品情報をJSONに変換する

        Returns:
            Dict[str, Union[str, List[Dict[str, str]]]]: JSONフォーマットの医薬品情報
        """
        return {
            'yj_code': self.extract_yj_code() or '',
            'therapeutic_classification': self.extract_therapeutic_classification() or '',
            'product_name': self.extract_product_name() or '',
            'form': self.extract_form() or '',
            'manufacturer_code': self.extract_manufacturer_code() or '',
            'manufacturer_name': self.extract_manufacturer_name() or '',
            'vectors': self.extract_vectors()
        }

def parse_medicine_files(directory: str, output_file: str):
    """
    指定されたディレクトリ内のXMLファイルを全てパースし、JSONに出力する

    Args:
        directory (str): パースするXMLファイルが存在するディレクトリ
        output_file (str): 出力するJSONファイルのパス
    """
    all_medicines = []

    # ディレクトリ内のXMLファイルを再帰的に検索
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(('.xml', '.sgml')):
                filepath = os.path.join(root, filename)
                try:
                    parser = MedicineParser(filepath)
                    medicine_data = parser.to_json()
                    all_medicines.append(medicine_data)
                except Exception:
                    pass

    # 結果をJSONファイルに出力
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_medicines, f, ensure_ascii=False, indent=2)

    pass