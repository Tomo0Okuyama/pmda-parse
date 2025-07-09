import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE, extract_condition_header, remove_duplicates_by_key

class SideEffectParser:
    """
    医薬品の副作用をパースするクラス
    """
    def __init__(self, root: ET.Element):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
        """
        self.root = root
        self.namespace = PMDA_NAMESPACE

    def _extract_condition_header(self, element: ET.Element) -> str:
        """
        要素から条件ヘッダー（副作用カテゴリなど）を抽出する
        
        Args:
            element: 対象のXML要素
            
        Returns:
            str: 条件ヘッダー文字列、見つからない場合は空文字列
        """
        # 直接の子要素のHeaderタグ内のLang要素を検索（ネストした要素は除外）
        header_elements = element.findall('./pmda:Header/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        
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

    def _format_side_effect_with_condition(self, text: str, condition_header: str, severity: str = "") -> str:
        """
        副作用テキストに条件ヘッダーと重篤度を付与する
        
        Args:
            text (str): 元の副作用テキスト
            condition_header (str): 条件ヘッダー（Header要素から抽出）
            severity (str): 重篤度（重篤/非重篤）
            
        Returns:
            str: 条件付きのテキスト（重篤度を先頭に配置）
        """
        # 重篤度情報を付与（重篤度を先頭に配置）
        if severity:
            if condition_header and condition_header.strip():
                return f"{severity}:{condition_header}:{text}"
            else:
                return f"{severity}:{text}"
        
        # 条件ヘッダーがある場合はそれを使用
        if condition_header and condition_header.strip():
            return f"{condition_header}:{text}"
        
        # 条件ヘッダーがない場合はそのまま返す
        return text

    def _extract_side_effect_name_and_description(self, header_text: str, detail_text: str) -> Tuple[str, str]:
        """
        副作用のヘッダーと詳細から副作用名と説明を分離する
        
        Args:
            header_text (str): ヘッダーテキスト（副作用名）
            detail_text (str): 詳細テキスト（説明）
            
        Returns:
            tuple: (副作用名, 説明)
        """
        # ヘッダーに副作用名があり、詳細に説明がある場合
        if header_text and detail_text:
            return header_text.strip(), detail_text.strip()
        
        # ヘッダーのみがある場合（詳細なし）
        if header_text and not detail_text:
            return header_text.strip(), ""
        
        # 詳細のみがある場合（ヘッダーなし）
        if not header_text and detail_text:
            return "", detail_text.strip()
        
        return "", ""

    def _process_nested_items(self, item_element, parent_condition: str = "", severity: str = "") -> List[Dict[str, str]]:
        """
        ネストしたItem要素を再帰的に処理する
        
        Args:
            item_element: Item要素
            parent_condition: 親階層の条件ヘッダー
            severity: 重篤度（重篤/非重篤）
            
        Returns:
            List[Dict[str, str]]: 副作用のリスト
        """
        side_effects = []
        
        # 現在のItemの条件ヘッダーを取得
        current_condition = extract_condition_header(item_element, self.namespace)
        
        # 親条件と結合
        if parent_condition and current_condition:
            combined_condition = f"{parent_condition}:{current_condition}"
        elif parent_condition:
            combined_condition = parent_condition
        elif current_condition:
            combined_condition = current_condition
        else:
            combined_condition = ""
        
        # Header要素から副作用名を取得
        header_elements = item_element.findall('./pmda:Header/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        header_text = ""
        if header_elements:
            header_text = "".join(header_elements[0].itertext()).strip()
        
        # 直接のDetail要素があるかチェック
        direct_detail_elements = item_element.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        
        for detail in direct_detail_elements:
            if detail.text and detail.text.strip():
                detail_text = detail.text.strip()
                
                # 副作用名と説明を分離
                side_effect_name, description = self._extract_side_effect_name_and_description(header_text, detail_text)
                
                # 重篤な副作用の場合、副作用名と説明を分けて格納
                if severity == "重篤" and side_effect_name and description:
                    formatted_text = f"重篤:{side_effect_name}:{description}"
                elif severity == "重篤" and side_effect_name:
                    # 説明がない場合は副作用名のみ
                    formatted_text = f"重篤:{side_effect_name}"
                else:
                    # その他の副作用または従来の形式
                    if header_text and detail_text:
                        formatted_text = self._format_side_effect_with_condition(f"{header_text}:{detail_text}", combined_condition, severity)
                    else:
                        formatted_text = self._format_side_effect_with_condition(detail_text, combined_condition, severity)
                
                side_effects.append({
                    'text': formatted_text,
                })
        
        # HeaderのみでDetailがない場合の処理
        if header_text and not direct_detail_elements:
            if severity == "重篤":
                formatted_text = f"重篤:{header_text}"
            else:
                formatted_text = self._format_side_effect_with_condition(header_text, combined_condition, severity)
            
            side_effects.append({
                'text': formatted_text,
            })
        
        # ネストしたSimpleList/Item要素を処理
        nested_items = item_element.findall('./pmda:SimpleList/pmda:Item', namespaces=self.namespace)
        for nested_item in nested_items:
            side_effects.extend(self._process_nested_items(nested_item, combined_condition, severity))
        
        return side_effects

    def extract_side_effects(self) -> List[Dict[str, str]]:
        """
        副作用情報を抽出し、ネストしたHeader構造を適切に処理する

        Returns:
            List[Dict[str, str]]: 副作用情報のリスト
        """
        side_effects = []
        
        # AdverseEventsタグから副作用を抽出
        adverse_events_elements = self.root.findall('.//pmda:AdverseEvents', namespaces=self.namespace)
        
        for adverse_events_element in adverse_events_elements:
            # SeriousAdverseEventsとOtherAdverseEventsから副作用を抽出
            serious_adverse_elements = adverse_events_element.findall('.//pmda:SeriousAdverseEvents', namespaces=self.namespace)
            other_adverse_elements = adverse_events_element.findall('.//pmda:OtherAdverseEvents', namespaces=self.namespace)
            
            # 重大な副作用の処理
            for serious_adverse in serious_adverse_elements:
                item_elements = serious_adverse.findall('.//pmda:Item', namespaces=self.namespace)
                
                for item in item_elements:
                    # ネストしたItem構造を再帰的に処理（重篤度：重篤）
                    nested_side_effects = self._process_nested_items(item, "", "重篤")
                    
                    # 重複チェックをして追加
                    for side_effect in nested_side_effects:
                        if not any(existing['text'] == side_effect['text'] for existing in side_effects):
                            side_effects.append(side_effect)
            
            # その他の副作用の処理
            for other_adverse in other_adverse_elements:
                # Instructions内のItem要素から条件を取得
                instruction_items = other_adverse.findall('.//pmda:Instructions/pmda:SimpleList/pmda:Item', namespaces=self.namespace)
                
                for instruction_item in instruction_items:
                    # 条件ヘッダーを取得（〈高血圧症〉など）
                    condition_header = extract_condition_header(instruction_item, self.namespace)
                    
                    # その下のAdverseReactionDescription要素から副作用情報を取得
                    adverse_reaction_elements = other_adverse.findall('.//pmda:AdverseReactionDescription', namespaces=self.namespace)
                    
                    for reaction in adverse_reaction_elements:
                        detail_elements = reaction.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                        
                        for detail in detail_elements:
                            if detail.text and detail.text.strip():
                                text = detail.text.strip()
                                formatted_text = self._format_side_effect_with_condition(text, condition_header, "非重篤")
                                
                                # 重複チェックをして追加
                                if not any(existing['text'] == formatted_text for existing in side_effects):
                                    side_effects.append({
                                        'text': formatted_text,
                                    })
                
                # OtherAdverse内のItem要素も処理
                other_item_elements = other_adverse.findall('.//pmda:OtherAdverse//pmda:Item', namespaces=self.namespace)
                
                for item in other_item_elements:
                    # ネストしたItem構造を再帰的に処理（重篤度：非重篤）
                    nested_side_effects = self._process_nested_items(item, "", "非重篤")
                    
                    # 重複チェックをして追加
                    for side_effect in nested_side_effects:
                        if not any(existing['text'] == side_effect['text'] for existing in side_effects):
                            side_effects.append(side_effect)
        
        # 構造化された副作用情報の抽出が完了
        
        return side_effects

def parse_side_effects(file_path: str) -> List[Dict[str, str]]:
    """
    XMLファイルから副作用情報をパースする

    Args:
        file_path (str): パースするXMLファイルのパス

    Returns:
        List[Dict[str, str]]: 副作用情報のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = SideEffectParser(root)
        return parser.extract_side_effects()
    except Exception as e:
        print(f"Error parsing side effects in {file_path}: {e}")
        return []