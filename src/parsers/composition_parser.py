import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE

class CompositionParser:
    """
    医薬品の成分・含量をパースするクラス
    """
    def __init__(self, root: ET.Element, brand_id: Optional[str] = None, file_path: Optional[str] = None):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
            brand_id (str): 特定のブランドID（BRD_Drug1など）
            file_path (str): XMLファイルのパス（生のXMLテキスト読み込み用）
        """
        self.root = root
        self.brand_id = brand_id
        self.file_path = file_path
        self.namespace = PMDA_NAMESPACE

    def extract_compositions(self) -> Dict[str, List[Dict[str, str]]]:
        """
        成分・含量情報を抽出する

        Returns:
            Dict[str, List[Dict[str, str]]]: カテゴリ別の成分・含量情報
            {
                "active_ingredients": [{"ingredient_name": "成分名", "value_and_unit": "250国際単位"}],
                "additives": [{"additive_name": "添加物名", "value_and_unit": "7.8mg"}],
                "other_components": [{"category": "カテゴリ", "content_title": "成分名", "content_detail": "20mL"}]
            }
        """
        result = {
            "active_ingredients": [],
            "additives": [],
            "other_components": []
        }
        
        # 1. CompositionAndPropertyセクションから成分情報を抽出
        if self.brand_id:
            # 特定のブランドIDに対応するCompositionForBrandを検索
            composition_elements = self.root.findall(f'.//pmda:CompositionForBrand[@ref="{self.brand_id}"]//pmda:CompositionTable', namespaces=self.namespace)
        else:
            # 全てのCompositionTableを検索
            composition_elements = self.root.findall('.//pmda:CompositionAndProperty//pmda:CompositionTable', namespaces=self.namespace)
        
        for composition_element in composition_elements:
            # 有効成分の抽出（ContainedAmount要素から）
            contained_amounts = composition_element.findall('.//pmda:ContainedAmount', namespaces=self.namespace)
            for contained_amount in contained_amounts:
                ingredient_name_elem = contained_amount.find('.//pmda:ActiveIngredientName/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                value_unit_elem = contained_amount.find('.//pmda:ValueAndUnit/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                if ingredient_name_elem is not None:
                    ingredient_name = self._clean_text(ingredient_name_elem.text or "")
                    value_and_unit = self._clean_text(value_unit_elem.text or "") if value_unit_elem is not None else ""
                    
                    if ingredient_name:
                        result["active_ingredients"].append({
                            "ingredient_name": ingredient_name,
                            "value_and_unit": value_and_unit
                        })
            
            # 添加物の抽出
            # 1. InfoIndividualAdditive要素から（個別の添加物）
            individual_additives = composition_element.findall('.//pmda:InfoIndividualAdditive', namespaces=self.namespace)
            for additive_info in individual_additives:
                additive_name_elem = additive_info.find('./pmda:IndividualAdditive/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                additive_value_elem = additive_info.find('./pmda:ValueAndUnit/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                if additive_name_elem is not None:
                    additive_name = self._clean_text(additive_name_elem.text or "")
                    value_and_unit = self._clean_text(additive_value_elem.text or "") if additive_value_elem is not None else ""
                    
                    if additive_name:
                        result["additives"].append({
                            "individual_additive": additive_name,
                            "value_and_unit": value_and_unit
                        })
            
            # 2. ListOfAdditives要素から（リスト形式の添加物）
            list_additives = composition_element.findall('.//pmda:ListOfAdditives/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            for list_elem in list_additives:
                if list_elem.text:
                    # XMLパーサーが<?enter?>を処理してしまうため、生のXMLから読み取る
                    additive_items = self._extract_additive_list_from_raw_xml(list_elem.text)
                    
                    for additive_item in additive_items:
                        if additive_item.strip():
                            # 添加物名と量を分離
                            additive_name, value_and_unit = self._parse_additive_item(additive_item.strip())
                            
                            if additive_name:
                                result["additives"].append({
                                    "individual_additive": additive_name,
                                    "value_and_unit": value_and_unit
                                })
            
            # その他の組成情報（OtherComposition要素から）
            other_compositions = composition_element.findall('.//pmda:OtherComposition', namespaces=self.namespace)
            for other_comp in other_compositions:
                category_elem = other_comp.find('.//pmda:CategoryName/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                content_title_elem = other_comp.find('.//pmda:ContentTitle/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                content_detail_elem = other_comp.find('.//pmda:ContentDetail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                
                # ContentTitleまたはContentDetailのいずれかがあれば処理
                if content_title_elem is not None or content_detail_elem is not None:
                    category_name = self._clean_text(category_elem.text or "") if category_elem is not None else ""
                    content_title = self._clean_text(content_title_elem.text or "") if content_title_elem is not None else ""
                    content_detail = self._clean_text(content_detail_elem.text or "") if content_detail_elem is not None else ""
                    
                    # ContentTitleがない場合はContentDetailをタイトルとして使用
                    if not content_title and content_detail:
                        content_title = content_detail
                        content_detail = ""
                    
                    if content_title:
                        result["other_components"].append({
                            "category_name": category_name,
                            "content_title": content_title,
                            "content_detail": content_detail
                        })
        
        # 重複除去
        self._remove_duplicates(result)
        
        return result
    
    def _clean_text(self, text: str) -> str:
        """
        テキストをクリーンアップする
        
        Args:
            text (str): クリーンアップ対象のテキスト
            
        Returns:
            str: クリーンアップされたテキスト
        """
        if not text:
            return ""
        
        # 改行コードやXMLマーカーを削除
        cleaned = text.replace('<?enter?>', '').replace('\n', ' ').strip()
        
        # コメント参照を削除（例: <CommentRef ref="TBLFN_01" />）
        import re
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        
        return cleaned
    
    def _extract_additive_list_from_raw_xml(self, parsed_text: str) -> List[str]:
        """
        XMLパーサーで失われた<?enter?>情報を生のXMLから復元して添加物リストを分割する
        
        Args:
            parsed_text (str): XMLパーサーによって処理されたテキスト
            
        Returns:
            List[str]: 個別の添加物のリスト
        """
        if not self.file_path or not parsed_text:
            return [parsed_text] if parsed_text else []
        
        try:
            # 生のXMLファイルを読み込む
            with open(self.file_path, 'r', encoding='utf-8') as f:
                raw_content = f.read()
            
            # ListOfAdditives内のテキストを検索
            import re
            # 対象のブランドに対応するCompositionForBrandを検索
            if self.brand_id:
                pattern = rf'<CompositionForBrand[^>]*ref="{re.escape(self.brand_id)}"[^>]*>.*?<ListOfAdditives[^>]*>.*?<Lang xml:lang="ja">(.*?)</Lang>.*?</ListOfAdditives>.*?</CompositionForBrand>'
            else:
                pattern = r'<ListOfAdditives[^>]*>.*?<Lang xml:lang="ja">(.*?)</Lang>.*?</ListOfAdditives>'
            
            matches = re.findall(pattern, raw_content, re.DOTALL)
            
            # 最も類似したマッチを探す（空白を除去して比較）
            parsed_clean = re.sub(r'\s+', '', parsed_text)
            for match in matches:
                match_clean = re.sub(r'<\?enter\?>', '', match)
                match_clean = re.sub(r'\s+', '', match_clean)
                
                if parsed_clean == match_clean:
                    # <?enter?>で分割
                    items = re.split(r'<\?enter\?>', match)
                    return [item.strip() for item in items if item.strip()]
            
            # マッチしない場合はそのまま返す
            return [parsed_text]
            
        except Exception:
            # エラーの場合はそのまま返す
            return [parsed_text]
    
    def _split_additive_list(self, text: str) -> List[str]:
        """
        <?enter?>で区切られた添加物リストを分割する
        
        Args:
            text (str): 添加物リストのテキスト
            
        Returns:
            List[str]: 個別の添加物のリスト
        """
        if not text:
            return []
        
        # <?enter?>で分割（最初に_clean_textで削除される前の原文から分割）
        import re
        # <?enter?>マーカーを改行に置換してから分割
        text_with_breaks = re.sub(r'<\?enter\?>', '\n', text)
        items = [item.strip() for item in text_with_breaks.split('\n') if item.strip()]
        
        return items
    
    def _parse_additive_item(self, item: str) -> tuple:
        """
        添加物項目から名前と量を分離する
        
        Args:
            item (str): 添加物項目（例: "人血清アルブミン 100mg"）
            
        Returns:
            tuple: (添加物名, 量・単位)
        """
        if not item:
            return "", ""
        
        # 数値と単位の正規表現パターン
        import re
        # パターン: 数値 + 単位
        pattern = r'(.+?)\s+([\d\.]+(?:\.\d+)?(?:mg|g|mL|L|％|%|単位|国際単位|IU)(?:/[\w\.]+)?)\s*$'
        
        match = re.match(pattern, item.strip())
        if match:
            additive_name = match.group(1).strip()
            value_and_unit = match.group(2).strip()
            return additive_name, value_and_unit
        
        # パターンにマッチしない場合はそのまま返す
        return item.strip(), ""
    
    
    def _remove_duplicates(self, result: Dict[str, List[Dict[str, str]]]):
        """
        重複を除去する
        
        Args:
            result (Dict): 成分情報の辞書
        """
        for category in result:
            seen = set()
            unique_items = []
            
            for item in result[category]:
                # カテゴリごとに適切なキーで重複チェック
                if category == "active_ingredients":
                    key = (item.get("ingredient_name", ""), item.get("value_and_unit", ""))
                elif category == "additives":
                    key = (item.get("individual_additive", ""), item.get("value_and_unit", ""))
                elif category == "other_components":
                    key = (item.get("category_name", ""), item.get("content_title", ""), item.get("content_detail", ""))
                else:
                    key = tuple(item.values())
                
                if key not in seen:
                    seen.add(key)
                    unique_items.append(item)
            
            result[category] = unique_items
    
    def _is_valid_composition_text(self, text: str) -> bool:
        """
        組成テキストが有効かどうかを判定する（禁忌情報を除外）
        
        Args:
            text (str): 判定対象のテキスト
            
        Returns:
            bool: 有効な組成テキストの場合True
        """
        # 空文字や短すぎるテキストは無効
        if not text or len(text.strip()) < 3:
            return False
        
        # 禁忌関連のテキストは除外
        contraindication_patterns = [
            '過敏症の既往歴', '過敏症既往歴', 'アレルギー', 'ショックの既往歴',
            '投与しないこと', '投与禁忌', '使用禁忌', '禁忌', 'に対し過敏症',
            'の成分に対し', '本剤の成分', '既往歴のある患者', '既往歴のある者'
        ]
        
        for pattern in contraindication_patterns:
            if pattern in text:
                return False
        
        # 有効な組成情報として認識
        return True

def parse_compositions(file_path: str, brand_id: Optional[str] = None) -> List[Dict[str, str]]:
    """
    XMLファイルから成分・含量情報をパースする（後方互換性のため古いフォーマット）

    Args:
        file_path (str): パースするXMLファイルのパス
        brand_id (str): 特定のブランドID（BRD_Drug1など）

    Returns:
        List[Dict[str, str]]: 成分・含量情報のリスト（後方互換性用）
    """
    try:
        # 新しい構造化データを取得
        structured_data = parse_compositions_structured(file_path, brand_id)
        
        # 古いフォーマットに変換
        compositions = []
        
        # 有効成分
        for ingredient in structured_data["active_ingredients"]:
            text = f"{ingredient['ingredient_name']}: {ingredient['value_and_unit']}"
            compositions.append({"text": text})
        
        # 添加物
        for additive in structured_data["additives"]:
            if additive['value_and_unit']:
                text = f"添加物: {additive['individual_additive']}: {additive['value_and_unit']}"
            else:
                text = f"添加物: {additive['individual_additive']}"
            compositions.append({"text": text})
        
        # その他成分
        for other in structured_data["other_components"]:
            if other['category_name']:
                text = f"{other['category_name']}: {other['content_title']}"
                if other['content_detail']:
                    text += f": {other['content_detail']}"
            else:
                text = other['content_title']
                if other['content_detail']:
                    text += f": {other['content_detail']}"
            compositions.append({"text": text})
        
        return compositions
        
    except Exception as e:
        print(f"Error parsing compositions in {file_path}: {e}")
        return []

def parse_compositions_structured(file_path: str, brand_id: Optional[str] = None) -> Dict[str, List[Dict[str, str]]]:
    """
    XMLファイルから成分・含量情報をパースする（新しい構造化フォーマット）

    Args:
        file_path (str): パースするXMLファイルのパス
        brand_id (str): 特定のブランドID（BRD_Drug1など）

    Returns:
        Dict[str, List[Dict[str, str]]]: カテゴリ別の成分・含量情報
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = CompositionParser(root, brand_id, file_path)
        return parser.extract_compositions()
    except Exception as e:
        print(f"Error parsing compositions in {file_path}: {e}")
        return {"active_ingredients": [], "additives": [], "other_components": []}