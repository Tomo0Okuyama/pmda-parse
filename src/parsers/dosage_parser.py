import xml.etree.ElementTree as ET
import re
import os
from typing import List, Dict, Optional
from parsers.xml_utils import register_xml_namespaces, PMDA_NAMESPACE, extract_condition_header, remove_duplicates_by_key

class DosageParser:
    """
    医薬品の用法・用量をパースするクラス
    """
    def __init__(self, root: ET.Element, file_path: str = ""):
        """
        初期化メソッド

        Args:
            root (ET.Element): XMLのルート要素
            file_path (str): XMLファイルのパス（特殊処理判定用）
        """
        self.root = root
        self.file_path = file_path
        self.namespace = PMDA_NAMESPACE

    def _format_dosage_with_condition(self, text: str, condition_header: str) -> str:
        """
        用法・用量テキストに条件ヘッダーを付与する
        
        Args:
            text (str): 元の用法・用量テキスト
            condition_header (str): 条件ヘッダー（Header要素から抽出）
            
        Returns:
            str: 条件付きのテキスト
        """
        # 条件ヘッダーがある場合はそれを使用（疾患:対象者:用法 形式）
        if condition_header and condition_header.strip():
            return f"{condition_header}:{text}"
        
        # 条件ヘッダーがない場合はそのまま返す
        return text
    
    
    def _process_nested_items(self, item_element: ET.Element, parent_condition: str = "") -> List[Dict[str, str]]:
        """
        ネストしたItem要素を再帰的に処理する
        
        Args:
            item_element: Item要素
            parent_condition: 親階層の条件ヘッダー
            
        Returns:
            List[Dict[str, str]]: 用法・用量のリスト
        """
        dosages = []
        
        # 現在のItemの条件ヘッダーを取得
        current_condition = extract_condition_header(item_element, self.namespace)
        
        # 親条件と結合（階層構造を維持）
        if parent_condition and current_condition:
            combined_condition = f"{parent_condition}:{current_condition}"
        elif parent_condition:
            combined_condition = parent_condition
        elif current_condition:
            combined_condition = current_condition
        else:
            combined_condition = ""
        
        # 直接のDetail要素があるかチェック
        direct_detail_elements = item_element.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        
        for detail in direct_detail_elements:
            if detail.text and detail.text.strip():
                text = detail.text.strip()
                formatted_text = self._format_dosage_with_condition(text, combined_condition)
                dosages.append({
                    'text': formatted_text,
                })
        
        # ネストしたSimpleList/Item要素を再帰的に処理
        nested_items = item_element.findall('./pmda:SimpleList/pmda:Item', namespaces=self.namespace)
        for nested_item in nested_items:
            dosages.extend(self._process_nested_items(nested_item, combined_condition))
        
        return dosages
    
    def _has_complex_dosage_methods(self) -> bool:
        """
        複雑な投与法構造（A法〜F法パターン）を持つファイルかどうかを判定する
        
        ※例外処理: 抗癌剤等で複雑な投与法(A法〜F法)と体表面積別用量テーブルを持つ薬剤
        確認済み: カペシタビン、TS-1、イリノテカン、パクリタキセル等
        
        Returns:
            bool: 複雑な投与法構造を持つ場合True
        """
        if not self.file_path:
            return False
        
        try:
            # InfoDoseAdminから投与法パターンをチェック
            info_dose_admin = self.root.find('.//pmda:InfoDoseAdmin', namespaces=self.namespace)
            if info_dose_admin is None:
                return False
            
            # 全てのDetailテキストを取得
            all_text = ""
            for detail in info_dose_admin.findall('.//pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace):
                if detail.text:
                    all_text += detail.text + " "
            
            # A法〜F法のパターンが2つ以上ある場合は複雑な構造と判定
            method_pattern = r'([A-F]法)：'
            methods = re.findall(method_pattern, all_text)
            
            # TblBlockの数もチェック（複数のテーブルがある場合）
            tbl_blocks = info_dose_admin.findall('.//pmda:TblBlock', namespaces=self.namespace)
            
            # 2つ以上の投与法がある、または複数のテーブルがある場合（抗癌剤等）
            return len(set(methods)) >= 2 or len(tbl_blocks) >= 2
            
        except Exception:
            return False
    
    def _parse_complex_dosage_table(self, table_element: ET.Element) -> List[str]:
        """
        複雑な投与法の体表面積別用量テーブルをパースする（汎用化）
        
        Args:
            table_element: SimpleTable要素
            
        Returns:
            List[str]: 体表面積範囲:用量のリスト
        """
        dosage_entries = []
        rows = table_element.findall('.//pmda:SimpTblRow', namespaces=self.namespace)
        
        # ヘッダー行をスキップして、データ行のみ処理
        for row in rows[1:]:  # 最初の行はヘッダーなのでスキップ
            cells = row.findall('./pmda:SimpTblCell', namespaces=self.namespace)
            if len(cells) >= 2:
                # 各セルの全内容を再帰的に取得
                bsa_content = self._extract_cell_content(cells[0])
                dose_content = self._extract_cell_content(cells[1])
                
                if bsa_content.strip() and dose_content.strip():
                    dosage_entries.append(f"{bsa_content.strip()}:{dose_content.strip()}")
        
        return dosage_entries
    
    def _extract_cell_content(self, cell_element: ET.Element) -> str:
        """
        テーブルセルの内容を抽出し、Sup要素も適切に処理する
        
        Args:
            cell_element: SimpTblCell要素
            
        Returns:
            str: セルの内容
        """
        content = ""
        
        def extract_text_recursive(element):
            text = element.text or ""
            
            for child in element:
                if child.tag.endswith('Sup'):
                    # 上付き文字の処理 (例: m<Sup>2</Sup> → m²)
                    child_text = child.text or ""
                    if child_text == "2":
                        text += "²"
                    else:
                        text += child_text
                else:
                    text += extract_text_recursive(child)
                
                # 子要素の後のテキストも追加
                text += (child.tail or "")
            
            return text
        
        # Langタグを探してテキストを抽出
        lang_elements = cell_element.findall('.//pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        for lang in lang_elements:
            content += extract_text_recursive(lang)
        
        return content
    
    def _extract_complex_dosages(self) -> List[Dict[str, str]]:
        """
        複雑な投与法構造の用法・用量をパースする（汎用化）
        
        ※例外処理: 抗癌剤等で複雑な投与法(A法〜F法)と体表面積別用量テーブルを持つ薬剤
        対応薬剤: カペシタビン、TS-1、イリノテカン、パクリタキセル等
        
        Returns:
            List[Dict[str, str]]: 構造化された用法・用量のリスト
        """
        dosages = []
        
        # InfoDoseAdminから詳細を取得
        info_dose_admin = self.root.find('.//pmda:InfoDoseAdmin', namespaces=self.namespace)
        if info_dose_admin is None:
            return []
        
        # 1. 前提条件（適応症別投与法選択指針）を抽出
        dose_admin = info_dose_admin.find('.//pmda:DoseAdmin', namespaces=self.namespace)
        if dose_admin is not None:
            first_detail = dose_admin.find('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
            if first_detail is not None and first_detail.text:
                # 前提条件の文章をそのまま追加（文節を区切らない）
                text = first_detail.text.strip()
                # 最初の改行までの部分を前提条件として抽出
                premise_text = text.split('<?enter?>')[0]
                if premise_text:
                    dosages.append({'text': premise_text})
        
        # 2. 各投与法（A法〜F法）の詳細を抽出
        method_pattern = r'([A-F]法)：(.+?)(?=(?:[A-F]法：|$))'
        all_text = ""
        
        # 全てのDetailテキストを連結
        for detail in dose_admin.findall('.//pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace):
            if detail.text:
                all_text += detail.text + " "
        
        # 投与法別に分離（正規表現パターンマッチング）
        methods = re.findall(method_pattern, all_text, re.DOTALL)
        
        for method_name, method_detail in methods:
            # 投与スケジュールを抽出（テーブル情報は後で追加）
            schedule_text = method_detail.split('<?enter?>')[0].strip()
            
            # 対応するテーブルを検索
            dosage_by_bsa = []
            
            # 各TblBlockを確認してこの投与法に対応するテーブルを見つける
            tbl_blocks = dose_admin.findall('.//pmda:TblBlock', namespaces=self.namespace)
            
            for i, tbl_block in enumerate(tbl_blocks):
                # テーブルの前後のテキストから対応する投与法を判定
                if method_name[0] in ['A', 'B', 'C', 'D', 'E', 'F']:  # A〜F法の判定
                    method_index = ord(method_name[0]) - ord('A')
                    if i == method_index:  # テーブルの順序で対応する投与法を特定
                        simple_table = tbl_block.find('.//pmda:SimpleTable', namespaces=self.namespace)
                        if simple_table is not None:
                            dosage_by_bsa = self._parse_complex_dosage_table(simple_table)
                            break
            
            # 投与法の詳細情報を構築（体表面積情報を含む）
            if dosage_by_bsa:
                bsa_info = "、".join(dosage_by_bsa)
                dosage_text = f"{method_name}:{schedule_text}/体表面積{bsa_info}"
            else:
                dosage_text = f"{method_name}:{schedule_text}"
            
            dosages.append({'text': dosage_text})
        
        return dosages

    def _parse_dosage_table(self, tbl_block: ET.Element) -> List[str]:
        """
        用法・用量テーブルをパースして構造化された情報を抽出する
        
        Args:
            tbl_block: TblBlock要素
            
        Returns:
            List[str]: 構造化された用法・用量情報
        """
        dosage_entries = []
        
        simple_table = tbl_block.find('.//pmda:SimpleTable', namespaces=self.namespace)
        if simple_table is None:
            return dosage_entries
        
        rows = simple_table.findall('.//pmda:SimpTblRow', namespaces=self.namespace)
        if not rows:
            return dosage_entries
        
        # ヘッダー行を取得
        header_row = rows[0]
        header_cells = header_row.findall('./pmda:SimpTblCell', namespaces=self.namespace)
        
        # ヘッダー情報を抽出
        headers = []
        for cell in header_cells:
            cell_text = self._extract_table_cell_text(cell)
            if cell_text:
                headers.append(cell_text)
        
        # データ行を処理（造影剤等の医療手技別用量テーブル）
        for row in rows[1:]:  # ヘッダー行をスキップ
            cells = row.findall('./pmda:SimpTblCell', namespaces=self.namespace)
            
            if not cells:
                continue
                
            # 最初のセルは効能・効果（医療手技名）
            procedure_cell = cells[0]
            procedure_name = self._extract_table_cell_text(procedure_cell)
            
            if not procedure_name or procedure_name.strip() == '':
                continue
            
            # 各薬剤濃度の用量情報を抽出
            dosage_info = []
            for i, cell in enumerate(cells[1:], 1):  # 最初のセルをスキップ
                cell_text = self._extract_table_cell_text(cell)
                if cell_text and cell_text.strip() != '－' and cell_text.strip() != '':
                    if i < len(headers):
                        # ヘッダー名（薬剤名）と用量を組み合わせ
                        drug_name = headers[i].replace('<?enter?>', '').strip()
                        dosage_info.append(f"{drug_name}:{cell_text}")
                    else:
                        dosage_info.append(cell_text)
            
            # 手技名と用量情報を組み合わせて構造化（例：脳血管撮影 → イオパミドール300注「F」:6～13mL）
            if dosage_info:
                combined_info = f"{procedure_name} → {' / '.join(dosage_info)}"
                dosage_entries.append(combined_info)
        
        return dosage_entries

    def _extract_table_cell_text(self, cell: ET.Element) -> str:
        """
        テーブルセルのテキスト内容を抽出する
        
        Args:
            cell: SimpTblCell要素
            
        Returns:
            str: セルのテキスト内容
        """
        text_parts = []
        
        # Detail/Lang要素からテキストを抽出
        lang_elements = cell.findall('.//pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
        for lang in lang_elements:
            if lang.text:
                text_parts.append(lang.text.strip())
        
        # <?enter?>を改行に変換
        combined_text = ' '.join(text_parts)
        combined_text = combined_text.replace('<?enter?>', '\n').strip()
        
        return combined_text

    def extract_dosages(self) -> List[Dict[str, str]]:
        """
        用法・用量を抽出し、ネストしたHeader構造を適切に処理する

        Returns:
            List[Dict[str, str]]: 用法・用量のリスト
        """
        # ※例外処理: 複雑な投与法構造（A法〜F法パターン）の特殊構造対応
        if self._has_complex_dosage_methods():
            return self._extract_complex_dosages()
        
        dosages = []
        
        # InfoDoseAdminタグから用法・用量を抽出
        info_dose_admin_elements = self.root.findall('.//pmda:InfoDoseAdmin', namespaces=self.namespace)
        
        for info_dose_admin in info_dose_admin_elements:
            # DoseAdminタグの内容を取得
            dose_admin_elements = info_dose_admin.findall('.//pmda:DoseAdmin', namespaces=self.namespace)
            
            for dose_admin in dose_admin_elements:
                # テーブル構造があるかチェック（造影剤等の医療手技別用量テーブル）
                tbl_blocks = dose_admin.findall('.//pmda:TblBlock', namespaces=self.namespace)
                
                if tbl_blocks:
                    # テーブル構造がある場合（造影剤等の医療手技別用量テーブル）
                    # まず基本的な説明文を抽出
                    base_detail_elements = dose_admin.findall('./pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                    for detail in base_detail_elements:
                        if detail.text and detail.text.strip():
                            text = detail.text.strip()
                            if not any(dosage['text'] == text for dosage in dosages):
                                dosages.append({'text': text})
                    
                    # 各テーブルブロックを処理
                    for tbl_block in tbl_blocks:
                        table_dosages = self._parse_dosage_table(tbl_block)
                        for table_dosage in table_dosages:
                            if not any(dosage['text'] == table_dosage for dosage in dosages):
                                dosages.append({'text': table_dosage})
                    
                    # テーブル後のComment要素も処理
                    comment_elements = dose_admin.findall('.//pmda:Comment/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                    for comment in comment_elements:
                        if comment.text and comment.text.strip():
                            text = comment.text.strip()
                            # コメント形式で追加
                            formatted_text = f"注意: {text}"
                            if not any(dosage['text'] == formatted_text for dosage in dosages):
                                dosages.append({'text': formatted_text})
                
                else:
                    # テーブル構造がない場合は従来の処理
                    # SimpleList/Item要素を処理
                    item_elements = dose_admin.findall('./pmda:SimpleList/pmda:Item', namespaces=self.namespace)
                    
                    for item in item_elements:
                        # ネストしたItem構造を再帰的に処理
                        nested_dosages = self._process_nested_items(item)
                        
                        # 重複チェックをして追加
                        for dosage in nested_dosages:
                            if not any(existing['text'] == dosage['text'] for existing in dosages):
                                dosages.append(dosage)
                    
                    # 従来のDetail/Langタグからも用法・用量テキストを抽出（Item構造にない場合）
                    if not item_elements:
                        detail_elements = dose_admin.findall('.//pmda:Detail/pmda:Lang[@xml:lang="ja"]', namespaces=self.namespace)
                        
                        # DoseAdmin全体での条件ヘッダーを取得
                        condition_header = extract_condition_header(dose_admin, self.namespace)
                        
                        for detail in detail_elements:
                            if detail.text and detail.text.strip():
                                text = detail.text.strip()
                                formatted_text = self._format_dosage_with_condition(text, condition_header)
                                
                                # 重複チェック
                                if not any(dosage['text'] == formatted_text for dosage in dosages):
                                    dosages.append({
                                        'text': formatted_text,
                                    })
        
        # 従来の方法での検索も併用（InfoDoseAdminに含まれない用法・用量も取得）
        for element in self.root.iter():
            if element.text and "用法・用量" in element.text:
                text = element.text.strip()
                # 従来検索の場合は条件ヘッダーなし
                formatted_text = self._format_dosage_with_condition(text, "")
                
                # 既に追加済みのテキストは除外
                if not any(dosage['text'] == formatted_text for dosage in dosages):
                    dosages.append({
                        'text': formatted_text,
                    })
        
        return dosages

def parse_dosages(file_path: str) -> List[Dict[str, str]]:
    """
    XMLファイルから用法・用量をパースする

    Args:
        file_path (str): パースするXMLファイルのパス

    Returns:
        List[Dict[str, str]]: 用法・用量のリスト
    """
    try:
        # 名前空間を登録
        register_xml_namespaces()
        
        # XMLファイルをパースして用法・用量を抽出
        tree = ET.parse(file_path)
        root = tree.getroot()
        parser = DosageParser(root, file_path)  # ファイルパスを渡して特殊処理判定用
        return parser.extract_dosages()
    except Exception:
        return []