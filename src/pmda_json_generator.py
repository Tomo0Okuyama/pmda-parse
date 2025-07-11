#!/usr/bin/env python3
"""
PMDA医薬品データJSON生成ツール

PMDAが提供するXML/SGMLファイルから医薬品情報を抽出し、
ベクトル検索用のJSONファイルを生成します。
"""

import os
import json
import time
import argparse
import glob
import re
import sys
from typing import Dict, List, Any, Optional
from collections import defaultdict

# パーサーのインポート - 各医療情報カテゴリ専用パーサー
from parsers.base_parser import MedicineParser
from parsers.indication_parser import parse_indications
from parsers.dosage_parser import parse_dosages
from parsers.contraindication_parser import parse_contraindications
from parsers.warning_parser import parse_warnings
from parsers.side_effect_parser import parse_side_effects
from parsers.interaction_parser import parse_interactions
from parsers.composition_parser import parse_compositions
from parsers.active_ingredient_parser import parse_active_ingredients
from utils.file_processor import find_duplicate_files, detect_parse_candidates

class PMDAJSONGenerator:
    """
    PMDAデータからベクトル検索用JSONを生成するクラス
    """
    
    def __init__(self, input_directory: str, output_file: str):
        """
        初期化メソッド
        
        Args:
            input_directory (str): PMDAデータディレクトリのパス
            output_file (str): 出力JSONファイルのパス
        """
        self.input_directory = input_directory
        self.output_file = output_file
        self.statistics = {
            'total_files_found': 0,
            'duplicate_files': 0,
            'processed_files': 0,
            'error_files': 0,
            'medicines_count': 0,
            'vectors_count': defaultdict(int),
            'medicines_with_clinical_info': defaultdict(int),
            'processing_time': 0
        }
        
    def process_single_medicine(self, file_path: str) -> List[Dict[str, Any]]:
        """
        単一の医薬品XMLファイルを処理し、複数医薬品の場合は複数のJSONエントリを返す
        
        Args:
            file_path (str): XMLファイルパス
            
        Returns:
            List[Dict[str, Any]]: 処理された医薬品データのリスト
        """
        try:
            # ベースパーサーで基本情報を取得
            base_parser = MedicineParser(file_path)
            
            # 全ての医薬品ブランド情報を取得（複数医薬品対応）
            all_brands = base_parser.extract_all_brands()
            
            if not all_brands:
                # フォールバック：従来の方法で単一医薬品として処理
                medicine_data = base_parser.to_json()
                medicine_data['source_filename'] = os.path.basename(file_path)
                return [self._process_clinical_info(medicine_data, file_path)]
            
            medicines_list = []
            for brand in all_brands:
                # 各医薬品に対してJSONエントリを作成
                medicine_data = {
                    'yj_code': brand['yj_code'],
                    'product_name': brand['product_name'],
                    'form': base_parser.extract_form() or '',
                    'manufacturer_code': base_parser.extract_manufacturer_code() or '',
                    'manufacturer_name': base_parser.extract_manufacturer_name() or '',
                    'source_filename': os.path.basename(file_path)
                }
                
                # 臨床情報を追加（brand情報を渡す）
                medicines_list.append(self._process_clinical_info(medicine_data, file_path, brand))
            
            return medicines_list
            
        except Exception as e:
            print(f"ERROR: {file_path}: {e}")
            self.statistics['error_files'] += 1
            return []
    
    def _process_clinical_info(self, medicine_data: Dict[str, Any], file_path: str, brand_info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        医薬品データに臨床情報を追加する
        
        Args:
            medicine_data (Dict[str, Any]): 基本医薬品データ
            file_path (str): XMLファイルパス
            brand_info (Dict[str, str]): ブランド情報（product_name, yj_code）
            
        Returns:
            Dict[str, Any]: 臨床情報が追加された医薬品データ
        """
        # 各専門パーサーで詳細情報を取得
        clinical_info = {}
        
        # 効能・効果（適応）- 全ての情報を抽出（重複除去）
        indications = parse_indications(file_path)
        indication_texts = []
        seen_indications = set()
        for indication in indications:
            text = indication['text']
            if text and text not in seen_indications:  # 空でなく、重複していなければ取得
                indication_texts.append(text)
                seen_indications.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if indication_texts:
            self.statistics['vectors_count']['indications'] += len(indication_texts)
        
        if indication_texts:
            clinical_info['indications'] = indication_texts
        
        # 用法・用量 - 全ての用法情報を取得（重複除去）
        dosages = parse_dosages(file_path)
        dosage_texts = []
        seen_dosages = set()
        for dosage in dosages:
            text = dosage['text']
            if text and text not in seen_dosages:  # 空でなく、重複していなければ取得
                dosage_texts.append(text)
                seen_dosages.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if dosage_texts:
            self.statistics['vectors_count']['dosage'] += len(dosage_texts)
        
        if dosage_texts:
            clinical_info['dosage'] = dosage_texts
        
        # 禁忌 - 全ての禁忌情報を取得（重複除去）
        contraindications = parse_contraindications(file_path)
        contraindication_texts = []
        seen_contraindications = set()
        for contraindication in contraindications:
            text = contraindication['text']
            if text and text not in seen_contraindications:  # 空でなく、重複していなければ取得
                contraindication_texts.append(text)
                seen_contraindications.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if contraindication_texts:
            self.statistics['vectors_count']['contraindications'] += len(contraindication_texts)
        
        if contraindication_texts:
            clinical_info['contraindications'] = contraindication_texts
        
        # 警告・注意事項 - 全ての警告情報を取得（重複除去）
        warnings = parse_warnings(file_path)
        warning_texts = []
        seen_warnings = set()
        for warning in warnings:
            text = warning['text']
            if text and text not in seen_warnings:  # 空でなく、重複していなければ取得
                warning_texts.append(text)
                seen_warnings.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if warning_texts:
            self.statistics['vectors_count']['warnings'] += len(warning_texts)
        
        if warning_texts:
            clinical_info['warnings'] = warning_texts
        
        # 副作用 - 全ての副作用情報を取得（重複除去）
        side_effects = parse_side_effects(file_path)
        side_effect_texts = []
        seen_side_effects = set()
        for side_effect in side_effects:
            text = side_effect['text']
            if text and text not in seen_side_effects:  # 空でなく、重複していなければ取得
                side_effect_texts.append(text)
                seen_side_effects.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if side_effect_texts:
            self.statistics['vectors_count']['side_effects'] += len(side_effect_texts)
        
        if side_effect_texts:
            clinical_info['side_effects'] = side_effect_texts
        
        # 相互作用 - 全ての相互作用情報を取得（重複除去）
        interactions = parse_interactions(file_path)
        interaction_texts = []
        seen_interactions = set()
        for interaction in interactions:
            text = interaction['text']
            if text and text not in seen_interactions:  # 空でなく、重複していなければ取得
                interaction_texts.append(text)
                seen_interactions.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if interaction_texts:
            self.statistics['vectors_count']['interactions'] += len(interaction_texts)
        
        if interaction_texts:
            clinical_info['interactions'] = interaction_texts
        
        # BRD_DrugのIDを推定（複数医薬品対応）
        brand_id = None
        if brand_info and isinstance(brand_info, dict) and brand_info.get('yj_code'):
            # YJコードからBRD_DrugのIDを推定
            # 複数医薬品XMLでは通常BRD_Drug1, BRD_Drug2... の形式
            import xml.etree.ElementTree as ET
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                namespace = {'pmda': 'http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0'}
                
                # DetailBrandNameからYJコードに対応するIDを検索
                brand_elements = root.findall('.//pmda:DetailBrandName', namespace)
                for brand_element in brand_elements:
                    yj_element = brand_element.find('.//pmda:YJCode', namespace)
                    if yj_element is not None and yj_element.text == brand_info['yj_code']:
                        brand_id = brand_element.get('id')
                        break
            except Exception as e:
                print(f"Error extracting brand_id: {e}")
                pass
        
        # 成分・含量 - 全ての成分情報を取得（重複除去）
        compositions = parse_compositions(file_path, brand_id)
        composition_texts = []
        seen_compositions = set()
        for composition in compositions:
            text = composition['text']
            if text and text not in seen_compositions:  # 空でなく、重複していなければ取得
                composition_texts.append(text)
                seen_compositions.add(text)
        
        # 統計情報は最終的な配列の長さで更新
        if composition_texts:
            self.statistics['vectors_count']['compositions'] += len(composition_texts)
        
        if composition_texts:
            clinical_info['compositions'] = composition_texts
        
        # 有効成分詳細情報 - PhyschemOfActIngredientsから物理化学的情報のみを取得
        active_ingredient_data = parse_active_ingredients(file_path, brand_id)
        
        # ingredient_nameとcontent_amountは組成データと重複するため除外
        # 物理化学的情報（一般名、化学名、分子式、分子量、性状等）のみを抽出
        filtered_active_ingredients = []
        for ingredient in active_ingredient_data:
            # 不要なフィールドを除外（組成データと重複するため）
            filtered_ingredient = {k: v for k, v in ingredient.items() 
                                 if k not in ['ingredient_name', 'content_amount']}
            
            # 物理化学的情報がある場合のみ追加
            if any(filtered_ingredient.values()):
                filtered_active_ingredients.append(filtered_ingredient)
        
        if filtered_active_ingredients:
            clinical_info['active_ingredients'] = filtered_active_ingredients
            self.statistics['medicines_with_clinical_info']['active_ingredients'] += 1
            self.statistics['vectors_count']['active_ingredients'] += len(filtered_active_ingredients)
        
        # 臨床情報を設定
        medicine_data['clinical_info'] = clinical_info
        
        # 各医療情報項目を持つ医薬品数をカウント（統計情報更新）
        has_all_info_types = True
        
        if clinical_info.get('indications'):
            self.statistics['medicines_with_clinical_info']['indications'] += 1
        else:
            has_all_info_types = False
            
        if clinical_info.get('dosage'):
            self.statistics['medicines_with_clinical_info']['dosage'] += 1
        else:
            has_all_info_types = False
            
        if clinical_info.get('contraindications'):
            self.statistics['medicines_with_clinical_info']['contraindications'] += 1
        else:
            has_all_info_types = False
            
        if clinical_info.get('warnings'):
            self.statistics['medicines_with_clinical_info']['warnings'] += 1
        else:
            has_all_info_types = False
            
        if clinical_info.get('side_effects'):
            self.statistics['medicines_with_clinical_info']['side_effects'] += 1
        else:
            has_all_info_types = False
            
        if clinical_info.get('interactions'):
            self.statistics['medicines_with_clinical_info']['interactions'] += 1
        else:
            has_all_info_types = False
            
        if clinical_info.get('compositions'):
            self.statistics['medicines_with_clinical_info']['compositions'] += 1
        else:
            has_all_info_types = False
            
        # 有効成分は全種類の計算には含めない（医薬品によっては存在しない場合があるため）
        
        # 全種類の医療情報を持つ医薬品をカウント
        if has_all_info_types:
            self.statistics['medicines_with_clinical_info']['all_types'] += 1
        
        # 古いvectorsキーを削除（後方互換性のため）
        if 'vectors' in medicine_data:
            del medicine_data['vectors']
        
        # 後方互換性のため、古いmanufacturerキーを削除
        if 'manufacturer' in medicine_data:
            del medicine_data['manufacturer']
        
        return medicine_data
    
    def discover_and_process_files(self) -> List[Dict[str, Any]]:
        """
        ファイルを発見し、全て処理する
        
        Returns:
            List[Dict[str, Any]]: 全ての医薬品データのリスト
        """
        print("=== PMDA医薬品データJSON生成開始 ===")
        start_time = time.time()
        
        # 1. XML/SGMLファイルの発見
        print("1. ファイル発見中...")
        xml_sgml_path = os.path.join(self.input_directory, 'SGML_XML')
        if not os.path.exists(xml_sgml_path):
            raise ValueError(f"SGML_XMLディレクトリが見つかりません: {xml_sgml_path}")
        
        # 全てのXML/SGMLファイルを検索
        all_files = []
        for root, _, files in os.walk(xml_sgml_path):
            for file in files:
                if file.endswith(('.xml', '.sgml')):
                    all_files.append(os.path.join(root, file))
        
        self.statistics['total_files_found'] = len(all_files)
        print(f"   発見されたファイル数: {self.statistics['total_files_found']}")
        
        # 2. 重複ファイルの除去（SHA-256ハッシュベース）
        print("2. 重複ファイル除去中...")
        _ = find_duplicate_files(xml_sgml_path, ['.xml', '.sgml'])
        
        # 重複を除いたファイルリストを生成
        unique_file_candidates = detect_parse_candidates(xml_sgml_path, ['.xml', '.sgml'], True)
        unique_files = [file_path for file_path, _ in unique_file_candidates]
        self.statistics['duplicate_files'] = len(all_files) - len(unique_files)
        
        print(f"   重複ファイル数: {self.statistics['duplicate_files']}")
        print(f"   処理対象ファイル数: {len(unique_files)}")
        
        # 3. 各ファイルの処理（医薬品データ抽出）
        print("3. 医薬品データ処理中...")
        all_medicines = []
        
        for i, file_path in enumerate(unique_files):
            # 進捗表示（100件ごと）
            if i % 100 == 0:
                print(f"   進捗: {i}/{len(unique_files)} ({i/len(unique_files)*100:.1f}%)")
            
            medicines_list = self.process_single_medicine(file_path)
            if medicines_list:
                all_medicines.extend(medicines_list)
                self.statistics['processed_files'] += 1
        
        self.statistics['medicines_count'] = len(all_medicines)
        self.statistics['processing_time'] = time.time() - start_time
        
        print(f"4. データ処理完了")
        print(f"   処理された医薬品数: {self.statistics['medicines_count']}")
        
        return all_medicines
    
    def save_json(self, medicines: List[Dict[str, Any]]):
        """
        医薬品データをJSONファイルに保存する
        
        Args:
            medicines (List[Dict[str, Any]]): 医薬品データのリスト
        """
        print(f"5. JSONファイル出力中...")
        
        # 出力ディレクトリの作成（ディレクトリが指定されている場合のみ）
        output_dir = os.path.dirname(self.output_file)
        if output_dir:  # ディレクトリが指定されている場合のみ作成
            os.makedirs(output_dir, exist_ok=True)
        
        # JSON出力（UTF-8エンコーディング、インデント付き）
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(medicines, f, ensure_ascii=False, indent=2)
        
        # ファイルサイズを取得・表示
        file_size = os.path.getsize(self.output_file)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"   出力完了: {self.output_file}")
        print(f"   ファイルサイズ: {file_size_mb:.2f} MB")
    
    def print_summary(self):
        """
        処理結果のサマリーを出力する
        """
        print("\n=== 処理サマリー ===")
        print(f"発見されたファイル数: {self.statistics['total_files_found']:,}")
        print(f"重複ファイル数: {self.statistics['duplicate_files']:,}")
        print(f"処理成功ファイル数: {self.statistics['processed_files']:,}")
        print(f"処理エラーファイル数: {self.statistics['error_files']:,}")
        print(f"生成された医薬品エントリー数: {self.statistics['medicines_count']:,}")
        print(f"処理時間: {self.statistics['processing_time']:.2f}秒")
        
        # 処理速度の計算
        if self.statistics['processing_time'] > 0:
            files_per_second = self.statistics['processed_files'] / self.statistics['processing_time']
            medicines_per_second = self.statistics['medicines_count'] / self.statistics['processing_time']
            print(f"処理速度: {files_per_second:.1f}ファイル/秒, {medicines_per_second:.1f}医薬品エントリー/秒")
        
        print("\n=== 抽出された臨床情報 ===")
        total_clinical_data = sum(self.statistics['vectors_count'].values())
        
        
        # 日本語文字幅を考慮した表示幅計算
        def get_display_width(text):
            """日本語文字を考慮した表示幅を計算"""
            width = 0
            for char in text:
                if ord(char) > 127:  # 日本語文字
                    width += 2
                else:  # 英数字
                    width += 1
            return width
        
        # 最適化版と同じ順序に変更
        clinical_info_order = [
            ('効能・効果', 'indications'),
            ('用法・用量', 'dosage'),
            ('成分・含量', 'compositions'),
            ('有効成分', 'active_ingredients'),
            ('禁忌', 'contraindications'),
            ('副作用', 'side_effects'),
            ('相互作用', 'interactions'),
            ('警告・注意', 'warnings')
        ]
        
        # 最大幅を計算
        max_width = max(get_display_width(display_name) for display_name, _ in clinical_info_order)
        max_width = max(max_width, get_display_width('総臨床情報数'))
        
        for display_name, key in clinical_info_order:
            count = self.statistics['vectors_count'].get(key, 0)
            current_width = get_display_width(display_name)
            padding = max_width - current_width
            print(f"{display_name}{' ' * padding}: {count:8,}件")
        
        total_vectors = sum(self.statistics['vectors_count'].values())
        total_width = get_display_width('総臨床情報数')
        total_padding = max_width - total_width
        print(f"総臨床情報数{' ' * total_padding}: {total_vectors:8,}件")
        
        print("\n=== 医療情報種別毎の医薬品数 ===")
        
        # 最適化版と同じ順序に変更
        info_order = [
            ('効能・効果', 'indications'),
            ('用法・用量', 'dosage'),
            ('成分・含量', 'compositions'),
            ('有効成分', 'active_ingredients'),
            ('禁忌', 'contraindications'),
            ('副作用', 'side_effects'),
            ('相互作用', 'interactions'),
            ('警告・注意', 'warnings')
        ]
        
        # 最大幅を計算
        max_width_info = max(get_display_width(display_name) for display_name, _ in info_order)
        
        for display_name, key in info_order:
            count = self.statistics['medicines_with_clinical_info'].get(key, 0)
            percentage = (count / self.statistics['medicines_count']) * 100 if self.statistics['medicines_count'] > 0 else 0
            current_width = get_display_width(display_name)
            padding = max_width_info - current_width
            print(f"{display_name}{' ' * padding}: {count:8,}件 ({percentage:5.1f}%)")
        
        # 全ての医療情報を持つ医薬品数
        medicines_with_all_info = self.statistics['medicines_with_clinical_info'].get('all_types', 0)
        
        print(f"\n全種類の医療情報を持つ医薬品: {medicines_with_all_info:,}件")
        
        print(f"\n=== 完了 ===")
        print(f"出力ファイル: {self.output_file}")
    
    def generate(self):
        """
        全体の生成処理を実行する（メイン処理）
        """
        try:
            # 医薬品データを処理
            medicines = self.discover_and_process_files()
            
            # JSONファイルに保存
            self.save_json(medicines)
            
            # サマリー出力
            self.print_summary()
            
        except Exception as e:
            print(f"エラーが発生しました: {e}")
            raise

def find_pmda_directories() -> List[str]:
    """
    カレントディレクトリでpmda_all_nnnnnnnn形式のディレクトリを検索
    
    Returns:
        List[str]: 見つかったPMDAディレクトリのリスト
    """
    pattern = 'pmda_all_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
    pmda_dirs = []
    
    for item in glob.glob(pattern):
        if os.path.isdir(item):
            # ディレクトリ名が正確にパターンに一致するかチェック
            dir_name = os.path.basename(item)
            if re.match(r'^pmda_all_\d{8}$', dir_name):
                pmda_dirs.append(item)
    
    return sorted(pmda_dirs)  # 日付順でソート

def auto_detect_pmda_directory() -> str:
    """
    PMDAディレクトリを自動検出
    
    Returns:
        str: 検出されたディレクトリパス
        
    Raises:
        SystemExit: ディレクトリが見つからないか、複数見つかった場合
    """
    pmda_dirs = find_pmda_directories()
    
    if len(pmda_dirs) == 0:
        print("エラー: pmda_all_nnnnnnnn形式のディレクトリが見つかりません")
        print("\n使用方法:")
        print("1. PMDAからデータをダウンロードし、pmda_all_nnnnnnnn形式で展開してください")
        print("   例: pmda_all_20250709")
        print("2. またはディレクトリパスを引数で明示的に指定してください")
        print("   例: python src/pmda_json_generator.py /path/to/pmda_data")
        sys.exit(1)
    
    if len(pmda_dirs) > 1:
        print(f"エラー: 複数のPMDAディレクトリが見つかりました: {', '.join(pmda_dirs)}")
        print("\n使用方法:")
        print("使用するディレクトリを引数で明示的に指定してください")
        print("例:")
        for pmda_dir in pmda_dirs:
            print(f"  python src/pmda_json_generator.py {pmda_dir}")
        sys.exit(1)
    
    detected_dir = pmda_dirs[0]
    print(f"PMDAディレクトリを自動検出しました: {detected_dir}")
    return detected_dir

def main():
    """
    メイン関数：コマンドライン引数を処理して実行
    """
    parser = argparse.ArgumentParser(
        description='PMDA医薬品データからベクトル検索用JSONを生成します',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # カレントディレクトリのpmda_all_nnnnnnnnを自動検出
  python src/pmda_json_generator.py
  
  # 特定のディレクトリを指定
  python src/pmda_json_generator.py pmda_all_20250709
  
  # 出力ファイルを指定
  python src/pmda_json_generator.py pmda_all_20250709 -o custom_output.json
  
デフォルトではカレントディレクトリにpmda_medicines.jsonとして出力されます。
        """
    )
    
    parser.add_argument(
        'pmda_directory',
        nargs='?',
        default=None,
        help='PMDAデータディレクトリのパス（省略時は自動検出）'
    )
    
    parser.add_argument(
        '-o', '--output',
        default='pmda_medicines.json',
        help='出力JSONファイルのパス（デフォルト: pmda_medicines.json）'
    )
    
    args = parser.parse_args()
    
    # 入力ディレクトリの決定
    if args.pmda_directory is None:
        # 自動検出モード
        pmda_directory = auto_detect_pmda_directory()
    else:
        # 明示的指定モード
        pmda_directory = args.pmda_directory
        if not os.path.exists(pmda_directory):
            print(f"エラー: 指定されたディレクトリが存在しません: {pmda_directory}")
            print("\n使用方法:")
            print("1. 正しいディレクトリパスを指定してください")
            print("2. または引数を省略して自動検出を使用してください")
            return 1
        
        # SGML_XMLディレクトリの確認
        sgml_xml_path = os.path.join(pmda_directory, 'SGML_XML')
        if not os.path.exists(sgml_xml_path):
            print(f"エラー: SGML_XMLディレクトリが見つかりません: {sgml_xml_path}")
            print(f"指定されたディレクトリがPMDAデータディレクトリか確認してください。")
            return 1
    
    # JSONジェネレーターを作成して実行
    generator = PMDAJSONGenerator(pmda_directory, args.output)
    generator.generate()
    
    return 0

if __name__ == "__main__":
    exit(main())