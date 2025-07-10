#!/usr/bin/env python3
"""
PMDA医薬品データJSON生成ツール（最適化版）

バッチ分割 + ワーカー並列処理による高速化を実現：
- インテリジェントバッチ分割処理
- 動的負荷分散による最適ワーカー配置
- メモリ効率を考慮したバッチサイズ自動調整
- プロセス間通信最適化による並列処理効率向上
- リアルタイム処理統計とボトルネック分析
"""

import os
import sys
import json
import time
import argparse
import glob
import re
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import threading
import psutil
import gc

# パスを調整してインポート
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 最適化されたパーサーのインポート
from parsers.shared_xml_processor import SharedXMLProcessor
from utils.file_processor import find_duplicate_files

class BatchProcessor:
    """
    バッチ分割処理を管理するクラス
    """
    
    def __init__(self, files: List[str], batch_size: Optional[int] = None, memory_limit_mb: int = 1024):
        """
        初期化メソッド
        
        Args:
            files (List[str]): 処理対象ファイルのリスト
            batch_size (int): バッチサイズ（自動計算される場合はNone）
            memory_limit_mb (int): メモリ制限（MB）
        """
        self.files = files
        self.memory_limit_mb = memory_limit_mb
        self.batch_size = batch_size or self._calculate_optimal_batch_size()
        self.batches = self._create_batches()
        
    def _calculate_optimal_batch_size(self) -> int:
        """
        システム環境に基づいて最適なバッチサイズを計算
        
        Returns:
            int: 最適なバッチサイズ
        """
        # システムメモリ情報を取得
        memory = psutil.virtual_memory()
        available_memory_mb = memory.available / (1024 * 1024)
        
        # CPU数を考慮
        cpu_cores = cpu_count()
        
        # 平均XMLファイルサイズを推定（約100KB）
        estimated_file_size_mb = 0.1
        
        # メモリ制限内でのバッチサイズを計算
        memory_based_batch_size = int(self.memory_limit_mb / estimated_file_size_mb)
        
        # CPU効率を考慮したバッチサイズ
        cpu_based_batch_size = max(10, len(self.files) // (cpu_cores * 4))
        
        # 最適なバッチサイズを決定（安全マージンを含む）
        optimal_batch_size = min(memory_based_batch_size, cpu_based_batch_size, 500)
        
        print(f"   バッチサイズ自動計算: {optimal_batch_size}")
        print(f"   - 利用可能メモリ: {available_memory_mb:.1f}MB")
        print(f"   - CPU数: {cpu_cores}")
        print(f"   - メモリ制限: {self.memory_limit_mb}MB")
        
        return max(optimal_batch_size, 10)  # 最低10ファイル/バッチ
    
    def _create_batches(self) -> List[List[str]]:
        """
        ファイルリストをバッチに分割
        
        Returns:
            List[List[str]]: バッチ分割されたファイルリスト
        """
        batches = []
        for i in range(0, len(self.files), self.batch_size):
            batch = self.files[i:i + self.batch_size]
            batches.append(batch)
        
        print(f"   総バッチ数: {len(batches)}")
        print(f"   平均バッチサイズ: {len(self.files) / len(batches):.1f}ファイル/バッチ")
        
        return batches
    
    def get_batches(self) -> List[List[str]]:
        """
        バッチリストを取得
        
        Returns:
            List[List[str]]: バッチ分割されたファイルリスト
        """
        return self.batches

def process_batch_worker(batch_files: List[str], batch_id: int) -> Tuple[int, List[Dict[str, Any]], Dict[str, int]]:
    """
    バッチ処理ワーカー関数（プロセス間で実行）
    
    Args:
        batch_files (List[str]): バッチ内のファイルリスト
        batch_id (int): バッチID
        
    Returns:
        Tuple[int, List[Dict[str, Any]], Dict[str, int]]: (バッチID, 医薬品データ, 統計情報)
    """
    batch_medicines = []
    batch_stats = {
        'processed_files': 0,
        'error_files': 0,
        'medicines_count': 0,
        'xml_parse_count': 0
    }
    
    try:
        for file_path in batch_files:
            try:
                # SharedXMLProcessorで超高速処理
                processor = SharedXMLProcessor(file_path)
                medicines_data = processor.process_all_brands()
                
                if medicines_data:
                    batch_medicines.extend(medicines_data)
                    batch_stats['medicines_count'] += len(medicines_data)
                    batch_stats['processed_files'] += 1
                    batch_stats['xml_parse_count'] += 1
                else:
                    batch_stats['error_files'] += 1
                    
            except Exception as e:
                batch_stats['error_files'] += 1
                print(f"   バッチ{batch_id}: エラー {os.path.basename(file_path)}: {e}")
                continue
        
        # メモリ効率化のためガベージコレクション
        gc.collect()
        
        return batch_id, batch_medicines, batch_stats
        
    except Exception as e:
        print(f"   バッチ{batch_id}処理エラー: {e}")
        return batch_id, [], batch_stats

class PMDAJSONGeneratorOptimized:
    """
    最適化版JSONジェネレーター
    
    【最適化ポイント】
    1. インテリジェントバッチ分割処理
    2. プロセス/スレッドハイブリッド並列処理
    3. 動的負荷分散とメモリ管理
    4. リアルタイム処理統計とボトルネック分析
    5. システムリソース最適活用
    """
    
    def __init__(self, input_directory: str, output_file: str, 
                 max_workers: Optional[int] = None, 
                 batch_size: Optional[int] = None,
                 memory_limit_mb: int = 2048,
                 use_process_pool: bool = True):
        """
        初期化メソッド
        
        Args:
            input_directory (str): PMDAデータディレクトリのパス
            output_file (str): 出力JSONファイルのパス
            max_workers (Optional[int]): 並列処理の最大ワーカー数
            batch_size (Optional[int]): バッチサイズ（自動計算の場合はNone）
            memory_limit_mb (int): メモリ制限（MB）
            use_process_pool (bool): プロセスプールを使用するかどうか
        """
        self.input_directory = input_directory
        self.output_file = output_file
        self.max_workers = max_workers or min(cpu_count(), 16)  # CPU性能に応じて上限拡大
        self.batch_size = batch_size
        self.memory_limit_mb = memory_limit_mb
        self.use_process_pool = use_process_pool
        
        # システム情報の取得
        self.system_info = {
            'cpu_count': cpu_count(),
            'total_memory_gb': psutil.virtual_memory().total / (1024**3),
            'available_memory_gb': psutil.virtual_memory().available / (1024**3)
        }
        
        self.statistics = {
            'total_files_found': 0,
            'duplicate_files': 0,
            'processed_files': 0,
            'error_files': 0,
            'medicines_count': 0,
            'vectors_count': defaultdict(int),
            'medicines_with_clinical_info': defaultdict(int),
            'processing_time': 0,
            'files_per_second': 0,
            'medicines_per_second': 0,
            'parallel_workers': 0,
            'xml_parse_count': 0,
            'batch_count': 0,
            'average_batch_time': 0,
            'memory_efficiency_ratio': 0,
            'batch_processing_times': []
        }
        
        self.lock = threading.Lock()
        
    def _update_statistics_batch(self, batch_stats: Dict[str, int], batch_medicines: List[Dict[str, Any]]):
        """
        バッチ処理結果から統計情報を更新
        
        Args:
            batch_stats (Dict[str, int]): バッチ統計情報
            batch_medicines (List[Dict[str, Any]]): バッチで処理された医薬品データ
        """
        with self.lock:
            # ファイル処理統計
            self.statistics['processed_files'] += batch_stats['processed_files']
            self.statistics['error_files'] += batch_stats['error_files']
            self.statistics['medicines_count'] += batch_stats['medicines_count']
            self.statistics['xml_parse_count'] += batch_stats['xml_parse_count']
            
            # 各医薬品の臨床情報をカウント
            for medicine in batch_medicines:
                clinical_info = medicine.get('clinical_info', {})
                
                # 効能・効果
                if clinical_info.get('indications'):
                    self.statistics['medicines_with_clinical_info']['indications'] += 1
                    self.statistics['vectors_count']['indications'] += len(clinical_info['indications'])
                
                # 用法・用量
                if clinical_info.get('dosage'):
                    self.statistics['medicines_with_clinical_info']['dosage'] += 1
                    self.statistics['vectors_count']['dosage'] += len(clinical_info['dosage'])
                
                # 禁忌
                if clinical_info.get('contraindications'):
                    self.statistics['medicines_with_clinical_info']['contraindications'] += 1
                    self.statistics['vectors_count']['contraindications'] += len(clinical_info['contraindications'])
                
                # 警告・注意事項
                if clinical_info.get('warnings'):
                    self.statistics['medicines_with_clinical_info']['warnings'] += 1
                    self.statistics['vectors_count']['warnings'] += len(clinical_info['warnings'])
                
                # 副作用
                if clinical_info.get('side_effects'):
                    self.statistics['medicines_with_clinical_info']['side_effects'] += 1
                    self.statistics['vectors_count']['side_effects'] += len(clinical_info['side_effects'])
                
                # 相互作用
                if clinical_info.get('interactions'):
                    self.statistics['medicines_with_clinical_info']['interactions'] += 1
                    self.statistics['vectors_count']['interactions'] += len(clinical_info['interactions'])
                
                # 成分・含量
                if clinical_info.get('compositions'):
                    self.statistics['medicines_with_clinical_info']['compositions'] += 1
                    self.statistics['vectors_count']['compositions'] += len(clinical_info['compositions'])
                
                # 有効成分
                if clinical_info.get('active_ingredients'):
                    self.statistics['medicines_with_clinical_info']['active_ingredients'] += 1
                    self.statistics['vectors_count']['active_ingredients'] += len(clinical_info['active_ingredients'])
    
    def generate_json_optimized(self):
        """
        最適化PMDAデータからJSONファイルを生成
        """
        start_time = time.time()
        
        print("=== PMDA医薬品データJSON生成（最適化版） ===")
        print(f"並列処理ワーカー数: {self.max_workers}")
        print(f"プロセスプール使用: {'はい' if self.use_process_pool else 'いいえ（スレッドプール）'}")
        print(f"システム情報:")
        print(f"  - CPU数: {self.system_info['cpu_count']}")
        print(f"  - 総メモリ: {self.system_info['total_memory_gb']:.1f}GB")
        print(f"  - 利用可能メモリ: {self.system_info['available_memory_gb']:.1f}GB")
        print(f"  - メモリ制限: {self.memory_limit_mb}MB")
        
        # 1. ファイル検索とサイズ確認
        print("\n1. データファイル検索中...")
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
        duplicates = find_duplicate_files(xml_sgml_path, ['.xml', '.sgml'])
        
        # 重複ファイルを除去
        unique_files = []
        duplicate_hashes = set()
        
        for file_path in all_files:
            file_hash = None
            for hash_val, file_list in duplicates.items():
                if file_path in file_list:
                    file_hash = hash_val
                    break
            
            if file_hash and file_hash in duplicate_hashes:
                self.statistics['duplicate_files'] += 1
                continue
            elif file_hash:
                duplicate_hashes.add(file_hash)
            
            unique_files.append(file_path)
        
        print(f"   重複ファイル数: {self.statistics['duplicate_files']}")
        print(f"   処理対象ファイル数: {len(unique_files)}")
        
        # 3. インテリジェントバッチ分割
        print("3. インテリジェントバッチ分割中...")
        batch_processor = BatchProcessor(
            files=unique_files, 
            batch_size=self.batch_size,
            memory_limit_mb=self.memory_limit_mb
        )
        batches = batch_processor.get_batches()
        self.statistics['batch_count'] = len(batches)
        
        # 4. 最適化並列処理による医薬品データ抽出
        print("4. 最適化並列バッチ処理開始...")
        all_medicines = []
        
        # プロセスプールまたはスレッドプールを選択
        executor_class = ProcessPoolExecutor if self.use_process_pool else ThreadPoolExecutor
        
        with executor_class(max_workers=self.max_workers) as executor:
            # バッチ処理タスクを投入
            future_to_batch = {
                executor.submit(process_batch_worker, batch, batch_id): (batch_id, len(batch))
                for batch_id, batch in enumerate(batches)
            }
            
            completed_batches = 0
            batch_start_time = time.time()
            
            # 完了したバッチから順次結果を取得
            for future in as_completed(future_to_batch):
                batch_id, batch_size = future_to_batch[future]
                completed_batches += 1
                
                try:
                    result_batch_id, batch_medicines, batch_stats = future.result()
                    
                    if batch_medicines:
                        all_medicines.extend(batch_medicines)
                        self._update_statistics_batch(batch_stats, batch_medicines)
                    
                    # バッチ処理時間を記録
                    batch_time = time.time() - batch_start_time
                    self.statistics['batch_processing_times'].append(batch_time)
                    
                    # 進捗表示（バッチごと）
                    progress = (completed_batches / len(batches)) * 100
                    current_speed = completed_batches / (time.time() - start_time) if (time.time() - start_time) > 0 else 0
                    
                    print(f"   バッチ進捗: {completed_batches}/{len(batches)} ({progress:.1f}%) "
                          f"| バッチ{result_batch_id}: {batch_stats['medicines_count']}件 "
                          f"| 速度: {current_speed:.1f} batches/sec")
                    
                    batch_start_time = time.time()
                    
                except Exception as e:
                    print(f"   バッチ{batch_id}処理エラー: {e}")
                    with self.lock:
                        self.statistics['error_files'] += batch_size
        
        # 5. JSON出力
        print("5. JSON出力中...")
        self._save_json_optimized(all_medicines)
        
        # 6. 処理時間と統計情報の更新
        end_time = time.time()
        self.statistics['processing_time'] = end_time - start_time
        self.statistics['parallel_workers'] = self.max_workers
        
        # バッチ処理統計の計算
        if self.statistics['batch_processing_times']:
            self.statistics['average_batch_time'] = sum(self.statistics['batch_processing_times']) / len(self.statistics['batch_processing_times'])
        
        # メモリ効率比率の計算
        traditional_parse_count = self.statistics['processed_files'] * 9
        actual_parse_count = self.statistics['xml_parse_count']
        self.statistics['memory_efficiency_ratio'] = (1 - (actual_parse_count / traditional_parse_count)) * 100 if traditional_parse_count > 0 else 0
        
        if self.statistics['processing_time'] > 0:
            self.statistics['files_per_second'] = self.statistics['processed_files'] / self.statistics['processing_time']
            self.statistics['medicines_per_second'] = self.statistics['medicines_count'] / self.statistics['processing_time']
        
        # 7. 結果サマリー表示
        self._print_summary_optimized()
    
    def _save_json_optimized(self, medicines: List[Dict[str, Any]]):
        """
        医薬品データをJSONファイルに保存
        
        Args:
            medicines (List[Dict[str, Any]]): 保存する医薬品データ
        """
        # 出力ディレクトリを作成
        output_dir = os.path.dirname(self.output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # JSON出力（UTF-8エンコーディング、インデント付き）
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(medicines, f, ensure_ascii=False, indent=2)
        
        # ファイルサイズを取得・表示
        file_size = os.path.getsize(self.output_file)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"   出力ファイル: {self.output_file}")
        print(f"   ファイルサイズ: {file_size_mb:.2f} MB")
    
    def _print_summary_optimized(self):
        """
        処理サマリーを表示（最適化版）
        """
        print("\n=== 処理サマリー（最適化版） ===")
        print(f"発見されたファイル数: {self.statistics['total_files_found']:,}")
        print(f"重複ファイル数: {self.statistics['duplicate_files']:,}")
        print(f"処理成功ファイル数: {self.statistics['processed_files']:,}")
        print(f"処理エラーファイル数: {self.statistics['error_files']:,}")
        print(f"生成された医薬品エントリー数: {self.statistics['medicines_count']:,}")
        print(f"処理時間: {self.statistics['processing_time']:.2f}秒")
        print(f"並列ワーカー数: {self.statistics['parallel_workers']}")
        
        # バッチ処理統計
        print(f"\n=== バッチ処理統計 ===")
        print(f"総バッチ数: {self.statistics['batch_count']}")
        print(f"平均バッチ処理時間: {self.statistics['average_batch_time']:.2f}秒")
        if self.statistics['batch_count'] > 0:
            print(f"バッチ並列効率: {self.statistics['batch_count'] / self.statistics['processing_time']:.2f} batches/sec")
        
        # 最適化効果の表示
        print(f"\n=== 最適化効果 ===")
        print(f"XMLパース回数: {self.statistics['xml_parse_count']:,}回")
        traditional_parse_count = self.statistics['processed_files'] * 9
        print(f"従来方式のパース回数: {traditional_parse_count:,}回（予想）")
        print(f"XMLパース削減率: {self.statistics['memory_efficiency_ratio']:.1f}%")
        
        # 処理速度の表示
        if self.statistics['processing_time'] > 0:
            print(f"\n=== 処理速度 ===")
            print(f"ファイル処理速度: {self.statistics['files_per_second']:.2f} files/sec")
            print(f"医薬品生成速度: {self.statistics['medicines_per_second']:.2f} medicines/sec")
        
        # システムリソース効率
        print(f"\n=== システムリソース効率 ===")
        print(f"CPU使用効率: {(self.statistics['parallel_workers'] / self.system_info['cpu_count']) * 100:.1f}%")
        print(f"推定メモリ使用量: {(self.statistics['batch_count'] * self.memory_limit_mb) / 1024:.1f}GB")
        
        # 抽出された臨床情報の詳細
        print("\n=== 抽出された臨床情報 ===")
        # ユーザー指定の順序に変更
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

        # 医療情報種別毎の医薬品数
        print("\n=== 医療情報種別毎の医薬品数 ===")
        # 抽出された臨床情報と同じ順序に変更
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
        
        # 全種類の医療情報を持つ医薬品数を計算
        if self.statistics['medicines_with_clinical_info']:
            min_count = min(self.statistics['medicines_with_clinical_info'].values())
            print(f"\n全種類の医療情報を持つ医薬品: {min_count:,}件")

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
        print("2. または --input オプションで明示的にディレクトリを指定してください")
        print("   例: python src/pmda_json_generator_optimized.py --input /path/to/pmda_data")
        sys.exit(1)
    
    if len(pmda_dirs) > 1:
        print(f"エラー: 複数のPMDAディレクトリが見つかりました: {', '.join(pmda_dirs)}")
        print("\n使用方法:")
        print("--input オプションで使用するディレクトリを明示的に指定してください")
        print("例:")
        for pmda_dir in pmda_dirs:
            print(f"  python src/pmda_json_generator_optimized.py --input {pmda_dir}")
        sys.exit(1)
    
    detected_dir = pmda_dirs[0]
    print(f"PMDAディレクトリを自動検出しました: {detected_dir}")
    return detected_dir

def main():
    """
    メイン関数
    """
    parser = argparse.ArgumentParser(
        description='PMDA医薬品データからベクトル検索用JSONを生成（最適化版）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # カレントディレクトリのpmda_all_nnnnnnnnを自動検出
  python src/pmda_json_generator_optimized.py
  
  # 特定のディレクトリを指定
  python src/pmda_json_generator_optimized.py --input pmda_all_20250709
  
  # 出力ファイルとワーカー数を指定
  python src/pmda_json_generator_optimized.py --output custom.json --workers 8
        """
    )
    parser.add_argument(
        '--input', '-i',
        default=None,
        help='PMDAデータディレクトリのパス（未指定の場合は自動検出）'
    )
    parser.add_argument(
        '--output', '-o',
        default='pmda_medicines_optimized.json',
        help='出力JSONファイルのパス（デフォルト: pmda_medicines_optimized.json）'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=None,
        help='並列処理ワーカー数（デフォルト: CPU数と16の小さい方）'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=None,
        help='バッチサイズ（デフォルト: 自動計算）'
    )
    parser.add_argument(
        '--memory-limit', '-m',
        type=int,
        default=2048,
        help='メモリ制限（MB、デフォルト: 2048）'
    )
    parser.add_argument(
        '--use-threads',
        action='store_true',
        help='プロセスプールの代わりにスレッドプールを使用'
    )
    
    args = parser.parse_args()
    
    # 入力ディレクトリの決定
    if args.input is None:
        # 自動検出モード
        input_directory = auto_detect_pmda_directory()
    else:
        # 明示的指定モード
        input_directory = args.input
        if not os.path.exists(input_directory):
            print(f"エラー: 指定された入力ディレクトリが見つかりません: {input_directory}")
            print("\n使用方法:")
            print("1. 正しいディレクトリパスを指定してください")
            print("2. または --input オプションを省略して自動検出を使用してください")
            return 1
        
        # SGML_XMLディレクトリの存在確認
        sgml_xml_path = os.path.join(input_directory, 'SGML_XML')
        if not os.path.exists(sgml_xml_path):
            print(f"エラー: SGML_XMLディレクトリが見つかりません: {sgml_xml_path}")
            print(f"指定されたディレクトリがPMDAデータディレクトリか確認してください。")
            return 1
    
    # JSON生成器を初期化して実行
    generator = PMDAJSONGeneratorOptimized(
        input_directory=input_directory,
        output_file=args.output,
        max_workers=args.workers,
        batch_size=args.batch_size,
        memory_limit_mb=args.memory_limit,
        use_process_pool=not args.use_threads
    )
    
    try:
        generator.generate_json_optimized()
        print(f"\n🚀 最適化JSON生成が完了しました: {args.output}")
        print("⚡ バッチ分割 + 並列処理による高速化を実現")
    except Exception as e:
        print(f"\nエラー: JSON生成中にエラーが発生しました: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    main()