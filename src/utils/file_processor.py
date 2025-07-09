import os
import hashlib
from typing import Dict, List, Tuple

def calculate_file_hash(file_path: str) -> str:
    """
    ファイルのハッシュ値を計算する
    
    Args:
        file_path (str): ハッシュ値を計算するファイルのパス
    
    Returns:
        str: ファイルのSHA-256ハッシュ値
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # チャンクで読み込むことで大きなファイルにも対応
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def find_duplicate_files(directory: str, extensions: List[str] = ['.pdf', '.xml', '.sgml']) -> Dict[str, List[str]]:
    """
    指定されたディレクトリ内のファイルを重複チェックする
    
    Args:
        directory (str): チェックするディレクトリのパス
        extensions (List[str], optional): チェック対象の拡張子のリスト
    
    Returns:
        Dict[str, List[str]]: 重複ファイルのハッシュとファイルパスの辞書
    """
    file_hashes = {}
    duplicates = {}

    for root, _, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            
            # 指定された拡張子のファイルのみ処理
            if any(filename.lower().endswith(ext) for ext in extensions):
                file_hash = calculate_file_hash(filepath)
                
                if file_hash in file_hashes:
                    # 重複ファイルを見つけた場合
                    if file_hash not in duplicates:
                        duplicates[file_hash] = [file_hashes[file_hash]]
                    duplicates[file_hash].append(filepath)
                else:
                    file_hashes[file_hash] = filepath

    return duplicates

def detect_parse_candidates(directory: str, 
                             file_extensions: List[str] = ['.xml', '.sgml'], 
                             ignore_duplicates: bool = True) -> List[Tuple[str, str]]:
    """
    パース対象のファイルを検出する
    
    Args:
        directory (str): 検索するディレクトリのパス
        file_extensions (List[str], optional): パース対象の拡張子のリスト
        ignore_duplicates (bool, optional): 重複ファイルを無視するかどうか
    
    Returns:
        List[Tuple[str, str]]: (ファイルパス, ファイル名)のリスト
    """
    candidates = []
    processed_hashes = set()
    
    # 重複ファイルを事前に検出
    duplicates = find_duplicate_files(directory, file_extensions) if ignore_duplicates else {}
    
    for root, _, files in os.walk(directory):
        for filename in files:
            if any(filename.lower().endswith(ext) for ext in file_extensions):
                filepath = os.path.join(root, filename)
                
                if ignore_duplicates:
                    # 重複ファイルの場合、最初のファイルのみを追加
                    file_hash = calculate_file_hash(filepath)
                    if file_hash not in processed_hashes:
                        candidates.append((filepath, filename))
                        processed_hashes.add(file_hash)
                else:
                    candidates.append((filepath, filename))
    
    return candidates

def main():
    base_dir = '/Users/tokuyama/workspace/pmda-parse/pmda_all_20250629/SGML_XML'
    
    # 重複XMLファイルの検出
    print("重複XMLファイルの検出:")
    duplicates = find_duplicate_files(base_dir, ['.xml', '.sgml'])
    for hash_val, files in duplicates.items():
        print(f"Hash: {hash_val}")
        for file in files:
            print(f"  - {file}")
    
    # パース対象XMLファイルの検出
    print("\nパース対象XMLファイルの検出:")
    candidates = detect_parse_candidates(base_dir)
    for filepath, filename in candidates:
        print(f"  - {filename}")

if __name__ == "__main__":
    main()