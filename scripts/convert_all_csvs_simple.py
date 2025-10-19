#!/usr/bin/env python3
"""
全てのCSVファイルをBigQueryスキーマに合わせて変換するスクリプト（標準ライブラリ版）
"""

import os
import csv
import glob
from pathlib import Path

def convert_csv_to_bigquery_schema(input_file, output_file, industry):
    """CSVファイルをBigQueryスキーマに合わせて変換"""
    try:
        # エンコーディングを自動検出して読み込み
        encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']
        rows = None
        encoding_used = None
        
        for encoding in encodings:
            try:
                with open(input_file, 'r', encoding=encoding, newline='') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    encoding_used = encoding
                    print(f"Successfully read {input_file} with encoding: {encoding}")
                    break
            except UnicodeDecodeError:
                continue
        
        if rows is None:
            print(f"Failed to read {input_file} with any encoding")
            return False
        
        if len(rows) < 2:  # ヘッダー + 最低1行のデータ
            print(f"No data rows in {input_file}")
            return False
        
        # ヘッダー行を取得
        header = rows[0]
        print(f"Original header: {header}")
        
        # 列名をマッピング
        column_mapping = {
            '企業名': 'company_name',
            '\ufeff企業名': 'company_name',  # BOM付きの場合
            '都道府県': 'prefecture', 
            'URL': 'website',
            '問合せフォーム': 'inquiry_url',
            '備考': 'notes'
        }
        
        # ヘッダーを変換
        new_header = []
        for col in header:
            new_col = column_mapping.get(col, col)
            new_header.append(new_col)
        
        # 業界列を追加
        new_header.append('industry')
        
        # データ行を処理
        converted_rows = []
        for i, row in enumerate(rows[1:], 1):
            # 行の長さをヘッダーに合わせる
            while len(row) < len(header):
                row.append('')
            
            # 行を切り詰める（ヘッダーより長い場合）
            row = row[:len(header)]
            
            # 各セルの改行を削除
            cleaned_row = []
            for cell in row:
                if isinstance(cell, str):
                    # 改行文字を削除し、複数の空白を1つにまとめる
                    cleaned_cell = ' '.join(cell.replace('\n', ' ').replace('\r', ' ').split())
                    cleaned_row.append(cleaned_cell)
                else:
                    cleaned_row.append(cell)
            
            # 業界列を追加
            cleaned_row.append(industry)
            
            # 空の行をスキップ
            if not any(cell.strip() for cell in cleaned_row[:2]):  # company_name, website が空
                continue
            
            converted_rows.append(cleaned_row)
        
        # 出力ファイルを保存
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(new_header)
            writer.writerows(converted_rows)
        
        print(f"Converted {input_file} -> {output_file} ({len(converted_rows)} records)")
        return True
        
    except Exception as e:
        print(f"Error converting {input_file}: {e}")
        return False

def main():
    """メイン処理"""
    lists_dir = Path("lists")
    output_dir = Path("lists_converted")
    
    # 出力ディレクトリを作成
    output_dir.mkdir(exist_ok=True)
    
    # CSVファイルを取得
    csv_files = list(lists_dir.glob("*.csv"))
    
    if not csv_files:
        print("No CSV files found in lists directory")
        return
    
    print(f"Found {len(csv_files)} CSV files to convert")
    
    success_count = 0
    total_count = 0
    
    for csv_file in csv_files:
        total_count += 1
        
        # 業界名をファイル名から取得（拡張子を除く）
        industry = csv_file.stem
        
        # 出力ファイル名
        output_file = output_dir / f"{industry}_converted.csv"
        
        print(f"\nProcessing: {csv_file.name}")
        print(f"Industry: {industry}")
        
        if convert_csv_to_bigquery_schema(csv_file, output_file, industry):
            success_count += 1
        else:
            print(f"Failed to convert {csv_file.name}")
    
    print(f"\n=== Conversion Summary ===")
    print(f"Total files: {total_count}")
    print(f"Successfully converted: {success_count}")
    print(f"Failed: {total_count - success_count}")
    print(f"Output directory: {output_dir}")

if __name__ == "__main__":
    main()
