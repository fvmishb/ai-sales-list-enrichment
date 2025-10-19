#!/usr/bin/env python3
"""
全てのCSVファイルをBigQueryスキーマに合わせて変換するスクリプト
"""

import os
import pandas as pd
import glob
from pathlib import Path

def convert_csv_to_bigquery_schema(input_file, output_file, industry):
    """CSVファイルをBigQueryスキーマに合わせて変換"""
    try:
        # CSVファイルを読み込み（エンコーディングを自動検出）
        encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(input_file, encoding=encoding)
                print(f"Successfully read {input_file} with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            print(f"Failed to read {input_file} with any encoding")
            return False
        
        # 列名をマッピング
        column_mapping = {
            '企業名': 'company_name',
            '都道府県': 'prefecture', 
            'URL': 'website',
            '問合せフォーム': 'inquiry_url',
            '備考': 'notes'
        }
        
        # 列名を変更
        df = df.rename(columns=column_mapping)
        
        # 必要な列が存在するかチェック
        required_columns = ['company_name', 'website']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing required columns in {input_file}: {missing_columns}")
            return False
        
        # 業界列を追加
        df['industry'] = industry
        
        # 欠損値を空文字列に置換
        df = df.fillna('')
        
        # 空の行を削除
        df = df.dropna(subset=['company_name', 'website'])
        df = df[df['company_name'].str.strip() != '']
        df = df[df['website'].str.strip() != '']
        
        # 出力ファイルを保存
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"Converted {input_file} -> {output_file} ({len(df)} records)")
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
