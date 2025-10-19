#!/usr/bin/env python3
"""CSVファイルの前処理スクリプト"""

import csv
import sys
import os
from pathlib import Path

def clean_csv_file(input_file, output_file):
    """CSVファイルをクリーンアップして出力"""
    with open(input_file, 'r', encoding='utf-8') as infile:
        content = infile.read()
    
    # 改行文字を統一
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # 行を分割
    lines = content.split('\n')
    
    # 空行を除去
    lines = [line for line in lines if line.strip()]
    
    # 出力ファイルに書き込み
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        
        for line in lines:
            # CSV行をパース
            try:
                reader = csv.reader([line])
                row = next(reader)
                writer.writerow(row)
            except Exception as e:
                print(f"Warning: Skipping malformed line: {line[:100]}...")
                continue

def main():
    if len(sys.argv) != 3:
        print("Usage: python preprocess_csv.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        sys.exit(1)
    
    clean_csv_file(input_file, output_file)
    print(f"Cleaned CSV saved to: {output_file}")

if __name__ == "__main__":
    main()
