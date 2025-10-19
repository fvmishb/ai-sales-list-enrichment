#!/usr/bin/env python3
"""CSVファイルの改行問題を修正するスクリプト"""

import csv
import sys
import os
import re

def fix_csv_file(input_file, output_file):
    """CSVファイルの改行問題を修正"""
    with open(input_file, 'r', encoding='utf-8') as infile:
        content = infile.read()
    
    # 改行文字を統一
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # 企業名の改行を修正（企業名の後に改行がある場合）
    # パターン: "企業名\n", → "企業名",
    content = re.sub(r'"([^"]+)\n",', r'"\1",', content)
    
    # 行を分割
    lines = content.split('\n')
    
    # 空行を除去
    lines = [line.strip() for line in lines if line.strip()]
    
    # 出力ファイルに書き込み
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        
        for i, line in enumerate(lines):
            if i == 0:  # ヘッダー行
                writer.writerow(['企業名', '都道府県', 'URL', '問合せフォーム', '備考'])
                continue
            
            # CSV行をパース
            try:
                # 手動でCSVをパース（より柔軟に）
                if line.startswith('"') and line.count('"') >= 2:
                    # 引用符で囲まれた行
                    parts = []
                    current_part = ""
                    in_quotes = False
                    
                    for char in line:
                        if char == '"':
                            in_quotes = not in_quotes
                        elif char == ',' and not in_quotes:
                            parts.append(current_part.strip())
                            current_part = ""
                            continue
                        current_part += char
                    
                    if current_part:
                        parts.append(current_part.strip())
                    
                    # 5列に調整
                    while len(parts) < 5:
                        parts.append("")
                    
                    writer.writerow(parts[:5])
                else:
                    # 通常のCSV行
                    reader = csv.reader([line])
                    row = next(reader)
                    while len(row) < 5:
                        row.append("")
                    writer.writerow(row[:5])
                    
            except Exception as e:
                print(f"Warning: Skipping malformed line {i+1}: {line[:100]}...")
                continue

def main():
    if len(sys.argv) != 3:
        print("Usage: python fix_csv.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found")
        sys.exit(1)
    
    fix_csv_file(input_file, output_file)
    print(f"Fixed CSV saved to: {output_file}")

if __name__ == "__main__":
    main()
