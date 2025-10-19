#!/usr/bin/env python3
"""
全ての業界のCSVファイルをBigQueryにアップロードするスクリプト
"""

import os
import subprocess
from pathlib import Path

def upload_csv_to_bigquery(csv_file, table_name="companies.raw"):
    """CSVファイルをBigQueryにアップロード"""
    try:
        cmd = [
            "bq", "load",
            "--source_format=CSV",
            "--skip_leading_rows=1",
            f"ai-sales-list:{table_name}",
            str(csv_file)
        ]
        
        print(f"Uploading {csv_file.name}...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0 or "Upload complete" in result.stdout or "DONE" in result.stdout:
            print(f"✅ Successfully uploaded {csv_file.name}")
            return True
        else:
            print(f"❌ Failed to upload {csv_file.name}: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error uploading {csv_file.name}: {e}")
        return False

def main():
    """メイン処理"""
    converted_dir = Path("lists_converted")
    
    if not converted_dir.exists():
        print("lists_converted directory not found. Please run convert_all_csvs_simple.py first.")
        return
    
    # 変換されたCSVファイルを取得
    csv_files = list(converted_dir.glob("*_converted.csv"))
    
    if not csv_files:
        print("No converted CSV files found")
        return
    
    print(f"Found {len(csv_files)} converted CSV files to upload")
    
    success_count = 0
    total_count = 0
    
    for csv_file in csv_files:
        total_count += 1
        print(f"\n[{total_count}/{len(csv_files)}] Processing: {csv_file.name}")
        
        if upload_csv_to_bigquery(csv_file):
            success_count += 1
    
    print(f"\n=== Upload Summary ===")
    print(f"Total files: {total_count}")
    print(f"Successfully uploaded: {success_count}")
    print(f"Failed: {total_count - success_count}")
    
    if success_count > 0:
        print(f"\n✅ Upload completed! Check BigQuery table: ai-sales-list:companies.raw")

if __name__ == "__main__":
    main()
