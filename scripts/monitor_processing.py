#!/usr/bin/env python3
"""
処理進行状況を監視するスクリプト
"""

import time
import subprocess
import sys
from datetime import datetime

def get_processing_stats():
    """BigQueryから処理統計を取得"""
    try:
        # 処理済みレコード数を取得
        cmd = [
            "bq", "query", "--use_legacy_sql=false",
            "--format=csv", "--max_rows=1",
            "SELECT COUNT(*) as processed FROM `ai-sales-list.companies.enriched`"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                return int(lines[1])
        return 0
    except Exception as e:
        print(f"Error getting stats: {e}")
        return 0

def get_industry_stats(industry):
    """特定業界の処理統計を取得"""
    try:
        # 処理済みレコード数を取得
        cmd = [
            "bq", "query", "--use_legacy_sql=false",
            "--format=csv", "--max_rows=1",
            f"SELECT COUNT(*) as processed FROM `ai-sales-list.companies.enriched` WHERE industry = '{industry}'"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                return int(lines[1])
        return 0
    except Exception as e:
        print(f"Error getting industry stats: {e}")
        return 0

def get_total_companies(industry):
    """特定業界の総企業数を取得"""
    try:
        cmd = [
            "bq", "query", "--use_legacy_sql=false",
            "--format=csv", "--max_rows=1",
            f"SELECT COUNT(*) as total FROM `ai-sales-list.companies.raw` WHERE industry = '{industry}'"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                return int(lines[1])
        return 0
    except Exception as e:
        print(f"Error getting total companies: {e}")
        return 0

def monitor_processing(industry, check_interval=60):
    """処理進行状況を監視"""
    print(f"監視開始: {industry}業界")
    print(f"チェック間隔: {check_interval}秒")
    print("-" * 50)
    
    total_companies = get_total_companies(industry)
    print(f"総企業数: {total_companies:,}社")
    
    start_time = datetime.now()
    last_processed = 0
    
    while True:
        try:
            current_time = datetime.now()
            elapsed = current_time - start_time
            
            processed = get_industry_stats(industry)
            total_processed = get_processing_stats()
            
            if processed > last_processed:
                # 処理速度を計算
                if elapsed.total_seconds() > 0:
                    rate = processed / elapsed.total_seconds() * 60  # 社/分
                    remaining = total_companies - processed
                    if rate > 0:
                        eta_minutes = remaining / rate
                        eta_hours = eta_minutes / 60
                        print(f"[{current_time.strftime('%H:%M:%S')}] "
                              f"処理済み: {processed:,}/{total_companies:,}社 "
                              f"({processed/total_companies*100:.1f}%) "
                              f"速度: {rate:.1f}社/分 "
                              f"残り時間: {eta_hours:.1f}時間")
                    else:
                        print(f"[{current_time.strftime('%H:%M:%S')}] "
                              f"処理済み: {processed:,}/{total_companies:,}社 "
                              f"({processed/total_companies*100:.1f}%)")
                else:
                    print(f"[{current_time.strftime('%H:%M:%S')}] "
                          f"処理済み: {processed:,}/{total_companies:,}社 "
                          f"({processed/total_companies*100:.1f}%)")
                
                last_processed = processed
                
                # 完了チェック
                if processed >= total_companies:
                    print(f"\n🎉 {industry}業界の処理が完了しました！")
                    print(f"総処理時間: {elapsed}")
                    break
            else:
                print(f"[{current_time.strftime('%H:%M:%S')}] 処理中... (処理済み: {processed:,}社)")
            
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            print(f"\n監視を停止しました。")
            break
        except Exception as e:
            print(f"エラー: {e}")
            time.sleep(check_interval)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方法: python scripts/monitor_processing.py <業界名> [チェック間隔(秒)]")
        print("例: python scripts/monitor_processing.py 人材業界 60")
        sys.exit(1)
    
    industry = sys.argv[1]
    check_interval = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    monitor_processing(industry, check_interval)
