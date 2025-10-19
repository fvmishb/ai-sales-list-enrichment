#!/usr/bin/env python3
"""Monitor processing progress and statistics."""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_processing_stats() -> dict:
    """Get processing statistics from BigQuery."""
    try:
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Query processing statistics
        query = f"""
        SELECT 
          industry,
          COUNT(*) as total,
          SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as completed,
          SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) / COUNT(*) * 100 as completion_rate,
          AVG(ARRAY_LENGTH(pain_hypotheses)) as avg_hypotheses,
          SUM(CASE WHEN employee_count IS NULL THEN 1 ELSE 0 END) as missing_employees,
          AVG(employee_count) as avg_employee_count,
          MIN(last_crawled_at) as first_processed,
          MAX(last_crawled_at) as last_processed
        FROM `{settings.gcp_project_id}.{settings.bq_dataset_id}.{settings.bq_enriched_table_id}`
        GROUP BY industry
        ORDER BY total DESC
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        stats = []
        for row in results:
            stats.append({
                "industry": row.industry,
                "total": row.total,
                "completed": row.completed,
                "completion_rate": round(row.completion_rate, 2),
                "avg_hypotheses": round(row.avg_hypotheses, 2),
                "missing_employees": row.missing_employees,
                "avg_employee_count": round(row.avg_employee_count, 2) if row.avg_employee_count else 0,
                "first_processed": row.first_processed,
                "last_processed": row.last_processed
            })
        
        return {"by_industry": stats}
        
    except Exception as e:
        logger.error(f"Error getting processing stats: {e}")
        return {"error": str(e)}


async def get_error_analysis() -> dict:
    """Get error analysis from BigQuery."""
    try:
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Query error analysis
        query = f"""
        SELECT 
          status,
          industry,
          COUNT(*) as error_count,
          ARRAY_AGG(DISTINCT JSON_EXTRACT_SCALAR(signals, '$.error') IGNORE NULLS) as error_types,
          MIN(last_crawled_at) as first_error,
          MAX(last_crawled_at) as last_error
        FROM `{settings.gcp_project_id}.{settings.bq_dataset_id}.{settings.bq_enriched_table_id}`
        WHERE status != 'ok'
        GROUP BY status, industry
        ORDER BY error_count DESC
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        errors = []
        for row in results:
            errors.append({
                "status": row.status,
                "industry": row.industry,
                "error_count": row.error_count,
                "error_types": list(row.error_types),
                "first_error": row.first_error,
                "last_error": row.last_error
            })
        
        return {"errors": errors}
        
    except Exception as e:
        logger.error(f"Error getting error analysis: {e}")
        return {"error": str(e)}


async def get_recent_activity(hours: int = 24) -> dict:
    """Get recent processing activity."""
    try:
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Query recent activity
        query = f"""
        SELECT 
          DATE(last_crawled_at) as processing_date,
          industry,
          status,
          COUNT(*) as count
        FROM `{settings.gcp_project_id}.{settings.bq_dataset_id}.{settings.bq_enriched_table_id}`
        WHERE last_crawled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        GROUP BY processing_date, industry, status
        ORDER BY processing_date DESC, count DESC
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        activity = []
        for row in results:
            activity.append({
                "date": str(row.processing_date),
                "industry": row.industry,
                "status": row.status,
                "count": row.count
            })
        
        return {"recent_activity": activity}
        
    except Exception as e:
        logger.error(f"Error getting recent activity: {e}")
        return {"error": str(e)}


async def get_quality_metrics() -> dict:
    """Get data quality metrics."""
    try:
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Query quality metrics
        query = f"""
        SELECT 
          industry,
          COUNT(*) as total,
          -- Data completeness
          SUM(CASE WHEN hq_address_raw IS NOT NULL AND hq_address_raw != '' THEN 1 ELSE 0 END) / COUNT(*) * 100 as address_completeness,
          SUM(CASE WHEN employee_count IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100 as employee_completeness,
          SUM(CASE WHEN overview_text IS NOT NULL AND overview_text != '' THEN 1 ELSE 0 END) / COUNT(*) * 100 as overview_completeness,
          SUM(CASE WHEN ARRAY_LENGTH(pain_hypotheses) >= 3 THEN 1 ELSE 0 END) / COUNT(*) * 100 as hypotheses_completeness,
          -- Data quality
          AVG(LENGTH(overview_text)) as avg_overview_length,
          AVG(ARRAY_LENGTH(pain_hypotheses)) as avg_hypotheses_count,
          AVG(employee_count) as avg_employee_count
        FROM `{settings.gcp_project_id}.{settings.bq_dataset_id}.{settings.bq_enriched_table_id}`
        WHERE status = 'ok'
        GROUP BY industry
        ORDER BY total DESC
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        metrics = []
        for row in results:
            metrics.append({
                "industry": row.industry,
                "total": row.total,
                "address_completeness": round(row.address_completeness, 2),
                "employee_completeness": round(row.employee_completeness, 2),
                "overview_completeness": round(row.overview_completeness, 2),
                "hypotheses_completeness": round(row.hypotheses_completeness, 2),
                "avg_overview_length": round(row.avg_overview_length, 2),
                "avg_hypotheses_count": round(row.avg_hypotheses_count, 2),
                "avg_employee_count": round(row.avg_employee_count, 2) if row.avg_employee_count else 0
            })
        
        return {"quality_metrics": metrics}
        
    except Exception as e:
        logger.error(f"Error getting quality metrics: {e}")
        return {"error": str(e)}


def print_stats_table(stats: dict) -> None:
    """Print statistics in a table format."""
    if "by_industry" not in stats:
        print("No statistics available")
        return
    
    print("\n" + "="*100)
    print("PROCESSING STATISTICS BY INDUSTRY")
    print("="*100)
    print(f"{'Industry':<25} {'Total':<8} {'Completed':<10} {'Rate%':<8} {'Hypotheses':<12} {'Missing Emp':<12}")
    print("-"*100)
    
    for stat in stats["by_industry"]:
        print(f"{stat['industry']:<25} {stat['total']:<8} {stat['completed']:<10} "
              f"{stat['completion_rate']:<8.1f} {stat['avg_hypotheses']:<12.1f} {stat['missing_employees']:<12}")
    
    print("="*100)


def print_error_analysis(errors: dict) -> None:
    """Print error analysis."""
    if "errors" not in errors:
        print("No error analysis available")
        return
    
    print("\n" + "="*80)
    print("ERROR ANALYSIS")
    print("="*80)
    print(f"{'Status':<15} {'Industry':<20} {'Count':<8} {'Error Types':<30}")
    print("-"*80)
    
    for error in errors["errors"]:
        error_types = ", ".join(error["error_types"][:3])  # Show first 3 error types
        if len(error["error_types"]) > 3:
            error_types += "..."
        
        print(f"{error['status']:<15} {error['industry']:<20} {error['error_count']:<8} {error_types:<30}")
    
    print("="*80)


def print_quality_metrics(metrics: dict) -> None:
    """Print quality metrics."""
    if "quality_metrics" not in metrics:
        print("No quality metrics available")
        return
    
    print("\n" + "="*120)
    print("DATA QUALITY METRICS")
    print("="*120)
    print(f"{'Industry':<20} {'Total':<8} {'Addr%':<8} {'Emp%':<8} {'Overview%':<10} {'Hyp%':<8} {'Avg Len':<10} {'Avg Hyp':<10}")
    print("-"*120)
    
    for metric in metrics["quality_metrics"]:
        print(f"{metric['industry']:<20} {metric['total']:<8} "
              f"{metric['address_completeness']:<8.1f} {metric['employee_completeness']:<8.1f} "
              f"{metric['overview_completeness']:<10.1f} {metric['hypotheses_completeness']:<8.1f} "
              f"{metric['avg_overview_length']:<10.1f} {metric['avg_hypotheses_count']:<10.1f}")
    
    print("="*120)


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Monitor processing progress and statistics')
    parser.add_argument('--stats', action='store_true', help='Show processing statistics')
    parser.add_argument('--errors', action='store_true', help='Show error analysis')
    parser.add_argument('--quality', action='store_true', help='Show quality metrics')
    parser.add_argument('--activity', type=int, default=24, help='Show recent activity (hours)')
    parser.add_argument('--all', action='store_true', help='Show all metrics')
    
    args = parser.parse_args()
    
    if args.all or args.stats:
        print("Fetching processing statistics...")
        stats = await get_processing_stats()
        print_stats_table(stats)
    
    if args.all or args.errors:
        print("\nFetching error analysis...")
        errors = await get_error_analysis()
        print_error_analysis(errors)
    
    if args.all or args.quality:
        print("\nFetching quality metrics...")
        metrics = await get_quality_metrics()
        print_quality_metrics(metrics)
    
    if args.all or args.activity:
        print(f"\nFetching recent activity (last {args.activity} hours)...")
        activity = await get_recent_activity(args.activity)
        if "recent_activity" in activity:
            print(f"\nRecent Activity (last {args.activity} hours):")
            for act in activity["recent_activity"][:10]:  # Show last 10 activities
                print(f"  {act['date']} - {act['industry']} - {act['status']} - {act['count']} companies")
        else:
            print("No recent activity data available")


if __name__ == "__main__":
    asyncio.run(main())
