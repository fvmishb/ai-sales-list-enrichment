#!/usr/bin/env python3
"""Upload CSV files to BigQuery for processing."""

import os
import sys
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse
import logging

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_csv_to_bigquery(csv_file: str, industry: str = None) -> bool:
    """Upload CSV file to BigQuery raw table."""
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Read CSV file
        logger.info(f"Reading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        
        # Validate required columns
        required_columns = ['企業名', 'URL']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Rename columns to match BigQuery schema
        column_mapping = {
            '企業名': 'name',
            '都道府県': 'prefecture',
            'URL': 'website',
            '問合せフォーム': 'inquiry_url',
            '備考': 'notes'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Add industry column
        if industry:
            df['industry'] = industry
        else:
            # Extract industry from filename
            filename = os.path.basename(csv_file)
            industry = filename.replace('.csv', '')
            df['industry'] = industry
        
        # Select only the columns we need
        df = df[['name', 'industry', 'prefecture', 'website', 'inquiry_url']]
        
        # Clean data
        df = df.dropna(subset=['website'])  # Remove rows without website
        df = df.fillna('')  # Fill NaN with empty string
        
        # Validate website URLs
        df = df[df['website'].str.contains('http', na=False)]
        
        logger.info(f"Processed {len(df)} companies for industry: {industry}")
        
        # Upload to BigQuery
        table_id = f"{settings.gcp_project_id}.{settings.bq_dataset_id}.{settings.bq_raw_table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        logger.info(f"Successfully uploaded {len(df)} companies to BigQuery")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading CSV to BigQuery: {e}")
        return False


def upload_all_csvs(csv_directory: str) -> bool:
    """Upload all CSV files in directory to BigQuery."""
    try:
        csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
        
        if not csv_files:
            logger.warning(f"No CSV files found in {csv_directory}")
            return False
        
        logger.info(f"Found {len(csv_files)} CSV files to upload")
        
        success_count = 0
        for csv_file in csv_files:
            file_path = os.path.join(csv_directory, csv_file)
            industry = csv_file.replace('.csv', '')
            
            if upload_csv_to_bigquery(file_path, industry):
                success_count += 1
            else:
                logger.error(f"Failed to upload {csv_file}")
        
        logger.info(f"Successfully uploaded {success_count}/{len(csv_files)} CSV files")
        return success_count == len(csv_files)
        
    except Exception as e:
        logger.error(f"Error uploading CSV files: {e}")
        return False


def create_bigquery_tables() -> bool:
    """Create BigQuery tables if they don't exist."""
    try:
        client = bigquery.Client(project=settings.gcp_project_id)
        
        # Read schema file
        schema_file = os.path.join(os.path.dirname(__file__), '..', 'infrastructure', 'bigquery', 'schema.sql')
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Execute schema creation
        job = client.query(schema_sql)
        job.result()  # Wait for completion
        
        logger.info("BigQuery tables created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error creating BigQuery tables: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Upload CSV files to BigQuery')
    parser.add_argument('--csv-file', help='Single CSV file to upload')
    parser.add_argument('--csv-directory', help='Directory containing CSV files')
    parser.add_argument('--industry', help='Industry name for single CSV file')
    parser.add_argument('--create-tables', action='store_true', help='Create BigQuery tables')
    
    args = parser.parse_args()
    
    if args.create_tables:
        if create_bigquery_tables():
            logger.info("BigQuery tables created successfully")
        else:
            logger.error("Failed to create BigQuery tables")
            sys.exit(1)
    
    if args.csv_file:
        if upload_csv_to_bigquery(args.csv_file, args.industry):
            logger.info("CSV file uploaded successfully")
        else:
            logger.error("Failed to upload CSV file")
            sys.exit(1)
    
    elif args.csv_directory:
        if upload_all_csvs(args.csv_directory):
            logger.info("All CSV files uploaded successfully")
        else:
            logger.error("Failed to upload some CSV files")
            sys.exit(1)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
