-- BigQuery schema for AI Sales List Enrichment

-- Create dataset
CREATE SCHEMA IF NOT EXISTS `companies`
OPTIONS (
  description = "Enterprise data enrichment dataset",
  location = "asia-northeast1"
);

-- Raw input table
CREATE TABLE IF NOT EXISTS `companies.raw` (
  website STRING NOT NULL,
  name STRING,
  industry STRING,
  prefecture STRING,
  inquiry_url STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
OPTIONS (
  description = "Raw input data from CSV files"
);

-- Enriched output table
CREATE TABLE IF NOT EXISTS `companies.enriched` (
  website STRING NOT NULL,
  name STRING,
  name_legal STRING,
  industry STRING,
  hq_address_raw STRING,
  prefecture_name STRING,
  overview_text STRING,
  services_text STRING,
  products_text STRING,
  pain_hypotheses ARRAY<STRING>,
  personalization_notes STRING,
  employee_count INT64,
  employee_count_source_url STRING,
  last_crawled_at TIMESTAMP,
  status STRING,
  signals JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(last_crawled_at)
CLUSTER BY industry, status
OPTIONS (
  description = "Enriched enterprise data with AI processing results"
);

-- Progress dashboard view
CREATE OR REPLACE VIEW `companies.progress_dashboard` AS
SELECT 
  industry,
  COUNT(*) as total_companies,
  SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as completed,
  SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) / COUNT(*) * 100 as completion_rate,
  AVG(ARRAY_LENGTH(pain_hypotheses)) as avg_hypotheses,
  SUM(CASE WHEN employee_count IS NULL THEN 1 ELSE 0 END) as missing_employees,
  SUM(CASE WHEN employee_count IS NOT NULL THEN 1 ELSE 0 END) as with_employees,
  AVG(employee_count) as avg_employee_count,
  MIN(last_crawled_at) as first_processed,
  MAX(last_crawled_at) as last_processed
FROM `companies.enriched`
GROUP BY industry
ORDER BY total_companies DESC;

-- Processing status view
CREATE OR REPLACE VIEW `companies.processing_status` AS
SELECT 
  status,
  COUNT(*) as count,
  COUNT(*) / SUM(COUNT(*)) OVER() * 100 as percentage,
  MIN(last_crawled_at) as first_occurrence,
  MAX(last_crawled_at) as last_occurrence
FROM `companies.enriched`
GROUP BY status
ORDER BY count DESC;

-- Quality metrics view
CREATE OR REPLACE VIEW `companies.quality_metrics` AS
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
FROM `companies.enriched`
WHERE status = 'ok'
GROUP BY industry
ORDER BY total DESC;

-- Recent processing activity view
CREATE OR REPLACE VIEW `companies.recent_activity` AS
SELECT 
  DATE(last_crawled_at) as processing_date,
  industry,
  status,
  COUNT(*) as count
FROM `companies.enriched`
WHERE last_crawled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY processing_date, industry, status
ORDER BY processing_date DESC, count DESC;

-- Error analysis view
CREATE OR REPLACE VIEW `companies.error_analysis` AS
SELECT 
  status,
  industry,
  COUNT(*) as error_count,
  ARRAY_AGG(DISTINCT JSON_EXTRACT_SCALAR(signals, '$.error') IGNORE NULLS) as error_types,
  MIN(last_crawled_at) as first_error,
  MAX(last_crawled_at) as last_error
FROM `companies.enriched`
WHERE status != 'ok'
GROUP BY status, industry
ORDER BY error_count DESC;
