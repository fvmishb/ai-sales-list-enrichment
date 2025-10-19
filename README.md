# AI Sales List Enrichment

AI-powered enterprise data enrichment pipeline using Google Custom Search API, Gemini 2.5 Flash, and GPT-5-mini to process 40,000+ companies with high-quality business intelligence.

## 🚀 Features

- **Multi-phase Processing**: Site-specific URL discovery → Element extraction → AI formatting
- **Rate Limiting**: Domain-based throttling (1-2 req/s per domain) with global controls
- **Scalable Architecture**: Cloud Run + Cloud Tasks + Pub/Sub + BigQuery
- **Cost Optimized**: ~$0.032 per company (GPT-5-mini pricing)
- **High Quality**: 200-400 character overviews, pain hypotheses, personalization notes
- **Monitoring**: Real-time progress tracking and quality metrics

## 📊 Expected Output

For each company, the system extracts:

- **Basic Info**: Legal name, industry, headquarters address, prefecture
- **Business Intelligence**: 200-400 character overview, services/products lists
- **Pain Hypotheses**: 3-5 industry-specific business challenges
- **Personalization Notes**: 1-3 lines for sales team approach
- **Employee Data**: Headcount with source URL
- **Metadata**: Processing status, timestamps, quality signals

## 🏗️ Architecture

```
CSV Files → BigQuery Raw → SimpleProcessor → Google Custom Search + Gemini 2.5 Flash + GPT-5-mini → BigQuery Enriched
```

### Processing Pipeline
1. **Google Custom Search API**: Comprehensive company information search
   - Searches for company details, addresses, employee counts
   - Extracts data from search results and linked pages
   - Handles Japanese company information effectively

2. **Gemini 2.5 Flash**: Lightweight information extraction
   - Processes HTML content from official websites
   - Extracts structured data (addresses, services, products)
   - Generates business intelligence insights

3. **GPT-5-mini**: Final formatting and synthesis
   - Synthesizes data from multiple sources
   - Generates pain hypotheses based on industry + size + news
   - Creates personalization notes and validates data

## 💰 Cost Breakdown (40,000 companies)

| Component | Cost | Notes |
|-----------|------|-------|
| **Google Custom Search API** | $400 | 100,000 queries @ $0.004/query |
| **Gemini 2.5 Flash** | $200 | 20M tokens @ $0.01/1K tokens |
| **GPT-5-mini** | $31 | 40M tokens @ $0.25/$2.00 per 1M |
| **GCP Infrastructure** | $32 | Cloud Run, BigQuery, Tasks, Pub/Sub |
| **Total** | **$663** | **$0.017 per company** |

## 🚀 Quick Start

### 1. Prerequisites

- GCP Project with billing enabled
- `gcloud` CLI installed and authenticated
- `terraform` installed
- `docker` installed
- Python 3.11+

### 2. Deploy Infrastructure

```bash
# Clone and setup
git clone <repository>
cd ai-sales-list

# Deploy everything
./infrastructure/deploy.sh your-project-id asia-northeast1 prod
```

### 3. Configure API Keys

```bash
# Set Google Custom Search API key and CSE ID
echo 'your-google-search-api-key' | gcloud secrets versions add google-search-api-key --data-file=-
echo 'your-google-cse-id' | gcloud secrets versions add google-cse-id --data-file=-

# Set OpenAI API key  
echo 'your-openai-api-key' | gcloud secrets versions add openai-api-key --data-file=-

# Set Gemini API key (optional, uses Vertex AI by default)
echo 'your-gemini-api-key' | gcloud secrets versions add gemini-api-key --data-file=-
```

### 4. Upload Data

```bash
# Upload all CSV files
python3 scripts/upload_to_bq.py --csv-directory lists/

# Or upload specific industry
python3 scripts/upload_to_bq.py --csv-file lists/IT・web.csv --industry "IT・web"
```

### 5. Start Processing

```bash
# Process specific industry (1,000 companies)
python3 scripts/trigger_batch.py --industry "IT・web" --limit 1000

# Process all industries
python3 scripts/trigger_batch.py --all-industries --limit-per-industry 1000
```

### 6. Monitor Progress

```bash
# View all metrics
python3 scripts/monitor.py --all

# View specific metrics
python3 scripts/monitor.py --stats --quality --errors
```

## 📁 Project Structure

```
ai-sales-list/
├── src/                          # Source code
│   ├── main.py                   # Cloud Run entry point
│   ├── config.py                 # Configuration management
│   ├── handlers/                 # Request handlers
│   │   ├── pubsub_handler.py    # Pub/Sub batch triggers
│   │   └── task_handler.py      # Cloud Tasks processing
│   ├── pipeline/                 # Processing phases
│   │   ├── phase_a.py           # URL discovery
│   │   ├── phase_b.py           # Element extraction
│   │   └── phase_c.py           # AI formatting
│   ├── services/                 # External API clients
│   │   ├── perplexity.py        # Perplexity API
│   │   ├── openai_client.py     # OpenAI API
│   │   └── bigquery.py          # BigQuery operations
│   └── utils/                    # Utilities
│       ├── extractors.py         # Data extraction
│       ├── rate_limiter.py       # Rate limiting
│       └── validators.py         # Data validation
├── infrastructure/               # Infrastructure as Code
│   ├── bigquery/
│   │   └── schema.sql           # BigQuery schema
│   ├── terraform/
│   │   └── main.tf              # Terraform configuration
│   └── deploy.sh                # Deployment script
├── scripts/                      # Utility scripts
│   ├── upload_to_bq.py          # CSV to BigQuery upload
│   ├── trigger_batch.py         # Batch processing trigger
│   └── monitor.py               # Progress monitoring
├── lists/                        # Input CSV files
├── Dockerfile                    # Container definition
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🔧 Configuration

### Environment Variables

```bash
# API Keys
PPLX_API_KEY=your_perplexity_api_key
OPENAI_API_KEY=your_openai_api_key

# GCP Configuration  
GCP_PROJECT_ID=your_project_id
GCP_REGION=asia-northeast1

# BigQuery
BQ_DATASET_ID=companies
BQ_RAW_TABLE_ID=raw
BQ_ENRICHED_TABLE_ID=enriched

# Rate Limiting
DOMAIN_RPS=1                    # Requests per second per domain
GLOBAL_RPS=100                  # Global requests per second
MAX_CALLS_PER_COMPANY=3         # Max Perplexity calls per company
```

### Rate Limiting Strategy

- **Domain-level**: 1-2 req/s per apex domain (e.g., example.com)
- **Global**: 100-400 req/s total (scalable)
- **Retry Logic**: Exponential backoff (1m → 5m → 15m → 60m)
- **Max Retries**: 5 attempts per company

## 📈 Monitoring & Quality

### BigQuery Views

- `progress_dashboard`: Industry-wise completion rates
- `processing_status`: Status distribution and error analysis  
- `quality_metrics`: Data completeness and quality scores
- `recent_activity`: Processing activity over time

### Quality Metrics

- **Address Completeness**: % of companies with HQ address
- **Employee Completeness**: % with employee count data
- **Overview Quality**: Average length and completeness
- **Hypotheses Quality**: % with 3+ pain hypotheses

### Error Handling

- **Status Codes**: `ok`, `not_found`, `timeout`, `rate_limited`, `parse_error`
- **Dead Letter Queue**: Failed messages for manual review
- **Signals JSON**: Detailed error context and processing metadata
- **Retry Logic**: Automatic retry with exponential backoff

## 🎯 Data Quality

### Input Requirements

CSV files with columns:
- `企業名` (Company Name) - Required
- `URL` (Website) - Required  
- `都道府県` (Prefecture) - Optional
- `問合せフォーム` (Inquiry URL) - Optional
- `備考` (Notes) - Optional

### Output Quality

- **Overview Text**: 200-400 characters, industry-specific
- **Pain Hypotheses**: 3-5 items, 80-120 characters each
- **Services/Products**: Bullet-pointed lists, 1-7 items each
- **Personalization Notes**: 1-3 lines, sales-ready
- **Employee Count**: Integer with source URL

## 🔍 Troubleshooting

### Common Issues

1. **Rate Limiting**: Check domain-specific and global rate limits
2. **API Errors**: Verify API keys in Secret Manager
3. **BigQuery Errors**: Check table permissions and schema
4. **Processing Failures**: Review Cloud Run logs and error analysis

### Debug Commands

```bash
# Check Cloud Run logs
gcloud logs read --service=sales-enrichment-worker --limit=100

# Check BigQuery data
bq query "SELECT * FROM companies.enriched LIMIT 10"

# Check processing stats
python3 scripts/monitor.py --all

# Test single company
curl -X POST $CLOUD_RUN_URL/tasks/process \
  -H "Content-Type: application/json" \
  -d '{"website":"example.com","name":"Example Corp","industry":"IT・web"}'
```

## 📊 Performance

### Throughput

- **Target**: 200-400 companies/hour
- **Peak**: 1,000+ companies/hour (with scaling)
- **Bottleneck**: Perplexity API rate limits

### Scalability

- **Cloud Run**: Auto-scales 0-100 instances
- **Cloud Tasks**: Handles 100M+ tasks
- **BigQuery**: Petabyte-scale data warehouse
- **Pub/Sub**: 10M+ messages/second

## 🔒 Security

- **API Keys**: Stored in Google Secret Manager
- **IAM**: Least-privilege service accounts
- **Network**: Private Google Access enabled
- **Data**: All processing within GCP

## 📝 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review Cloud Run logs for errors

---

**Built with ❤️ for enterprise data enrichment**
