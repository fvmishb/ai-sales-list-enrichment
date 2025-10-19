#!/bin/bash
# AI Sales List Enrichment - Deployment Script

set -e

# Configuration
PROJECT_ID=${1:-"your-project-id"}
REGION=${2:-"asia-northeast1"}
ENVIRONMENT=${3:-"prod"}

echo "🚀 Starting deployment for project: $PROJECT_ID"
echo "📍 Region: $REGION"
echo "🌍 Environment: $ENVIRONMENT"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Check if terraform is installed
if ! command -v terraform &> /dev/null; then
    echo "❌ Terraform is not installed. Please install it first."
    exit 1
fi

# Check if docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install it first."
    exit 1
fi

# Set project
echo "🔧 Setting GCP project..."
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔌 Enabling required APIs..."
gcloud services enable run.googleapis.com
gcloud services enable cloudtasks.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Deploy infrastructure with Terraform
echo "🏗️  Deploying infrastructure with Terraform..."
cd terraform

# Initialize Terraform
terraform init

# Create terraform.tfvars
cat > terraform.tfvars << EOF
project_id = "$PROJECT_ID"
region = "$REGION"
environment = "$ENVIRONMENT"
EOF

# Plan and apply
terraform plan -var="project_id=$PROJECT_ID" -var="region=$REGION" -var="environment=$ENVIRONMENT"
terraform apply -var="project_id=$PROJECT_ID" -var="region=$REGION" -var="environment=$ENVIRONMENT" -auto-approve

# Get outputs
CLOUD_RUN_URL=$(terraform output -raw cloud_run_url)
BIGQUERY_DATASET=$(terraform output -raw bigquery_dataset_id)
PUBSUB_TOPIC=$(terraform output -raw pubsub_topic_id)
TASKS_QUEUE=$(terraform output -raw tasks_queue_id)

echo "✅ Infrastructure deployed successfully!"
echo "🌐 Cloud Run URL: $CLOUD_RUN_URL"
echo "📊 BigQuery Dataset: $BIGQUERY_DATASET"
echo "📢 Pub/Sub Topic: $PUBSUB_TOPIC"
echo "⚡ Tasks Queue: $TASKS_QUEUE"

cd ..

# Build and push Docker image
echo "🐳 Building and pushing Docker image..."
docker build -t sales-enrichment-worker .
docker tag sales-enrichment-worker $REGION-docker.pkg.dev/$PROJECT_ID/sales-enrichment/sales-enrichment-worker:latest

# Configure Docker authentication
gcloud auth configure-docker $REGION-docker.pkg.dev

# Push image
docker push $REGION-docker.pkg.dev/$PROJECT_ID/sales-enrichment/sales-enrichment-worker:latest

# Create BigQuery tables
echo "📊 Creating BigQuery tables..."
python3 scripts/upload_to_bq.py --create-tables

# Set up secrets (manual step)
echo "🔐 Setting up secrets..."
echo "Please set the following secrets in Google Secret Manager:"
echo "1. pplx-api-key: Your Perplexity API key"
echo "2. openai-api-key: Your OpenAI API key"
echo ""
echo "You can set them using:"
echo "echo 'your-perplexity-api-key' | gcloud secrets versions add pplx-api-key --data-file=-"
echo "echo 'your-openai-api-key' | gcloud secrets versions add openai-api-key --data-file=-"

# Test deployment
echo "🧪 Testing deployment..."
curl -f $CLOUD_RUN_URL/health || echo "⚠️  Health check failed, but deployment may still be successful"

echo ""
echo "🎉 Deployment completed!"
echo ""
echo "Next steps:"
echo "1. Set up your API keys in Secret Manager"
echo "2. Upload your CSV files: python3 scripts/upload_to_bq.py --csv-directory lists/"
echo "3. Trigger batch processing: python3 scripts/trigger_batch.py --industry 'IT・web' --limit 100"
echo "4. Monitor progress: python3 scripts/monitor.py --all"
echo ""
echo "Cloud Run URL: $CLOUD_RUN_URL"
echo "BigQuery Dataset: $BIGQUERY_DATASET"
