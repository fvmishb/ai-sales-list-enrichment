# AI Sales List Enrichment - Main Terraform Configuration

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-northeast1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

# Local values
locals {
  service_name = "sales-enrichment-worker"
  dataset_id   = "companies"
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "run.googleapis.com",
    "cloudtasks.googleapis.com",
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com"
  ])

  service = each.value
  disable_on_destroy = false
}

# BigQuery Dataset
resource "google_bigquery_dataset" "companies" {
  dataset_id = local.dataset_id
  location   = var.region
  description = "Enterprise data enrichment dataset"

  depends_on = [google_project_service.required_apis]
}

# Secret Manager secrets
resource "google_secret_manager_secret" "pplx_api_key" {
  secret_id = "pplx-api-key"
  
  replication {
    auto {}
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret" "openai_api_key" {
  secret_id = "openai-api-key"
  
  replication {
    auto {}
  }

  depends_on = [google_project_service.required_apis]
}

# Pub/Sub Topic
resource "google_pubsub_topic" "company_batch_trigger" {
  name = "company-batch-trigger"

  depends_on = [google_project_service.required_apis]
}

# Pub/Sub Subscription
resource "google_pubsub_subscription" "company_batch_sub" {
  name  = "company-batch-sub"
  topic = google_pubsub_topic.company_batch_trigger.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  dead_letter_policy {
    dead_letter_topic = google_pubsub_topic.company_dlq.id
    max_delivery_attempts = 5
  }
}

# Dead Letter Queue
resource "google_pubsub_topic" "company_dlq" {
  name = "company-dlq"
}

# Cloud Tasks Queue
resource "google_cloud_tasks_queue" "enrichment_tasks" {
  name     = "enrichment-tasks"
  location = var.region

  rate_limits {
    max_concurrent_dispatches = 100
    max_dispatches_per_second = 10
  }

  retry_config {
    max_attempts = 5
    max_retry_duration = "3600s"
    max_backoff = "300s"
    min_backoff = "10s"
    max_doublings = 5
  }

  depends_on = [google_project_service.required_apis]
}

# Artifact Registry
resource "google_artifact_registry_repository" "sales_enrichment" {
  location      = var.region
  repository_id = "sales-enrichment"
  description   = "Docker repository for AI Sales List Enrichment"
  format        = "DOCKER"

  depends_on = [google_project_service.required_apis]
}

# Cloud Run Service
resource "google_cloud_run_v2_service" "sales_enrichment_worker" {
  name     = local.service_name
  location = var.region

  template {
    service_account = google_service_account.sales_enrichment.email

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/sales-enrichment/sales-enrichment-worker:latest"
      
      ports {
        container_port = 8080
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "GCP_REGION"
        value = var.region
      }

      env {
        name  = "BQ_DATASET_ID"
        value = local.dataset_id
      }

      env {
        name  = "PUBSUB_TOPIC_ID"
        value = google_pubsub_topic.company_batch_trigger.name
      }

      env {
        name  = "TASKS_QUEUE_ID"
        value = google_cloud_tasks_queue.enrichment_tasks.name
      }

      env {
        name  = "TASKS_LOCATION"
        value = var.region
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 100
    }

    timeout = "300s"
  }

  depends_on = [
    google_project_service.required_apis,
    google_artifact_registry_repository.sales_enrichment
  ]
}

# Service Account
resource "google_service_account" "sales_enrichment" {
  account_id   = "sales-enrichment-worker"
  display_name = "AI Sales List Enrichment Worker"
  description  = "Service account for AI Sales List Enrichment Cloud Run service"
}

# IAM bindings
resource "google_project_iam_member" "sales_enrichment_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.sales_enrichment.email}"
}

resource "google_project_iam_member" "sales_enrichment_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.sales_enrichment.email}"
}

resource "google_project_iam_member" "sales_enrichment_tasks" {
  project = var.project_id
  role    = "roles/cloudtasks.enqueuer"
  member  = "serviceAccount:${google_service_account.sales_enrichment.email}"
}

resource "google_project_iam_member" "sales_enrichment_secrets" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.sales_enrichment.email}"
}

# Cloud Run IAM
resource "google_cloud_run_service_iam_member" "sales_enrichment_invoker" {
  service  = google_cloud_run_v2_service.sales_enrichment_worker.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.sales_enrichment.email}"
}

# Outputs
output "cloud_run_url" {
  value = google_cloud_run_v2_service.sales_enrichment_worker.uri
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.companies.dataset_id
}

output "pubsub_topic_id" {
  value = google_pubsub_topic.company_batch_trigger.name
}

output "tasks_queue_id" {
  value = google_cloud_tasks_queue.enrichment_tasks.name
}
