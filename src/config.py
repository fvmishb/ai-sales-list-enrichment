"""Configuration management for AI Sales List Enrichment."""

import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # API Keys (optional for local development)
    pplx_api_key: str = ""
    openai_api_key: str = ""
    
    # Google Search API Configuration
    google_search_api_key: str = ""
    google_cse_id: str = ""
    
    # Gemini API Configuration
    gemini_api_key: str = ""
    use_vertex_ai: bool = True  # Vertex AI認証を使用
    
    # Web Scraping Configuration
    scraper_timeout: int = 10
    scraper_max_content_length: int = 50000  # 50KB
    
    # GCP Configuration
    gcp_project_id: str
    gcp_region: str = "asia-northeast1"
    
    # BigQuery Configuration
    bq_dataset_id: str = "companies"
    bq_raw_table_id: str = "raw"
    bq_enriched_table_id: str = "enriched"
    
    # Pub/Sub Configuration
    pubsub_topic_id: str = "company-batch-trigger"
    pubsub_subscription_id: str = "company-batch-sub"
    
    # Cloud Tasks Configuration
    tasks_queue_id: str = "enrichment-tasks"
    tasks_location: str = "asia-northeast1"
    
    # Rate Limiting
    domain_rps: int = 1
    global_rps: int = 100
    max_calls_per_company: int = 3
    
    # Processing Configuration
    pplx_mode: str = "search"  # Deep Research禁止
    batch_size: int = 1000
    
    # Cloud Run Configuration
    port: int = 8080
    
    class Config:
        env_file = ".env"
        case_sensitive = False


def get_secret(secret_name: str) -> str:
    """Get secret from Secret Manager."""
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{os.getenv('GCP_PROJECT_ID', 'ai-sales-list')}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Warning: Could not fetch secret {secret_name}: {e}")
        return ""

# Global settings instance
settings = Settings()

# Override API keys from Secret Manager if running in Cloud Run
if os.getenv('K_SERVICE'):  # Cloud Run environment variable
    pplx_key = get_secret("pplx-api-key")
    openai_key = get_secret("openai-api-key")
    google_search_key = get_secret("google-search-api-key")
    google_cse_id = get_secret("google-cse-id")
    gemini_key = get_secret("gemini-api-key")
    
    # 新しいCustom Search API用のSecret
    custom_search_key = get_secret("google-custom-search-api-key")
    custom_search_cse_id = get_secret("google-custom-search-cse-id")
    
    print(f"DEBUG: Retrieved pplx_api_key length: {len(pplx_key) if pplx_key else 0}")
    print(f"DEBUG: Retrieved openai_api_key length: {len(openai_key) if openai_key else 0}")
    print(f"DEBUG: Retrieved google_search_api_key length: {len(google_search_key) if google_search_key else 0}")
    print(f"DEBUG: Retrieved google_cse_id length: {len(google_cse_id) if google_cse_id else 0}")
    print(f"DEBUG: Retrieved gemini_api_key length: {len(gemini_key) if gemini_key else 0}")
    print(f"DEBUG: Retrieved custom_search_api_key length: {len(custom_search_key) if custom_search_key else 0}")
    print(f"DEBUG: Retrieved custom_search_cse_id length: {len(custom_search_cse_id) if custom_search_cse_id else 0}")
    
    settings.pplx_api_key = pplx_key
    settings.openai_api_key = openai_key
    settings.gemini_api_key = gemini_key
    
    # Custom Search APIの設定を優先
    if custom_search_key and custom_search_cse_id:
        settings.google_search_api_key = custom_search_key
        settings.google_cse_id = custom_search_cse_id
    else:
        # フォールバック: 既存の設定を使用
        settings.google_search_api_key = google_search_key
        settings.google_cse_id = google_cse_id
