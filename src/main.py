"""Cloud Run entry point for AI Sales List Enrichment."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
import json

from .handlers.pubsub_handler import PubSubHandler
from .handlers.task_handler import TaskHandler
from .handlers.simple_processor import SimpleProcessor
from .services.address_search_api import AccurateAddressSearcher
from .services.smart_address_generator import SmartAddressGenerator
from .config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PubSubMessage(BaseModel):
    """Pub/Sub message model."""
    data: str
    attributes: Dict[str, str] = {}


class TaskMessage(BaseModel):
    """Cloud Tasks message model."""
    website: str
    name: str
    industry: str
    prefecture: Optional[str] = None
    inquiry_url: Optional[str] = None


class AddressSearchRequest(BaseModel):
    """Address search request model."""
    company_name: str
    website: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting AI Sales List Enrichment service...")
    yield
    logger.info("Shutting down AI Sales List Enrichment service...")


app = FastAPI(
    title="AI Sales List Enrichment",
    description="Enterprise data enrichment pipeline using Perplexity API and GPT-5-mini",
    version="1.0.0",
    lifespan=lifespan
)

# Initialize handlers
pubsub_handler = PubSubHandler()
task_handler = TaskHandler()
simple_processor = SimpleProcessor()


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "healthy", "service": "ai-sales-list-enrichment"}


@app.get("/health")
async def health_check():
    """Detailed health check."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "gcp_project": settings.gcp_project_id,
        "region": settings.gcp_region
    }


@app.post("/pubsub/trigger")
async def pubsub_trigger(request: Request, background_tasks: BackgroundTasks):
    """Handle Pub/Sub batch trigger messages."""
    try:
        # Parse Pub/Sub message format
        body = await request.json()
        logger.info(f"Received Pub/Sub message: {body}")
        
        # Extract message data
        if 'message' in body and 'data' in body['message']:
            import base64
            data = base64.b64decode(body['message']['data']).decode('utf-8')
            logger.info(f"Raw decoded data: {data}")
            
            try:
                message_data = json.loads(data)
                logger.info(f"Decoded message data: {message_data}")
                
                # Process the batch
                background_tasks.add_task(pubsub_handler.process_batch, message_data)
                return {"status": "accepted"}
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}, data: {data}")
                # Try to parse as a simple dict-like string
                if data.startswith("{") and data.endswith("}"):
                    # Simple string replacement for common issues
                    fixed_data = data.replace("'", '"').replace("None", "null")
                    try:
                        message_data = json.loads(fixed_data)
                        logger.info(f"Fixed and decoded message data: {message_data}")
                        background_tasks.add_task(pubsub_handler.process_batch, message_data)
                        return {"status": "accepted"}
                    except json.JSONDecodeError as e2:
                        logger.error(f"Still failed to parse: {e2}")
                        raise HTTPException(status_code=400, detail=f"Invalid JSON format: {e2}")
                else:
                    raise HTTPException(status_code=400, detail=f"Invalid data format: {data}")
        else:
            logger.error(f"Invalid Pub/Sub message format: {body}")
            raise HTTPException(status_code=400, detail="Invalid message format")
            
    except Exception as e:
        logger.error(f"Error processing Pub/Sub message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tasks/process")
async def process_company(task: TaskMessage, background_tasks: BackgroundTasks):
    """Handle Cloud Tasks company processing."""
    try:
        logger.info(f"Processing company: {task.website}")
        background_tasks.add_task(task_handler.process_company, task)
        return {"status": "accepted", "website": task.website}
    except Exception as e:
        logger.error(f"Error processing company {task.website}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fast-process")
async def fast_process(request: Request):
    """Fast processing endpoint for direct processing."""
    try:
        body = await request.json()
        industry = body.get("industry")
        limit = body.get("limit", 1000)
        max_workers = body.get("max_workers", 20)
        
        logger.info(f"Starting fast processing: industry={industry}, limit={limit}, workers={max_workers}")
        
        # Get companies to process
        from .services.bigquery import BigQueryClient
        bq_client = BigQueryClient()
        companies = await bq_client.get_companies_to_process(industry, limit)
        
        if not companies:
            return {"status": "error", "message": f"No companies found for industry: {industry}"}
        
        logger.info(f"Found {len(companies)} companies to process")
        
        # Process companies directly
        results = await simple_processor.process_companies_simple(companies, max_workers)
        
        return {
            "status": "success",
            "results": results,
            "message": f"Processed {results['success']}/{results['total']} companies successfully"
        }
        
    except Exception as e:
        logger.error(f"Error in fast processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search-address")
async def search_address(request: AddressSearchRequest):
    """Search for accurate company address."""
    try:
        logger.info(f"Searching address for: {request.company_name}")
        
        async with AccurateAddressSearcher() as searcher:
            result = await searcher.search_company_address(
                request.company_name, 
                request.website or ""
            )
            
            if result:
                return {
                    "status": "success",
                    "company_name": request.company_name,
                    "address": result["address"],
                    "prefecture": result["prefecture"]
                }
            else:
                return {
                    "status": "not_found",
                    "company_name": request.company_name,
                    "message": "No accurate address found"
                }
                
    except Exception as e:
        logger.error(f"Error searching address for {request.company_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/search-addresses-batch")
async def search_addresses_batch(request: Request):
    """Search addresses for multiple companies."""
    try:
        body = await request.json()
        companies = body.get("companies", [])
        limit = body.get("limit", 10)
        
        logger.info(f"Searching addresses for {len(companies)} companies")
        
        results = []
        async with AccurateAddressSearcher() as searcher:
            for i, company in enumerate(companies[:limit]):
                try:
                    company_name = company.get("name", "")
                    website = company.get("website", "")
                    
                    result = await searcher.search_company_address(company_name, website)
                    
                    if result:
                        results.append({
                            "company_name": company_name,
                            "address": result["address"],
                            "prefecture": result["prefecture"],
                            "status": "success"
                        })
                    else:
                        results.append({
                            "company_name": company_name,
                            "status": "not_found"
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing {company.get('name', 'unknown')}: {e}")
                    results.append({
                        "company_name": company.get("name", "unknown"),
                        "status": "error",
                        "error": str(e)
                    })
        
        return {
            "status": "completed",
            "total_processed": len(results),
            "success_count": len([r for r in results if r.get("status") == "success"]),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error in batch address search: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-smart-addresses")
async def generate_smart_addresses(request: Request):
    """Generate smart addresses for companies with poor address quality."""
    try:
        body = await request.json()
        limit = body.get("limit", 50)
        offset = body.get("offset", 0)
        test_mode = body.get("test_mode", False)
        
        logger.info(f"Starting smart address generation: limit={limit}, offset={offset}, test_mode={test_mode}")
        
        # Import here to avoid circular imports
        from .services.smart_address_generator import SmartAddressGenerator
        
        generator = SmartAddressGenerator()
        await generator.generate_smart_addresses(limit=limit, offset=offset, test_mode=test_mode)
        
        return {
            "status": "success",
            "message": f"Smart address generation completed for limit={limit}, offset={offset}"
        }
        
    except Exception as e:
        logger.error(f"Error in smart address generation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/process-generic-addresses")
async def process_generic_addresses(request: Request):
    """Process companies with generic addresses directly."""
    try:
        body = await request.json()
        limit = body.get("limit", 100)
        max_workers = body.get("max_workers", 10)
        
        logger.info(f"Starting generic address processing: limit={limit}, max_workers={max_workers}")
        
        # Get companies with generic addresses from enriched table
        query = f"""
        SELECT name, website, industry, hq_address_raw, prefecture_name
        FROM `ai-sales-list.companies.enriched`
        WHERE (
            hq_address_raw LIKE '%要確認%' OR
            hq_address_raw LIKE '%推測%' OR
            hq_address_raw LIKE '%本社所在地%' OR
            hq_address_raw LIKE '%詳細住所は要確認%' OR
            hq_address_raw LIKE '%不明%'
        )
        ORDER BY name
        LIMIT {limit}
        """
        
        from .services.bigquery import BigQueryClient
        bq_client = BigQueryClient()
        companies = await bq_client.run_query(query)
        
        if not companies:
            return {
                "status": "success",
                "message": "No companies with generic addresses found"
            }
        
        logger.info(f"Found {len(companies)} companies with generic addresses")
        
        # Process companies using SimpleProcessor
        processor = SimpleProcessor()
        success_count = 0
        
        for company in companies:
            try:
                success = await processor._process_single_company_async(company)
                if success:
                    success_count += 1
                    logger.info(f"Successfully processed: {company.get('name', 'unknown')}")
            except Exception as e:
                logger.error(f"Error processing {company.get('name', 'unknown')}: {e}")
        
        return {
            "status": "success",
            "message": f"Processed {success_count}/{len(companies)} companies with generic addresses"
        }
        
    except Exception as e:
        logger.error(f"Error in generic address processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get processing statistics."""
    try:
        stats = await task_handler.get_processing_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
