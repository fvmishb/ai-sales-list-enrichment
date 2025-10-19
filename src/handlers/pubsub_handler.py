"""Pub/Sub handler for batch processing triggers."""

import asyncio
import json
import logging
from typing import Dict, Any, List, List
from google.cloud import pubsub_v1
from google.cloud import tasks_v2
from google.cloud.tasks_v2 import CloudTasksClient, Task, HttpRequest, HttpMethod

from ..config import settings
from ..services.bigquery import BigQueryClient

logger = logging.getLogger(__name__)


class PubSubHandler:
    """Handler for Pub/Sub batch processing triggers."""
    
    def __init__(self):
        self.project_id = settings.gcp_project_id
        self.region = settings.gcp_region
        self.tasks_queue_id = settings.tasks_queue_id
        self.tasks_location = settings.tasks_location
        
        # Initialize clients
        self.pubsub_client = pubsub_v1.PublisherClient()
        self.tasks_client = CloudTasksClient()
        self.bigquery_client = BigQueryClient()
        
        # Queue path
        self.queue_path = self.tasks_client.queue_path(
            self.project_id, self.tasks_location, self.tasks_queue_id
        )
    
    async def process_batch(self, message_data: Dict[str, Any]) -> None:
        """Process batch trigger message."""
        try:
            # Parse message data (already decoded in main.py)
            industry = message_data.get("industry")
            limit = int(message_data.get("limit", 1000))
            
            logger.info(f"Processing batch trigger: industry={industry}, limit={limit}")
            
            # Get companies to process
            companies = await self.bigquery_client.get_companies_to_process(industry, limit)
            
            if not companies:
                logger.warning(f"No companies found for industry: {industry}")
                return
            
            logger.info(f"Found {len(companies)} companies to process")
            
            # Create Cloud Tasks for each company
            logger.info(f"Starting to create Cloud Tasks for {len(companies)} companies")
            await self._create_company_tasks(companies)
            
            logger.info(f"Completed creating Cloud Tasks for processing")
            
        except Exception as e:
            logger.error(f"Error processing batch trigger: {e}")
            raise
    
    async def _create_company_tasks(self, companies: List[Dict[str, Any]]) -> None:
        """Create Cloud Tasks for company processing with parallel execution."""
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        logger.info(f"Creating Cloud Tasks for {len(companies)} companies")
        
        # バッチサイズを設定（Cloud Tasks APIの制限を考慮）
        batch_size = 100
        tasks_created = 0
        
        # 並列処理でタスクを作成
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(0, len(companies), batch_size):
                batch = companies[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(companies) + batch_size - 1)//batch_size}")
                
                # バッチ内で並列処理
                futures = []
                for company in batch:
                    future = executor.submit(self._create_single_task, company)
                    futures.append(future)
                
                # バッチの完了を待つ
                for future in futures:
                    try:
                        if future.result():
                            tasks_created += 1
                    except Exception as e:
                        logger.error(f"Error in batch processing: {e}")
                        continue
        
        logger.info(f"Successfully created {tasks_created} Cloud Tasks")
    
    def _create_single_task(self, company: Dict[str, Any]) -> bool:
        """Create a single Cloud Task (for parallel execution)."""
        try:
            task = self._create_company_task(company)
            response = self.tasks_client.create_task(
                parent=self.queue_path,
                task=task
            )
            return True
        except Exception as e:
            logger.error(f"Error creating task for company {company.get('name', 'unknown')}: {e}")
            return False
    
    def _create_company_task(self, company: Dict[str, Any]) -> Task:
        """Create a Cloud Task for company processing."""
        # Build task payload
        payload = {
            "website": company.get("website", ""),
            "name": company.get("name", ""),
            "industry": company.get("industry", ""),
            "prefecture": company.get("prefecture", ""),
            "inquiry_url": company.get("inquiry_url", "")
        }
        
        # Create HTTP request
        http_request = HttpRequest(
            http_method=HttpMethod.POST,
            url=f"https://sales-enrichment-worker-905635292309.asia-northeast1.run.app/tasks/process",
            headers={"Content-Type": "application/json"},
            body=json.dumps(payload).encode("utf-8")
        )
        
        # Create task
        task = Task(
            http_request=http_request,
            schedule_time=None  # Execute immediately
        )
        
        return task
    
    async def trigger_batch_processing(self, industry: str = None, limit: int = 1000) -> Dict[str, Any]:
        """Trigger batch processing for companies."""
        try:
            # Build message attributes
            attributes = {
                "industry": industry or "all",
                "limit": str(limit),
                "timestamp": str(asyncio.get_event_loop().time())
            }
            
            # Publish message to Pub/Sub
            topic_path = self.pubsub_client.topic_path(
                self.project_id, settings.pubsub_topic_id
            )
            
            message_data = json.dumps({
                "trigger": "batch_processing",
                "industry": industry,
                "limit": limit
            }).encode("utf-8")
            
            future = self.pubsub_client.publish(
                topic_path, message_data, **attributes
            )
            
            message_id = future.result()
            
            logger.info(f"Published batch trigger message: {message_id}")
            
            return {
                "status": "success",
                "message_id": message_id,
                "industry": industry,
                "limit": limit
            }
            
        except Exception as e:
            logger.error(f"Error triggering batch processing: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_queue_status(self) -> Dict[str, Any]:
        """Get Cloud Tasks queue status."""
        try:
            # Get queue information
            queue = self.tasks_client.get_queue(name=self.queue_path)
            
            return {
                "queue_name": queue.name,
                "state": queue.state.name,
                "max_concurrent_dispatches": queue.rate_limits.max_concurrent_dispatches,
                "max_dispatches_per_second": queue.rate_limits.max_dispatches_per_second,
                "max_burst_size": queue.rate_limits.max_burst_size
            }
            
        except Exception as e:
            logger.error(f"Error getting queue status: {e}")
            return {
                "error": str(e)
            }
    
    async def cleanup_failed_tasks(self) -> Dict[str, Any]:
        """Clean up failed tasks from the queue."""
        try:
            # List tasks in the queue
            tasks = self.tasks_client.list_tasks(parent=self.queue_path)
            
            failed_tasks = []
            for task in tasks:
                # Check if task has failed (this is a simplified check)
                if hasattr(task, 'schedule_time') and task.schedule_time:
                    failed_tasks.append(task.name)
            
            # Delete failed tasks
            deleted_count = 0
            for task_name in failed_tasks:
                try:
                    self.tasks_client.delete_task(name=task_name)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting task {task_name}: {e}")
            
            logger.info(f"Cleaned up {deleted_count} failed tasks")
            
            return {
                "status": "success",
                "deleted_count": deleted_count
            }
            
        except Exception as e:
            logger.error(f"Error cleaning up failed tasks: {e}")
            return {
                "status": "error",
                "error": str(e)
            }


# Global instance
pubsub_handler = PubSubHandler()
