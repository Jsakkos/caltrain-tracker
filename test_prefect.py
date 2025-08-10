"""
Simple test script to check Prefect connectivity.
"""
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set environment variable explicitly
os.environ["PREFECT_API_URL"] = "http://prefect:4200"

try:
    from prefect import flow
    from prefect import task
    import httpx
    
    @task
    def test_task():
        logger.info("Testing HTTP connection to Prefect server...")
        try:
            with httpx.Client() as client:
                health_response = client.get("http://prefect:4200/api/health", timeout=5.0)
                logger.info(f"Health response status: {health_response.status_code}")
                logger.info(f"Health response text: {health_response.text}")
                
                version_response = client.get("http://prefect:4200/api/admin/version", timeout=5.0)
                logger.info(f"Version response status: {version_response.status_code}")
                logger.info(f"Version response text: {version_response.text}")
                
            return "Task succeeded"
        except Exception as e:
            logger.error(f"HTTP connection test failed: {e}")
            return f"Task failed: {e}"
    
    @flow
    def test_flow():
        logger.info("Starting test flow...")
        result = test_task()
        logger.info(f"Flow result: {result}")
        return "Flow succeeded"
    
    if __name__ == "__main__":
        logger.info("Running test script...")
        result = test_flow()
        logger.info(f"Final result: {result}")
        
except Exception as e:
    logger.error(f"Failed to import or run Prefect: {e}")
    sys.exit(1)
