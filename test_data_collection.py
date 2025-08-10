"""
Test the data collection flow with SQLite database.
"""
import logging
import sys
from src.flows.data_collection import collect_train_data_flow

if __name__ == "__main__":
    # Configure logging to show more details
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    
    # Run the data collection flow
    print("Starting data collection test...")
    result = collect_train_data_flow()
    print(f"Data collection test completed. Added {result} new records.")
