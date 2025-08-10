"""
Test the data processing flow with SQLite database.
"""
import logging
from src.flows.data_processing import process_data_flow

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_data_flow()
