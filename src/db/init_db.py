"""
Initialize the SQLite database schema.
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from src.db.database import Base, engine
from src.models.train_data import TrainLocation, Stop, StopTime, ArrivalData

# Set up logging
logger = logging.getLogger(__name__)

def init_db():
    """
    Initialize the database by creating all tables defined in the models.
    """
    try:
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except OperationalError as e:
        logger.error(f"Error creating database tables: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
