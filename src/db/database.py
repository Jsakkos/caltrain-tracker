"""
Database connection and session management.
"""
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pathlib import Path

from src.config import SQLITE_DB_PATH

# Set up logging
logger = logging.getLogger(__name__)

# Ensure the data directory exists
data_dir = Path(SQLITE_DB_PATH).parent
data_dir.mkdir(parents=True, exist_ok=True)

# Construct the database URI for SQLite
db_uri = f"sqlite:///{SQLITE_DB_PATH}"
logger.info(f"Using SQLite database: {SQLITE_DB_PATH}")

# Create SQLAlchemy engine with SQLite-specific settings
engine = create_engine(
    db_uri, 
    connect_args={"check_same_thread": False}  # Needed for SQLite
)

# Create sessionmaker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for SQLAlchemy models
Base = declarative_base()

# Dependency for FastAPI
def get_db():
    """
    Get database session for dependency injection in FastAPI endpoints.
    Yields a database session and ensures it's closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
