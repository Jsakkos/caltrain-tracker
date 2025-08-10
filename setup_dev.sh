#!/bin/bash

# Setup script for development environment

# Exit on error
set -e

echo "Setting up development environment for Caltrain Tracker..."

# Create necessary directories
mkdir -p static/plots static/data gtfs_data
echo "Created directories for static content and GTFS data"

# Check if Python virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
source venv/bin/activate
echo "Virtual environment activated"

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# API Key for 511.org
API_KEY=your_api_key_here

# Database settings
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=caltrain_tracker

# Application settings
DEBUG=True
LOG_LEVEL=INFO

# Prefect settings
PREFECT_API_URL=http://127.0.0.1:4200/api
DATA_COLLECTION_INTERVAL=60
VISUALIZATION_UPDATE_INTERVAL=3600

# FastAPI settings
API_HOST=0.0.0.0
API_PORT=8000
EOF
    echo "Please edit the .env file to set your API key and other configurations"
fi

# Setup Alembic for migrations
if [ ! -d "migrations" ]; then
    echo "Initializing Alembic migrations..."
    alembic init migrations
    
    # Update alembic.ini
    sed -i 's|sqlalchemy.url = driver://user:pass@localhost/dbname|sqlalchemy.url = postgresql://postgres:postgres@localhost:5432/caltrain_tracker|g' alembic.ini
    
    echo "Alembic initialized. You can create migrations with: alembic revision --autogenerate -m 'description'"
fi

echo "Development environment setup complete!"
echo "You can start the application with: python main.py"
echo "Make sure to run your PostgreSQL database and Prefect Orion server"
echo ""
echo "For PostgreSQL: docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=caltrain_tracker -p 5432:5432 postgres:14"
echo "For Prefect: prefect orion start"
