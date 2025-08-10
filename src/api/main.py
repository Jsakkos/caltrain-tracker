"""
FastAPI main application for the Caltrain Tracker API.
"""
import logging
import os
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from src.api.routes import router as api_router
from src.config import STATIC_CONTENT_PATH

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Caltrain Tracker API",
    description="API for Caltrain real-time tracking and performance analysis",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Mount static files
if os.path.exists(STATIC_CONTENT_PATH):
    app.mount("/static", StaticFiles(directory=STATIC_CONTENT_PATH), name="static")

# Create templates directory
templates_dir = Path(__file__).parent / "templates"
templates_dir.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(templates_dir))

# Include API routes
app.include_router(api_router, prefix="/api")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """
    Root endpoint that serves the main HTML page.
    """
    # Check if index.html exists in the templates directory, if not create a simple one
    index_path = templates_dir / "index.html"
    if not index_path.exists():
        create_default_template(index_path)
    
    return templates.TemplateResponse("index.html", {"request": request})

@app.exception_handler(404)
async def not_found_exception_handler(request: Request, exc: Exception):
    """
    Handle 404 errors.
    """
    return JSONResponse(
        status_code=404,
        content={"message": "Resource not found"},
    )

@app.exception_handler(500)
async def server_error_exception_handler(request: Request, exc: Exception):
    """
    Handle 500 errors.
    """
    logger.error(f"Internal server error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error"},
    )

def create_default_template(index_path):
    """
    Create a default index.html template if none exists.
    """
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Caltrain Tracker</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            padding-top: 20px;
            background-color: #f8f9fa;
        }
        .header {
            background-color: #007bff;
            color: white;
            padding: 20px 0;
            margin-bottom: 30px;
            border-radius: 5px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .card {
            margin-bottom: 20px;
            border: none;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s;
        }
        .card:hover {
            transform: translateY(-5px);
        }
        .card-header {
            background-color: #007bff;
            color: white;
            border-radius: 10px 10px 0 0 !important;
        }
        .btn-primary {
            background-color: #007bff;
            border-color: #007bff;
        }
        .btn-primary:hover {
            background-color: #0069d9;
            border-color: #0062cc;
        }
        .iframe-container {
            position: relative;
            overflow: hidden;
            padding-top: 56.25%; /* 16:9 aspect ratio */
        }
        .iframe-container iframe {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            border: 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header text-center">
            <h1>Caltrain Performance Tracker</h1>
            <p class="lead">Real-time monitoring and analysis of Caltrain performance metrics</p>
        </div>

        <div class="row">
            <div class="col-md-12">
                <div class="card">
                    <div class="card-header">
                        <h2>On-Time Performance by Date</h2>
                    </div>
                    <div class="card-body">
                        <div class="iframe-container">
                            <iframe src="/static/plots/daily_stats.html" frameborder="0"></iframe>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h2>Commute Period Analysis</h2>
                    </div>
                    <div class="card-body">
                        <div class="iframe-container">
                            <iframe src="/static/plots/commute_delays.html" frameborder="0"></iframe>
                        </div>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h2>API Access</h2>
                    </div>
                    <div class="card-body">
                        <p>Use our API endpoints to access Caltrain performance data programmatically:</p>
                        <ul class="list-group list-group-flush">
                            <li class="list-group-item">
                                <a href="/api/train-locations" target="_blank">/api/train-locations</a> - Real-time train locations
                            </li>
                            <li class="list-group-item">
                                <a href="/api/arrival-data" target="_blank">/api/arrival-data</a> - Processed arrival data
                            </li>
                            <li class="list-group-item">
                                <a href="/api/stops" target="_blank">/api/stops</a> - Caltrain stops information
                            </li>
                            <li class="list-group-item">
                                <a href="/api/summary" target="_blank">/api/summary</a> - Performance summary statistics
                            </li>
                            <li class="list-group-item">
                                <a href="/api/delay-stats-by-date" target="_blank">/api/delay-stats-by-date</a> - Delay stats by date
                            </li>
                            <li class="list-group-item">
                                <a href="/docs" target="_blank" class="btn btn-primary">View API Documentation</a>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <footer class="bg-light text-center text-lg-start mt-4">
        <div class="container p-4">
            <div class="row">
                <div class="col-lg-12 text-center">
                    <p>
                        Caltrain Tracker - Real-time monitoring and analysis of Caltrain performance
                        <br>
                        Data sourced from the 511.org GTFS-RT API
                    </p>
                </div>
            </div>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""
    
    with open(index_path, 'w') as f:
        f.write(html_content)
    
    logger.info(f"Created default index.html template at {index_path}")
