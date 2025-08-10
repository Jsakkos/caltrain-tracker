# Use the official Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set environment variables
ENV UV_SYSTEM_PYTHON=1
ENV PATH="/root/.local/bin:$PATH"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    tzdata \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set the timezone
ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set the working directory in the container
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt /app/
# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -r requirements.txt

# Copy the rest of the application
COPY . /app/

# Create necessary directories
RUN mkdir -p /app/static/plots /app/static/data /app/gtfs_data

# Make ports available
EXPOSE 8181

# Run application
CMD ["python", "main.py"]
