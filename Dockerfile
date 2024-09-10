# Use an official Python runtime as the base image
FROM python:3.11-slim

# Install tzdata package for timezones
RUN apt-get update && apt-get install -y tzdata

# Set the timezone
ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for data and GTFS files
RUN mkdir -p /data/gtfs_data /static

# Make port 8050 available to the world outside this container (if needed for testing)
EXPOSE 8050

# Start the data collection script and schedule daily plot regeneration
CMD ["sh", "-c", "while true; do python fetch_and_process_gtfsrt.py; sleep 60; done & while true; do python rebuild_plots.py; sleep 3600; done"]
