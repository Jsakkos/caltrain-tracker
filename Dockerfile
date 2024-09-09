# Use an official Python runtime as the base image
FROM python:3.11-slim

# Install cron and tzdata for timezone handling
RUN apt-get update && apt-get install -y cron tzdata

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

# Add the cron job to the crontab to run rebuild_plots.py at 1 AM daily
RUN echo "0 1 * * * python /app/rebuild_plots.py" >> /etc/crontab

# Add the cron service to start with the container
CMD cron && python fetch_and_process_gtfsrt.py
