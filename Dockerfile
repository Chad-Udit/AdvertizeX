# Use the official Python base image
FROM python:3.9-slim

# Set environment variables to avoid writing .pyc files
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory
WORKDIR /app

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y openjdk-11-jdk && apt-get clean

# Install dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the application code
COPY . /app/

# Expose the port for Kafka (if needed)
EXPOSE 9092

# Define the default command to run your application
# This can be overridden by providing a different command in docker run
CMD ["python", "data_processing/spark_streaming_processing.py"]
