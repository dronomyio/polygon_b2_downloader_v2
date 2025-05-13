# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables to prevent Python from writing pyc files to disc and to buffer output
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies that might be needed by some Python packages (e.g., for certain DB drivers if not SQLite)
# For SQLite, typically no extra system deps are needed with standard Python builds.
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the .env.example file (the actual .env will be provided at runtime)
COPY .env.example .

# Copy the entire src directory into the container at /app/src
COPY src/ ./src/

# Create the default data directory where the SQLite DB might be stored
# This directory will be a mount point for a volume in docker-compose or Kubernetes
RUN mkdir -p /app/data
VOLUME /app/data

# Expose any ports if your application were a web service (not needed for this batch app)
# EXPOSE 8000

# Define the entry point for the application.
# This allows running `docker run <image> discoverer ...` or `docker run <image> worker ...`
# Using `python -m src.main` ensures that Python resolves imports correctly within the src package.
ENTRYPOINT ["python", "-m", "src.main"]

# Default command can be set here if desired, e.g., to run worker by default.
# CMD ["worker"] 
# However, it's often better to specify the command (role + args) when running the container,
# especially with docker-compose or Kubernetes, to clearly define the role of each container instance.

