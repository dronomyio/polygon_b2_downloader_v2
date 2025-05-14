# Polygon.io to Backblaze B2 Downloader (Scalable Version v2)

This application is designed to download flat files (specifically US stocks daily data) from Polygon.io, track their status in a local SQLite database, and upload them to a Backblaze B2 bucket. It is structured to support scalable deployment, for example, using multiple worker instances on platforms like morph.so or Kubernetes.

## Architecture Overview

The application consists of two main components (roles):

1.  **Discoverer/Scheduler:**
    *   Identifies files to be processed from Polygon.io based on the operational mode (historical, daily, on-demand).
    *   Adds these files as tasks to a shared SQLite database with a "pending" status. It avoids adding duplicate `file_key` entries.
    *   Typically run as a scheduled job (for daily tasks) or a one-off task (for historical backfill or on-demand requests).

2.  **Worker(s):**
    *   Multiple worker instances can run concurrently.
    *   Each worker queries the SQLite database for "pending" tasks or tasks that previously failed but are eligible for retry.
    *   Atomically claims a task by updating its status to "processing" and assigning its `worker_id`.
    *   Downloads the file from Polygon.io.
    *   Uploads the file to Backblaze B2.
    *   Updates the file's status in the SQLite database throughout the process (e.g., downloading, downloaded, uploading, completed, failed_download, failed_upload, permanent_failure).
    *   Handles retries for failed operations (up to 2 retries as configured in `db_manager.MAX_RETRIES`).

## Features

-   Scalable architecture with separate discoverer and worker roles.
-   Tracks download and upload status in a local SQLite database (`data/download_tracker.db` by default).
-   Supports historical, daily, and on-demand data processing modes for the discoverer.
-   Uploads downloaded files to a specified Backblaze B2 bucket.
-   Uses a `.env` file for configuration (API keys, bucket details, database URL, log level).
-   Logs errors and manages retries for processing tasks.
-   Dockerized for deployment, with `docker-compose` support for local execution and role management.

## Prerequisites

-   Docker and Docker Compose installed on your local machine or deployment environment.
-   Polygon.io API Key.
-   Backblaze B2 Account with:
    -   Key ID
    -   Application Key
    -   Bucket Name
    -   S3 Endpoint URL (e.g., `s3.us-west-000.backblazeb2.com`)

## Setup Instructions

1.  **Project Files:**
    *   Clone this repository or download and extract the provided project files to a directory on your system.

2.  **Configuration (`.env` file):**
    *   Navigate to the project root directory.
    *   Copy the `.env.example` file to a new file named `.env`:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file with your actual credentials and settings:
        *   `POLYGON_API_KEY`: Your Polygon.io API key.
        *   `B2_KEY_ID`: Your Backblaze B2 Key ID.
        *   `B2_APPLICATION_KEY`: Your Backblaze B2 Application Key.
        *   `B2_BUCKET_NAME`: The name of your Backblaze B2 bucket where files will be stored.
        *   `B2_ENDPOINT_URL`: The S3 endpoint URL for your B2 bucket's region.
        *   `DATABASE_URL`: The connection string for the SQLite database. By default, it's `sqlite:///data/download_tracker.db`, meaning the database file `download_tracker.db` will be created inside a `data` subdirectory within the project (or `/app/data` inside the Docker container). This path is important for volume mounting.
        *   `LOG_LEVEL`: Set the desired logging level (e.g., `INFO`, `DEBUG`, `WARNING`, `ERROR`). Defaults to `INFO`.

3.  **Database Directory:**
    *   The application will attempt to create the `data` directory if it doesn't exist (as specified by the default `DATABASE_URL`). When using Docker, this directory inside the container (`/app/data`) will be mapped to a persistent volume to ensure the database survives container restarts.

## Running the Application with Docker Compose (Recommended for Local Execution)

`docker-compose.yml` is provided to simplify running the discoverer and worker roles.

**1. Build the Docker Image:**
   (This step is often handled automatically by `docker-compose run` or `docker-compose up` if the image doesn't exist, but you can build it explicitly)
   ```bash
   docker-compose build
   ```
   Or, if you prefer to tag it manually first:
   ```bash
   docker build -t polygon-b2-downloader-v2 .
   ```

**2. Running the Discoverer Role:**
   The discoverer populates the database with tasks. It's typically run as a one-off command.

   

   *   **Historical Mode (to discover all available files, or within a date range):**

   *   
       Historical Mode:
When you run the discoverer in historical mode (e.g., discoverer historical or discoverer historical --start_date YYYY-MM-DD --end_date YYYY-MM-DD):
The PolygonClient's list_us_stocks_daily_files method is called. This method queries Polygon.io's S3 storage for files matching the prefix us_stocks_sip/day_aggs_v1/.
Polygon.io itself organizes its files using this date-based path structure. So, the S3 keys returned by Polygon.io (via the list_objects_v2 S3 API call) will already have the YYYY/YYYY-MM-DD.csv.gz format.
The list_us_stocks_daily_files method in our PolygonClient then parses the date from each S3 key it finds to see if it falls within the optional start_date and end_date range you might provide. If no range is provided, it attempts to list all matching files.
Source of YYYY, MM, DD: Directly from the S3 object keys provided by Polygon.io, then filtered by the application if a date range is specified.

     
       ```bash
       # Discover all historical files
       docker-compose run --rm discoverer discoverer historical

       # Discover historical files within a specific date range
       docker-compose run --rm discoverer discoverer historical --start_date YYYY-MM-DD --end_date YYYY-MM-DD
       ```
       Replace `YYYY-MM-DD` with actual dates.

   *   **Daily Mode (to discover yesterday's file):**

   *   When you run the discoverer in daily mode (e.g., discoverer daily):
The discoverer/main.py script calculates yesterday's date (e.g., if today is 2023-05-14, yesterday was 2023-05-13).
It then formats this calculated date to get the year (YYYY, e.g., "2023") and the full date string (YYYY-MM-DD, e.g., "2023-05-13").
It constructs the expected S3 file key using these formatted date parts: f"us_stocks_sip/day_aggs_v1/{year_str}/{date_str}.csv.gz".
Source of YYYY, MM, DD: Calculated by the application based on the current date (to determine yesterday).

       ```bash
       docker-compose run --rm discoverer discoverer daily
       ```
       This is suitable for scheduling via cron or a similar task scheduler.

   *   **On-Demand Mode (to discover files for specific dates):**

   *   When you run the discoverer in on-demand mode with specific dates (e.g., discoverer on-demand --dates YYYY-MM-DD,YYYY-MM-DD):
The script takes the date strings you provide (e.g., "2023-01-15").
For each provided date string, it parses it to get the year (YYYY) and the full date string (YYYY-MM-DD).
It then constructs the S3 file key using these parts, similar to the daily mode: f"us_stocks_sip/day_aggs_v1/{year_str}/{date_str}.csv.gz".
Source of YYYY, MM, DD: Directly from the date strings provided by you as command-line arguments.
In all cases, the base path structure us_stocks_sip/day_aggs_v1/ is hardcoded in the PolygonClient as the prefix for listing US stocks daily aggregate files, as this is the known location for these files in Polygon.io's S3 storage. The date components are then either derived from Polygon.io's S3 listing (for historical) or constructed by the application based on the operational mode and any user-provided dates (for daily and on-demand).

       ```bash
       docker-compose run --rm discoverer discoverer on-demand --dates YYYY-MM-DD,YYYY-MM-DD,...
       ```
       Replace `YYYY-MM-DD,...` with a comma-separated list of dates.

**3. Running the Worker Role(s):**
   Workers pick up tasks from the database and process them. You can scale the number of workers.

   *   **Run a single worker (in the foreground, for testing):**
       ```bash
       docker-compose run --rm worker worker
       ```
       (The second `worker` is the command argument to `src/main.py` specifying the role)

   *   **Run multiple workers (e.g., 3 workers, in detached mode):**
       ```bash
       docker-compose up --scale worker=3 -d worker
       ```
       The `-d` flag runs them in the background. Logs can be viewed with `docker-compose logs worker`.

   *   **To stop workers running in detached mode:**
       ```bash
       docker-compose down
       ```
       Or `docker-compose stop worker` to just stop them.

**Important for Docker Compose:**
*   The `docker-compose.yml` defines a named volume `app_data` which is mounted to `/app/data` inside the containers. This ensures your SQLite database (`download_tracker.db`) persists even if containers are stopped and removed.
*   The `src` directory can be optionally mounted for local development to reflect code changes without rebuilding the image (see commented-out lines in `docker-compose.yml`). For production-like runs, it's better to rely on the code baked into the image.

## Deployment Guidelines

### Deploying on morph.so (General Guidance)

morph.so is a platform for running cron jobs, background workers, and APIs. Here's how you might adapt this application:

1.  **Container Registry:** Push your built Docker image (`polygon-b2-downloader-v2`) to a container registry that morph.so can access (e.g., Docker Hub, GitHub Container Registry, AWS ECR, etc.).
    ```bash
    docker tag polygon-b2-downloader-v2 your-registry/your-image-name:latest
    docker push your-registry/your-image-name:latest
    ```

2.  **Database Persistence on morph.so:**
    *   morph.so services often have options for persistent storage or attaching volumes. You'll need to configure your morph.so service to mount a persistent volume at the path where the SQLite database is stored (e.g., `/app/data` inside the container).
    *   Consult morph.so documentation for the exact mechanism to provision and mount persistent storage for your service.

3.  **Configuring Services on morph.so:**
    *   **Discoverer:**
        *   **Daily Discoverer:** Set up a cron job service on morph.so. Configure it to use your Docker image and run the command: `discoverer daily`. Schedule it to run once a day.
        *   **Historical/On-Demand Discoverer:** You might run these as one-off tasks or jobs on morph.so, providing the appropriate command arguments (`discoverer historical ...` or `discoverer on-demand ...`).
    *   **Worker(s):**
        *   Set up a background worker service on morph.so using your Docker image. The command for this service would be `worker`.
        *   You can configure the number of instances (replicas/pods) for this worker service on the morph.so platform to scale processing.

4.  **Environment Variables on morph.so:**
    *   Securely provide your API keys and other configurations (from your `.env` file) as environment variables to your services/jobs on morph.so. Most platforms have a way to manage secrets or environment variables for deployed applications.

### Deploying on Kubernetes (General Guidance)

Kubernetes offers a more complex but powerful environment.

1.  **Container Registry:** As with morph.so, push your Docker image to a registry accessible by your Kubernetes cluster.

2.  **Persistent Storage for SQLite:**
    *   Define a `PersistentVolume` (PV) and a `PersistentVolumeClaim` (PVC) for your SQLite database. This ensures the `download_tracker.db` file persists across pod restarts and rescheduling.
    *   Example PVC snippet:
        ```yaml
        apiVersion: v1
        kind: PersistentVolumeClaim
        metadata:
          name: app-data-pvc
        spec:
          accessModes:
            - ReadWriteOnce # Suitable for SQLite as it's single-writer
          resources:
            requests:
              storage: 1Gi # Adjust size as needed
        ```

3.  **Configuration and Secrets:**
    *   Store your API keys and sensitive data in Kubernetes `Secrets`.
    *   Store non-sensitive configurations (like default log level if not in .env, or bucket names if public) in `ConfigMaps`.
    *   Mount these into your pods as environment variables or files.

4.  **Discoverer Role Deployment:**
    *   **Daily Discoverer:** Use a `CronJob` resource to run the discoverer pod on a schedule (e.g., daily). The `CronJob` spec would define the container using your image and the command `["discoverer", "daily"]`.
    *   **Historical/On-Demand Discoverer:** Use a `Job` resource for one-off tasks. You can trigger these jobs manually or via other automation.

5.  **Worker Role Deployment:**
    *   Use a `Deployment` resource for the worker pods. This allows you to specify the number of replicas (pods) and manage updates.
    *   The container spec in the `Deployment` would use your image and the command `["worker"]`.
    *   Mount the `PersistentVolumeClaim` (e.g., `app-data-pvc`) to `/app/data` in the worker pods.
    *   Consider using a `HorizontalPodAutoscaler` (HPA) if you want to automatically scale the number of worker pods based on CPU/memory or custom metrics (though custom metrics for SQLite queue depth can be tricky).

6.  **Example Kubernetes Manifest Snippets (Conceptual):**

    *   **Secret (for .env contents):**
        ```bash
        # Create from your .env file
        kubectl create secret generic app-env --from-env-file=.env
        ```

    *   **CronJob for Daily Discoverer:**
        ```yaml
        apiVersion: batch/v1
        kind: CronJob
        metadata:
          name: discoverer-daily
        spec:
          schedule: "0 1 * * *" # Example: Run at 1 AM UTC daily
          jobTemplate:
            spec:
              template:
                spec:
                  restartPolicy: OnFailure
                  volumes:
                    - name: app-data-storage
                      persistentVolumeClaim:
                        claimName: app-data-pvc
                  containers:
                  - name: discoverer
                    image: your-registry/your-image-name:latest
                    command: ["python", "-m", "src.main", "discoverer", "daily"]
                    envFrom:
                      - secretRef:
                          name: app-env
                    volumeMounts:
                    - name: app-data-storage
                      mountPath: /app/data
        ```

    *   **Deployment for Workers:**
        ```yaml
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: worker-deployment
        spec:
          replicas: 3 # Example: 3 worker pods
          selector:
            matchLabels:
              app: polygon-worker
          template:
            metadata:
              labels:
                app: polygon-worker
            spec:
              restartPolicy: Always
              volumes:
                - name: app-data-storage
                  persistentVolumeClaim:
                    claimName: app-data-pvc
              containers:
              - name: worker
                image: your-registry/your-image-name:latest
                command: ["python", "-m", "src.main", "worker"]
                envFrom:
                  - secretRef:
                      name: app-env
                volumeMounts:
                - name: app-data-storage
                  mountPath: /app/data
        ```

**Important Note on SQLite and Concurrency:**
SQLite is a file-based database. While it supports concurrent reads, writes are typically serialized (one writer at a time for the whole database). With multiple worker pods accessing the same SQLite database file over a shared volume (like NFS or some cloud provider block storage in ReadWriteMany mode, or ReadWriteOnce if all pods are on the same node), you might encounter write contention or performance issues if write operations are very frequent and numerous. The `db_manager.py` uses standard SQLAlchemy practices which should handle SQLite's locking, but at high scale, this could become a bottleneck. For very high-concurrency needs, migrating to a server-based database like PostgreSQL or MySQL within your Kubernetes cluster or as a managed service would be a more robust long-term solution.

## Project Structure

```
polygon_b2_downloader_v2/
├── Dockerfile
├── docker-compose.yml
├── README.md
├── .env.example
├── requirements.txt
├── data/                     # Default location for SQLite DB (created if not exists, volume-mounted in Docker)
│   └── download_tracker.db   # (Example, actual DB file)
└── src/
    ├── __init__.py
    ├── main.py               # Main entry point to select discoverer or worker role
    ├── shared/
    │   ├── __init__.py
    │   ├── config.py         # Configuration loading and logging setup
    │   ├── db_manager.py     # SQLite database interactions and schema
    │   ├── polygon_client.py # Polygon.io S3 API interaction
    │   └── b2_client.py      # Backblaze B2 S3 API interaction
    ├── discoverer/
    │   ├── __init__.py
    │   └── main.py           # Logic for discoverer/scheduler role
    └── worker/
        ├── __init__.py
        └── main.py           # Logic for worker role
```

## Logging

Application logs are printed to the console (stdout/stderr), making them easy to capture by Docker, Docker Compose, morph.so, or Kubernetes logging systems.

