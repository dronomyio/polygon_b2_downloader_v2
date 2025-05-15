# Polygon.io to Backblaze B2 Downloader (Scalable Version v2)

This application is designed to download flat files (specifically US Options Day Aggregates) from Polygon.io, track their status in a local SQLite database, and upload them to a Backblaze B2 bucket. It is structured to support scalable deployment, for example, using multiple worker instances on platforms like morph.so or Kubernetes.

The target S3 path for US Options Day Aggregates is `us_options_opra/day_aggs_v1/`.

## Architecture Overview

The application consists of two main components (roles):

1.  **Discoverer/Scheduler:**
    *   Identifies US Options Day Aggregates files to be processed from Polygon.io (using the `us_options_opra/day_aggs_v1/` path) based on the operational mode (historical, daily, on-demand).
    *   Adds these files as tasks to a shared SQLite database with a "pending" status. It avoids adding duplicate `file_key` entries.
    *   Typically run as a scheduled job (for daily tasks) or a one-off task (for historical backfill or on-demand requests).

2.  **Worker(s):**
    *   Multiple worker instances can run concurrently.
    *   Each worker queries the SQLite database for "pending" tasks or tasks that previously failed but are eligible for retry.
    *   Atomically claims a task by updating its status to "processing" and assigning its `worker_id`.
    *   Downloads the file from Polygon.io (from the `us_options_opra/day_aggs_v1/` path) using dedicated S3 credentials.
    *   Uploads the file to Backblaze B2.
    *   Updates the file's status in the SQLite database throughout the process (e.g., downloading, downloaded, uploading, completed, failed_download, failed_upload, permanent_failure).
    *   Handles retries for failed operations (up to 2 retries as configured in `db_manager.MAX_RETRIES`).

## Features

-   Scalable architecture with separate discoverer and worker roles.
-   Downloads **US Options Day Aggregates** from Polygon.io.
-   Tracks download and upload status in a local SQLite database (`data/download_tracker.db` by default).
-   Supports historical, daily, and on-demand data processing modes for the discoverer for US Options Day Aggregates.
-   Uploads downloaded files to a specified Backblaze B2 bucket.
-   Uses a `.env` file for configuration (API keys, S3 credentials, bucket details, database URL, log level).
-   Logs errors and manages retries for processing tasks.
-   Dockerized for deployment, with `docker-compose` support for local execution and role management.

## Prerequisites

-   Docker and Docker Compose installed on your local machine or deployment environment.
-   **Polygon.io Account with S3 Flat File Access Credentials:**
    *   **S3 Access Key ID** (for `POLYGON_S3_ACCESS_KEY_ID`)
    *   **S3 Secret Access Key** (for `POLYGON_S3_SECRET_ACCESS_KEY`)
    *   These are **DIFFERENT** from your general Polygon.io API Key. Obtain them from your Polygon.io dashboard under the "Accessing Flat Files (S3)" section. Ensure these credentials have access to the `us_options_opra/day_aggs_v1/` path in the `flatfiles` bucket.
-   (Optional) Your general Polygon.io API Key (for `POLYGON_API_KEY`), if you plan to extend the application to use their REST APIs for other purposes.
-   Backblaze B2 Account with:
    *   Key ID (for `B2_KEY_ID`)
    *   Application Key (for `B2_APPLICATION_KEY`)
    *   Bucket Name (for `B2_BUCKET_NAME`)
    *   S3 Endpoint URL (e.g., `s3.us-west-000.backblazeb2.com` for `B2_ENDPOINT_URL`)

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
        *   `POLYGON_S3_ACCESS_KEY_ID`: **Your dedicated S3 Access Key ID from Polygon.io for flat file access.**
        *   `POLYGON_S3_SECRET_ACCESS_KEY`: **Your dedicated S3 Secret Access Key from Polygon.io for flat file access.**
        *   `POLYGON_API_KEY`: (Optional) Your general Polygon.io API key.
        *   `B2_KEY_ID`: Your Backblaze B2 Key ID.
        *   `B2_APPLICATION_KEY`: Your Backblaze B2 Application Key.
        *   `B2_BUCKET_NAME`: The name of your Backblaze B2 bucket where files will be stored.
        *   `B2_ENDPOINT_URL`: The S3 endpoint URL for your B2 bucket's region.
        *   `DATABASE_URL`: The connection string for the SQLite database. By default, it's `sqlite:///data/download_tracker.db`, meaning the database file `download_tracker.db` will be created inside a `data` subdirectory within the project (or `/app/data` inside the Docker container). This path is important for volume mounting.
        *   `LOG_LEVEL`: Set the desired logging level (e.g., `INFO`, `DEBUG`, `WARNING`, `ERROR`). Defaults to `INFO`.

3.  **Database Directory:**
    *   The application will attempt to create the `data` directory if it doesn't exist (as specified by the default `DATABASE_URL`). When using Docker, this directory inside the container (`/app/data`) will be mapped to a persistent volume to ensure the database survives container restarts.

## Running the Application with Docker Compose (Recommended for Local Execution)

`docker-compose.yml` is provided to simplify running the discoverer and worker roles for US Options Day Aggregates.

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
   The discoverer populates the database with tasks for US Options Day Aggregates. It's typically run as a one-off command.

<<<<<<< HEAD
   *   **Historical Mode (to discover all available US Options Day Aggregates files, or within a date range):**
=======
   

   *   **Historical Mode (to discover all available files, or within a date range):**

   *   
       Historical Mode:
When you run the discoverer in historical mode (e.g., discoverer historical or discoverer historical --start_date YYYY-MM-DD --end_date YYYY-MM-DD):
The PolygonClient's list_us_stocks_daily_files method is called. This method queries Polygon.io's S3 storage for files matching the prefix us_stocks_sip/day_aggs_v1/.
Polygon.io itself organizes its files using this date-based path structure. So, the S3 keys returned by Polygon.io (via the list_objects_v2 S3 API call) will already have the YYYY/YYYY-MM-DD.csv.gz format.
The list_us_stocks_daily_files method in our PolygonClient then parses the date from each S3 key it finds to see if it falls within the optional start_date and end_date range you might provide. If no range is provided, it attempts to list all matching files.
Source of YYYY, MM, DD: Directly from the S3 object keys provided by Polygon.io, then filtered by the application if a date range is specified.

     
>>>>>>> 2a71fca8076908ae62422d1cd8c29536786b63e7
       ```bash
       # Discover all historical US Options Day Aggregates files
       docker-compose run --rm discoverer discoverer historical

       # Discover historical US Options Day Aggregates files within a specific date range
       docker-compose run --rm discoverer discoverer historical --start_date YYYY-MM-DD --end_date YYYY-MM-DD
       ```
       Replace `YYYY-MM-DD` with actual dates.

<<<<<<< HEAD
   *   **Daily Mode (to discover yesterday's US Options Day Aggregates file):**
=======
   *   **Daily Mode (to discover yesterday's file):**

   *   When you run the discoverer in daily mode (e.g., discoverer daily):
The discoverer/main.py script calculates yesterday's date (e.g., if today is 2023-05-14, yesterday was 2023-05-13).
It then formats this calculated date to get the year (YYYY, e.g., "2023") and the full date string (YYYY-MM-DD, e.g., "2023-05-13").
It constructs the expected S3 file key using these formatted date parts: f"us_stocks_sip/day_aggs_v1/{year_str}/{date_str}.csv.gz".
Source of YYYY, MM, DD: Calculated by the application based on the current date (to determine yesterday).

>>>>>>> 2a71fca8076908ae62422d1cd8c29536786b63e7
       ```bash
       docker-compose run --rm discoverer discoverer daily
       ```
       This is suitable for scheduling via cron or a similar task scheduler.

<<<<<<< HEAD
   *   **On-Demand Mode (to discover US Options Day Aggregates files for specific dates):**
=======
   *   **On-Demand Mode (to discover files for specific dates):**

   *   When you run the discoverer in on-demand mode with specific dates (e.g., discoverer on-demand --dates YYYY-MM-DD,YYYY-MM-DD):
The script takes the date strings you provide (e.g., "2023-01-15").
For each provided date string, it parses it to get the year (YYYY) and the full date string (YYYY-MM-DD).
It then constructs the S3 file key using these parts, similar to the daily mode: f"us_stocks_sip/day_aggs_v1/{year_str}/{date_str}.csv.gz".
Source of YYYY, MM, DD: Directly from the date strings provided by you as command-line arguments.
In all cases, the base path structure us_stocks_sip/day_aggs_v1/ is hardcoded in the PolygonClient as the prefix for listing US stocks daily aggregate files, as this is the known location for these files in Polygon.io's S3 storage. The date components are then either derived from Polygon.io's S3 listing (for historical) or constructed by the application based on the operational mode and any user-provided dates (for daily and on-demand).

>>>>>>> 2a71fca8076908ae62422d1cd8c29536786b63e7
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

morph.so is a platform for running cron jobs, background workers, and APIs. Here's how you might adapt this application for US Options Day Aggregates:

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
    *   Securely provide your API keys and other configurations (from your `.env` file, especially the `POLYGON_S3_ACCESS_KEY_ID` and `POLYGON_S3_SECRET_ACCESS_KEY`) as environment variables to your services/jobs on morph.so. Most platforms have a way to manage secrets or environment variables for deployed applications.

### Deploying on Kubernetes (General Guidance)

Kubernetes offers a more complex but powerful environment for the US Options Day Aggregates downloader.

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
    *   Store your API keys and S3 credentials in Kubernetes `Secrets`.
    *   Store non-sensitive configurations in `ConfigMaps`.
    *   Mount these into your pods as environment variables or files.

4.  **Discoverer Role Deployment:**
    *   **Daily Discoverer:** Use a `CronJob` resource to run the discoverer pod on a schedule (e.g., daily). The `CronJob` spec would define the container using your image and the command `["python", "-m", "src.main", "discoverer", "daily"]`.
    *   **Historical/On-Demand Discoverer:** Use a `Job` resource for one-off tasks.

5.  **Worker Role Deployment:**
    *   Use a `Deployment` resource for the worker pods. This allows you to specify the number of replicas (pods) and manage updates.
    *   The container spec in the `Deployment` would use your image and the command `["python", "-m", "src.main", "worker"]`.
    *   Mount the `PersistentVolumeClaim` (e.g., `app-data-pvc`) to `/app/data` in the worker pods.

6.  **Example Kubernetes Manifest Snippets (Conceptual):**

    *   **Secret (for .env contents):**
        ```bash
        # Create from your .env file (ensure it contains the correct Polygon S3 keys for Options data)
        kubectl create secret generic app-env --from-env-file=.env
        ```

    *   **CronJob for Daily Discoverer:**
        ```yaml
        apiVersion: batch/v1
        kind: CronJob
        metadata:
          name: discoverer-daily-options
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
          name: worker-deployment-options
        spec:
          replicas: 3 # Example: 3 worker pods
          selector:
            matchLabels:
              app: polygon-worker-options
          template:
            metadata:
              labels:
                app: polygon-worker-options
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
SQLite is a file-based database. While it supports concurrent reads, writes are typically serialized. With multiple worker pods accessing the same SQLite database file over a shared volume, you might encounter write contention or performance issues if write operations are very frequent. The `db_manager.py` uses standard SQLAlchemy practices which should handle SQLite's locking, but at high scale, this could become a bottleneck. For very high-concurrency needs, migrating to a server-based database like PostgreSQL or MySQL would be a more robust long-term solution.

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
    │   ├── polygon_client.py # Polygon.io S3 API interaction (for US Options Day Aggregates)
    │   └── b2_client.py      # Backblaze B2 S3 API interaction
    ├── discoverer/
    │   ├── __init__.py
    │   └── main.py           # Logic for discoverer/scheduler role (for US Options Day Aggregates)
    └── worker/
        ├── __init__.py
        └── main.py           # Logic for worker role
```

## Logging

Application logs are printed to the console (stdout/stderr), making them easy to capture by Docker, Docker Compose, morph.so, or Kubernetes logging systems.

## Conceptual Architecture

![polygon_b2_downloader_architecture](https://github.com/user-attachments/assets/ef92ac0d-ae31-4cf0-8b21-8ff7b991a0b4)


conceptual architecture diagram for the Polygon B2 Downloader application. Please find it attached.
Here is an explanation of the components and their interactions as depicted in the diagram:

## Polygon B2 Downloader - Architecture Explained:

User / Operator: This represents you, interacting with the system. You initiate operations by running docker-compose commands in your terminal (CLI commands).

 .env File: This file, located in your project root, stores sensitive configuration data like your API keys for Polygon.io and Backblaze B2, as well as other settings like bucket names and endpoint URLs. Docker Compose reads this file to inject these settings as environment variables into the services it manages.

Docker Compose: This tool orchestrates the different services (containers) that make up the application. Based on the docker-compose.yml file, it starts, stops, and manages the Discoverer and Worker services. It also handles the creation and management of the shared Docker volume for the SQLite database.

## Discoverer Service (Docker Container):

Input: Receives commands (e.g., discoverer historical, discoverer daily) and parameters like start/end dates from the User via docker-compose run ....

Interaction with Polygon.io S3: It connects to the Polygon.io S3 service (an external system) using the provided S3 credentials to list the available financial data files for the specified dataset (e.g., US Stocks Daily Aggregates) and date range.

Interaction with Shared SQLite Database: For each relevant file found on Polygon.io S3, the Discoverer creates a new task entry in the Shared SQLite Database. This entry typically includes the file key (S3 path), its date, and an initial status (e.g., pending).

## Worker Service (Docker Container):

Polling Task Database: The Worker continuously polls the Shared SQLite Database at regular intervals (e.g., every 10 seconds) to check for new tasks with a pending status.

File Download from Polygon.io S3: When a pending task is found, the Worker connects to Polygon.io S3 and downloads the specified data file to a temporary local directory within its container.

Database Update (Downloaded): After a successful download, the Worker updates the task's status in the Shared SQLite Database to downloaded.

File Upload to Backblaze B2 S3: The Worker then connects to your Backblaze B2 S3 bucket (an external system) using your B2 credentials and uploads the downloaded file from its temporary local path to the corresponding path in your B2 bucket.

Database Update (Completed/Failed): Upon successful upload, the Worker updates the task's status in the Shared SQLite Database to completed. If any step fails (download or upload), it updates the status to failed or permanent_failure after retries (if implemented).

Local Cleanup: After a successful upload (or a permanent failure where no more retries are needed), the Worker deletes the temporary local copy of the downloaded file from its container to free up space.

## Shared SQLite Database (Docker Volume):

This is a persistent database file (e.g., download_tracker.db) stored in a Docker-managed volume (like app_data in your setup). This ensures the task data persists even if the containers are stopped and restarted.
It acts as the central communication and coordination point between the Discoverer (which writes new tasks) and the Worker(s) (which read and update tasks).

Polygon.io S3 Service (External): This is the third-party service provided by Polygon.io that hosts the raw financial data files. The application reads (lists and downloads) from this service.

Backblaze B2 S3 Service (External): This is your chosen cloud storage provider. The application writes (uploads) the processed data files to your bucket on this service.

## Data Flow Summary:

The user initiates a discovery process.
The Discoverer finds files on Polygon.io and creates tasks in the SQLite database.
The Worker picks up tasks from the database, downloads files from Polygon.io, uploads them to Backblaze B2, and updates task statuses in the database.
This architecture allows for a decoupled system where the discovery of files and the processing/transferring of files are handled by separate, scalable components, coordinated through a persistent task queue.
I hope this explanation and the diagram are helpful! Let me know if you have any questions or would like any part clarified.

