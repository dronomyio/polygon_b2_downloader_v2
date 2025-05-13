# Project Todo List (Rewrite for Scalability)

## Phase 1: Design and Setup (New Architecture)
- [x] Clarify new architecture and database requirements (Done)
- [x] Define application architecture: Discoverer/Scheduler, Workers, SQLite DB (Done)
- [x] Create new project directory structure (`polygon_b2_downloader_v2`)
- [x] Initialize `requirements.txt` (include `python-dotenv`, `requests`, `boto3`, `sqlalchemy` for SQLite)
- [x] Create `.env.example` file (similar to before)
- [x] Create `README.md` (initial draft for new architecture)

## Phase 2: Shared Modules Implementation
- [x] Implement configuration loading (`shared/config.py`)
- [x] Implement SQLite database manager (`shared/db_manager.py`)
    - [x] Define database schema (files table: file_key, status, timestamps, retry_count, error_message, worker_id)
    - [x] Function to initialize DB and create tables
    - [x] Functions for CRUD operations on the files table (add task, get pending task with locking/status update, update task status/retries)
- [x] Re-use/adapt Polygon.io client (`shared/polygon_client.py`)
- [x] Re-use/adapt Backblaze B2 client (`shared/b2_client.py`)

## Phase 3: Discoverer Component Implementation
- [x] Implement Discoverer logic (`discoverer/main.py` or as a mode in `main.py`)
    - [x] Argument parsing for modes (historical, daily, on-demand --dates)
    - [x] Logic to list files from Polygon.io based on mode
    - [x] Logic to check against database and add new/re-triable tasks to SQLite DB with "pending" status

## Phase 4: Worker Component Implementation
- [x] Implement Worker logic (`worker/main.py` or as a mode in `main.py`)
    - [x] Logic to continuously query SQLite DB for "pending" tasks
    - [x] Task acquisition: update status to "processing" / assign worker_id (to prevent race conditions)
    - [x] Download file using Polygon client
    - [x] Update DB status: "downloaded" or "failed_download" (manage retries, max 2 retries)
    - [x] Upload file using B2 client
    - [x] Update DB status: "completed" or "failed_upload" (manage retries, max 2 retries)
    - [x] Cleanup local downloaded file

## Phase 5: Application Packaging (Docker)
- [x] Create main entry point (`main.py`) to select discoverer or worker role based on argument
- [x] Create `Dockerfile` for the new application structure
    - [x] Ensure it can run either the discoverer or worker role
    - [x] Manage SQLite database location (expecting a volume mount)
- [x] Create `docker-compose.yml`
    - [x] Define service for Discoverer (can be run as a one-off command)
    - [x] Define service for Workers (can be scaled)
    - [x] Define volume for SQLite database persistence

## Phase 6: Documentation and Deployment Guidelines
-- [x] Update `README.md` comprehensively:
    - [x] New architecture overview
    - [x] Setup instructions (including SQLite DB file location)
    - [x] How to run Discoverer (historical, daily, on-demand)
    - [x] How to run Workers
    - [x] Instructions for using `docker-compose`
    - [x] Guidelines for deploying on morph.so/Kubernetes (emphasizing persistent volume for SQLite, scaling workers, running discoverer as a job/cronjob)m

## Phase 8: Final Packaging and Delivery
- [ ] Package all deliverables (source code, Dockerfile, docker-compose.yml, .env.example, README.md)
- [ ] Send deliverables to user
