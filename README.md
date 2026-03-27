# ETL Pipeline

## What it does
Extracts 100 posts from a public API, transforms and cleans the data, loads into a SQLite database.

## Project structure
- logger.py — logging setup with run_id traceability
- config.py — centralized configuration from .env
- extract.py — API extraction with retry logic
- transform.py — data cleaning and validation
- load.py — SQLite upsert with idempotency
- main.py — pipeline orchestration

## How to run
1. Clone the repository
2. Install dependencies: pip install requests python-dotenv
3. Create .env file with API_URL, DB_PATH, LOG_FILE, RETRY_ATTEMPTS
4. Run: python main.py

## Architecture
API → extract.py → transform.py → load.py → SQLite DB
All steps logged to pipeline.log with unique run_id per execution