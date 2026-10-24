# Basic__pipeline-using-snowflake-

# Automated Data Ingestion Pipeline using Snowflake Streams and Tasks

An end-to-end Data Engineering pipeline designed to automatically detect new data files uploaded to a Snowflake Stage and ingest them into target database tables in near real-time using Change Data Capture (CDC) concepts.

## Architecture Overview
The pipeline follows a modern, event-driven data engineering pattern:
1. Local CSV Data is staged to Snowflake internal storage via PUT.
2. Directory Tables are refreshed to capture file metadata.
3. A Snowflake Stream acts as a CDC layer, tracking whenever a new file arrives.
4. A scheduled Snowflake Task executes every minute, checking the stream condition.
5. If new files are detected, COPY INTO consumes the stream data using METADATA$FILENAME to prevent duplicate ingestion.

## Tech Stack & Concepts
- Database / Data Warehouse: Snowflake
- Data Engineering Concepts: Change Data Capture (CDC), Idempotency, Task Automation, File Formats, Internal Staging
- SQL Commands: Streams, Tasks, Directory Tables, Metadata Querying

## SQL Pipeline Implementation

### 1. Database & Table Setup
Creates the structural landing zone for incoming flight data.

CREATE OR REPLACE TABLE flight_pp(
    fl_date date,
    op_carrier varchar(50),
    op_carrier_num varchar(50),
    origin_s varchar(50),
    dest varchar(50),
    crs_dep_time int,
    dep_time int,
    dep_delay int,
    tax_out int,
    whrris_of int    
);

### 2. File Format & Stage Creation
Configures the stage with a Directory Table enabled to dynamically audit file arrivals.

CREATE OR REPLACE FILE FORMAT my_stages
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE my_flights
  DIRECTORY = (ENABLE = TRUE);

### 3. Change Data Capture (CDC) Layer
A stream is placed on top of the stage directory to flag new files instantly.

CREATE OR REPLACE STREAM file_stream 
  ON STAGE my_flights;

### 4. Automated Task Engine
The task evaluates the stream state every 1 minute. It automatically skips warehouse execution if no new files exist, optimizing cloud compute costs.

CREATE OR REPLACE TASK first_task
  WAREHOUSE = compute_wh
  SCHEDULE = '1 MINUTE'
  WHEN system$stream_has_data('file_stream')
AS
  COPY INTO flight_pp
  FROM @my_flights
  FILE_FORMAT = (FORMAT_NAME = my_stages, DATE_FORMAT = 'DD-MM-YYYY')
  FILES = (SELECT METADATA$FILENAME FROM file_stream);

-- Activate the Task
ALTER TASK first_task RESUME;

## Key Data Engineering Safeguards Applied
- Cost Optimization (WHEN clause): The warehouse is kept asleep and costs $0 unless the Stream explicitly verifies new data exists.
- Data Idempotency (Anti-Duplication): By leveraging FILES = (SELECT METADATA$FILENAME FROM file_stream), the pipeline targets only newly staged filenames, ensuring historically processed files are never re-ingested.
- Operational Auditing: Uses Snowflake's information_schema.task_history to monitor pipeline run states (SUCCEEDED, SKIPPED, FAILED).
