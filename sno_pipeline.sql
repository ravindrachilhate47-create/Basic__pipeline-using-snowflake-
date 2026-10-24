
create table 
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

create file format 
CREATE OR REPLACE FILE FORMAT my_stages
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';
create stage 
CREATE OR REPLACE STAGE my_flights
  DIRECTORY = (ENABLE = TRUE);

make create stream 
CREATE OR REPLACE STREAM file_stream 
  ON STAGE my_flights;

make create task with copy command 
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


select * from flight_pp;










