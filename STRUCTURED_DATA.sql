PUBLIC_DATA.PUBLIC.PUBLIC_DATA_COMPANY_METADATAPUBLIC_DATA.PUBLIC.PUBLIC_DATA_COMPANY_METADATAPUBLIC_DATA.PUBLIC.COMPANY_METADATA 

-- In this tutorial we'll be loading structured date (.csv file) from an external stage (AWS S3) 
-- into a new Snowflake table. 
-- https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html#4

------- Setting up context -------
-- On the upper right corner, make sure to set the following values

-- Role: SYSADMIN
-- WAREHOUSE: COMPUTE_WH
-- DATABASE: PUBLIC_DATA
-- SCHEMA: PUBLIC

-- Alternatively, we can run:

-- USE ROLE SYSADMIN; ---> set the Role
-- USE WAREHOUSE COMPUTE_WH; ---> set the Warehouse
-- USE DATABASE PUBLIC_DATA; ---> set the Database
-- USE SCHEMA PUBLIC ---> set the Schema


------- Creating Snowflake table -------
-- Data Definition Language (DDL) operations are free!
CREATE OR REPLACE TABLE company_metadata (
    cybersyn_company_id string,
    company_name string,
    permid_security_id string,
    primary_ticker string,
    security_name string,
    asset_class string,
    primary_exchange_code string,
    primary_exchange_name string,
    security_status string,
    global_tickers variant,
    exchange_code variant,
    permid_quote_id variant
);

------- Adding the external stage -------
-- From console UI
-- Ingestion -> Add Data -> AWS S3

-- We could also run:
-- CREATE STAGE external_stage
-- url = 's3://path_to_file/'
-- file_format = (type = csv);

-- LIST the files within the external stage
LIST @Public_Data_company_metadata;

------- Creating file format -------
-- create a file format that matches the data structure
CREATE OR REPLACE FILE FORMAT csv
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'  -- Automatically determines the compression of files
    FIELD_DELIMITER = ','  -- Specifies comma as the field delimiter
    RECORD_DELIMITER = '\n'  -- Specifies newline as the record delimiter
    SKIP_HEADER = 1  -- Skip the first line
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  -- Fields are optionally enclosed by double quotes (ASCII code 34)
    TRIM_SPACE = FALSE  -- Spaces are not trimmed from fields
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Does not raise an error if the number of fields in the data file varies
    ESCAPE = 'NONE'  -- No escape character for special character escaping
    ESCAPE_UNENCLOSED_FIELD = '\134'  -- Backslash is the escape character for unenclosed fields
    DATE_FORMAT = 'AUTO'  -- Automatically detects the date format
    TIMESTAMP_FORMAT = 'AUTO'  -- Automatically detects the timestamp format
    NULL_IF = ('')  -- Treats empty strings as NULL values
    COMMENT = 'File format for ingesting data for zero to snowflake';

-- Verify the file format has been created with the correct settings
SHOW FILE FORMATS IN DATABASE PUBLIC_DATA;


------- Copying data from stage into table -------


-- Running with Warehouse of size: Small
------------------------------------------
COPY INTO company_metadata
FROM @Public_Data_company_metadata 
file_format = csv
PATTERN = '.*csv.*'
ON_ERROR = 'CONTINUE';


SELECT *
FROM company_metadata
LIMIT 10;

SELECT COUNT(*)
FROM company_metadata;



-- Running with Warehouse of size: LARGE
------------------------------------------

-- First we clear the table
TRUNCATE TABLE company_metadata;

-- Verify that the table is empty:
SELECT * FROM company_metadata LIMIT 10;

-- Changing the warehouse size to LARGE:
ALTER WAREHOUSE compute_wh SET warehouse_size='large';

-- Verify the change:
SHOW WAREHOUSES;

COPY INTO company_metadata FROM @Public_Data_company_metadata file_format=csv PATTERN = '.*csv.*' ON_ERROR = 'CONTINUE';