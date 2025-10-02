------- Setting up context -------
-- Snowflake's powerful Time Travel feature enables accessing historical data, as well as the objects storing the data, at any point within a period of time. The default window is 24 hours and, if you are using Snowflake Enterprise Edition, can be increased up to 90 days.
USE ROLE SYSADMIN; ---> set the Role
USE WAREHOUSE ANALYTICS_WH; ---> set the Warehouse
USE DATABASE PUBLIC_DATA; ---> set the Database
USE SCHEMA PUBLIC ---> set the Schema

------- Restoring a Table -------

-- Removing SEC_FILINGS_INDEX table
DROP TABLE sec_filings_index;

-- This throws error:
SELECT * FROM sec_filings_index LIMIT 10;

-- Restoring SEC_FILINGS_INDEX table
UNDROP TABLE sec_filings_index;

-- This works
SELECT * FROM sec_filings_index LIMIT 10;


------- Roll Back a Table -------
-- Let's roll back the COMPANY_METADATA table in the Public_Data database to a previous state to fix an unintentional DML error that replaces all the company names in the table with the word "oops".

-- Using the proper context
USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE Public_Data;
USE SCHEMA public;

-- Setting company names to 'oops'
UPDATE company_metadata SET company_name = 'oops';

SELECT * FROM company_metadata;

--  In Snowflake, we can simply run a command to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID. Use Time Travel to recreate the table with the correct company names and verify the company names have been restored:

-- Set the session variable for the query_id
SET query_id = (
  SELECT query_id
  FROM TABLE(information_schema.query_history_by_session(result_limit=>5))
  WHERE query_text LIKE 'UPDATE%'
  ORDER BY start_time DESC
  LIMIT 1
);

-- Use the session variable with the identifier syntax (e.g., $query_id)
CREATE OR REPLACE TABLE company_metadata AS
SELECT *
FROM company_metadata
BEFORE (STATEMENT => $query_id);

-- Verify the company names have been restored
SELECT * FROM company_metadata;