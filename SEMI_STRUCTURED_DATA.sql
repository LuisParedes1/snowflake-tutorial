PUBLIC_DATA.PUBLIC.SEC_FILINGS_INDEX_VIEW-- In this tutorial we'll be loading semi-structured date (.json file) from an external stage (AWS S3) 
-- into a new Snowflake table. 
-- https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html#5

------- Setting up context -------
USE ROLE SYSADMIN; ---> set the Role
USE WAREHOUSE COMPUTE_WH; ---> set the Warehouse
USE DATABASE PUBLIC_DATA; ---> set the Database
USE SCHEMA PUBLIC ---> set the Schema


------- Creating Snowflake table -------
-- Snowflake has a special data type called VARIANT that allows storing the entire JSON object as a single row and querying the object directly.
-- Semi-Structured Data Magic -> The VARIANT data type allows Snowflake to ingest semi-structured data without having to predefine the schema.
CREATE TABLE sec_filings_index (v variant);
CREATE TABLE sec_filings_attributes (v variant);

------- Adding the external stage -------
CREATE STAGE Public_Data_sec_filings
url = 's3://sfquickstarts/zero_to_snowflake/cybersyn_cpg_sec_filings/';

LIST @Public_Data_sec_filings;

------- Copying data from stage into table -------
COPY INTO sec_filings_index
FROM @Public_Data_sec_filings/cybersyn_sec_report_index.json.gz
    file_format = (type = json strip_outer_array = true);

COPY INTO sec_filings_attributes
FROM @Public_Data_sec_filings/cybersyn_sec_report_attributes.json.gz
    file_format = (type = json strip_outer_array = true);

------- Querying the tables -------
SELECT * FROM sec_filings_index LIMIT 10;
SELECT * FROM sec_filings_attributes LIMIT 10;


------- Create a columnar view of the semi-structured JSON -------
-- A view allows the result of a query to be accessed as if it were a table

CREATE OR REPLACE VIEW sec_filings_index_view AS
SELECT
    v:CIK::string                   AS cik,
    v:COMPANY_NAME::string          AS company_name,
    v:EIN::int                      AS ein,
    v:ADSH::string                  AS adsh,
    v:TIMESTAMP_ACCEPTED::timestamp AS timestamp_accepted,
    v:FILED_DATE::date              AS filed_date,
    v:FORM_TYPE::string             AS form_type,
    v:FISCAL_PERIOD::string         AS fiscal_period,
    v:FISCAL_YEAR::string           AS fiscal_year
FROM sec_filings_index;

-- Notice the results look just like a regular structured data source
SELECT * FROM sec_filings_index_view LIMIT 10;


CREATE OR REPLACE VIEW sec_filings_attributes_view AS
SELECT
    v:VARIABLE::string            AS variable,
    v:CIK::string                 AS cik,
    v:ADSH::string                AS adsh,
    v:MEASURE_DESCRIPTION::string AS measure_description,
    v:TAG::string                 AS tag,
    v:TAG_VERSION::string         AS tag_version,
    v:UNIT_OF_MEASURE::string     AS unit_of_measure,
    v:VALUE::string               AS value,
    v:REPORT::int                 AS report,
    v:STATEMENT::string           AS statement,
    v:PERIOD_START_DATE::date     AS period_start_date,
    v:PERIOD_END_DATE::date       AS period_end_date,
    v:COVERED_QTRS::int           AS covered_qtrs,
    TRY_PARSE_JSON(v:METADATA)    AS metadata
FROM sec_filings_attributes;

SELECT * FROM sec_filings_attributes_view LIMIT 10;