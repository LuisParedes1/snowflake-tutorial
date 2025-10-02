-- ACCOUNTADMIN encapsulates the SYSADMIN and SECURITYADMIN system-defined roles.
USE ROLE accountadmin;

-- Before a role can be used for access control, at least one user must be assigned to it.
CREATE ROLE junior_dba;

-- Setting variable with current user name
SET user = current_user();
GRANT ROLE junior_dba TO USER IDENTIFIER($user);


-- Change your worksheet context to the new JUNIOR_DBA role:
USE ROLE junior_dba;

-- Grating JUNIOR_DBA usage privileges to COMPUTE_WH warehouse.
USE ROLE accountadmin;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE junior_dba;

-- Using JUNIOR_DBA role and COMPUTE_WH warehouse.
USE ROLE junior_dba;
USE WAREHOUSE compute_wh;

-- Grating JUNIOR_DBA role the USAGE privilege required to view and use the Public_data and WEATHER_SOURCE_LLC_FROSTBYTE
USE ROLE accountadmin;

GRANT USAGE ON DATABASE Public_Data TO ROLE junior_dba;

GRANT IMPORTED PRIVILEGES ON DATABASE WEATHER_SOURCE_LLC_FROSTBYTE TO ROLE junior_dba;

USE ROLE junior_dba;