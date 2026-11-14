-- 01_setup.sql
-- One-time Snowflake setup for the WA streamflow pipeline.
-- Safe to re-run: nothing here is created twice or overwritten.

USE ROLE ACCOUNTADMIN;

-- 1. Cost guardrail: a 20-credit cap (notify at 80%, suspend at 100%)
CREATE RESOURCE MONITOR IF NOT EXISTS WA_RM
  WITH CREDIT_QUOTA = 20
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;

-- 2. Warehouse: the account's default COMPUTE_WH, kept small and quick to suspend
ALTER WAREHOUSE COMPUTE_WH SET
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = WA_RM;

-- 3. Database and RAW schema
CREATE DATABASE IF NOT EXISTS WA_STREAMFLOW;
CREATE SCHEMA IF NOT EXISTS WA_STREAMFLOW.RAW;

-- 4. Checks: resource_monitor should show WA_RM, auto_suspend should show 60
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SHOW RESOURCE MONITORS LIKE 'WA_RM';