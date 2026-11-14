-- 02_integration.sql
-- Storage integration and external stage between Snowflake and the S3 bronze layer.
-- Prerequisite: IAM role created via snowflake/aws_iam_setup.sh (Steps 3 and 4).
-- Replace <role-arn> with: aws iam get-role --role-name snowflake-wa-streamflow --query Role.Arn --output text
-- After running DESC INTEGRATION, update the role's trust policy (Step 6). Do not commit the DESC output.
-- Never use CREATE OR REPLACE on the integration: it generates a new external ID and breaks the AWS trust policy.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1. Storage integration (read access limited to the bronze/ prefix)
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_wa_streamflow_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<role-arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://chaoyue-wa-streamflow/bronze/');

DESC INTEGRATION s3_wa_streamflow_int;

-- 2. External stage over the bronze layer
USE SCHEMA WA_STREAMFLOW.RAW;

CREATE STAGE IF NOT EXISTS bronze_stage
  URL = 's3://chaoyue-wa-streamflow/bronze/'
  STORAGE_INTEGRATION = s3_wa_streamflow_int;

-- 3. Check: should list the BoM and DWER files in bronze
LIST @bronze_stage;
