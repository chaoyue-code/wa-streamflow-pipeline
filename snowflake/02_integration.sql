-- 02_integration.sql
-- Storage integration between Snowflake and the S3 bronze layer.
-- Prerequisite: IAM role created via snowflake/aws_iam_setup.sh (Steps 3 and 4).
-- Replace <role-arn> with: aws iam get-role --role-name snowflake-wa-streamflow --query Role.Arn --output text
-- After running DESC INTEGRATION, update the role's trust policy (Step 6). Do not commit the DESC output.

USE ROLE ACCOUNTADMIN;


USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION s3_wa_streamflow_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<role-arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://chaoyue-wa-streamflow/bronze/');

DESC INTEGRATION s3_wa_streamflow_int;

USE ROLE ACCOUNTADMIN;


USE WAREHOUSE compute_wh;
USE SCHEMA wa_streamflow.RAW;

CREATE STAGE bronze_stage
  URL = 's3://chaoyue-wa-streamflow/bronze/'
  STORAGE_INTEGRATION = s3_wa_streamflow_int;

LIST @bronze_stage;