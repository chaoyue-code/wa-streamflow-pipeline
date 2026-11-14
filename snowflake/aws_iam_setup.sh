#!/usr/bin/env bash
# aws_iam_setup.sh
# Creates the read-only IAM policy and role that let Snowflake read the S3 bronze layer.
# Run once, before snowflake/02_integration.sql.
# Requires: AWS CLI v2, credentials with IAM permissions.

set -euo pipefail

BUCKET="chaoyue-wa-streamflow"
POLICY_NAME="snowflake-wa-streamflow-read"
ROLE_NAME="snowflake-wa-streamflow"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# --- Step 3: read-only S3 policy (bronze/ prefix only) ---
cat > /tmp/snowflake-s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::${BUCKET}/bronze/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET}",
      "Condition": { "StringLike": { "s3:prefix": ["bronze/*"] } }
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::${BUCKET}"
    }
  ]
}
EOF

aws iam create-policy \
  --policy-name "${POLICY_NAME}" \
  --policy-document file:///tmp/snowflake-s3-policy.json

# --- Step 4: role with a placeholder trust policy, then attach the policy ---
cat > /tmp/snowflake-trust-placeholder.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::${ACCOUNT_ID}:root" },
      "Action": "sts:AssumeRole",
      "Condition": { "StringEquals": { "sts:ExternalId": "0000" } }
    }
  ]
}
EOF

aws iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document file:///tmp/snowflake-trust-placeholder.json

aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

echo "Role ARN (use as STORAGE_AWS_ROLE_ARN in 02_integration.sql):"
aws iam get-role --role-name "${ROLE_NAME}" --query Role.Arn --output text


# --- Step 5 is in Snowflake: run CREATE STORAGE INTEGRATION and DESC INTEGRATION in 02_integration.sql ---

# --- Step 6 (manual, after DESC INTEGRATION in Snowflake) ---
# Set these from DESC INTEGRATION output. Never commit the real values.
#   SF_USER_ARN='<STORAGE_AWS_IAM_USER_ARN>'
#   SF_EXT_ID='<STORAGE_AWS_EXTERNAL_ID>'   # not the role ARN
# Then write /tmp/snowflake-trust.json with Principal.AWS = $SF_USER_ARN
# and sts:ExternalId = $SF_EXT_ID, and run:
#   aws iam update-assume-role-policy --role-name snowflake-wa-streamflow \
#     --policy-document file:///tmp/snowflake-trust.json