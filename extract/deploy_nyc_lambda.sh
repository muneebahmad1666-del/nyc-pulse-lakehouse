#!/usr/bin/env bash
# Creates or updates the IAM role and the NYC extractor Lambda. Safe to re-run.
#
#   bash deploy_nyc_lambda.sh            deploy the code; the `dev` alias follows the latest code
#   bash deploy_nyc_lambda.sh promote    publish the current code as a version and point `prod` at it
#
# Optional: export SODA_APP_TOKEN=... before deploying (free token from NYC Open Data) so the
# 311 API does not throttle you.
#
# Try new code with `invoke_nyc.py test ...` (alias dev, writes raw/test/).
# Promote only after a test month looks right; then `invoke_nyc.py raw ...` (alias prod)
# is the only thing that writes the shared raw/ folder.
set -euo pipefail

export AWS_DEFAULT_REGION=us-east-1
BUCKET=tb-scale-lakehouse-852824353987-us-east-1-an
ROLE=tb-scale-nyc-lambda-role
FN=tb-scale-nyc-extract
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

upsert_alias() {  # usage: upsert_alias <alias-name> <function-version>
  if aws lambda get-alias --function-name "$FN" --name "$1" >/dev/null 2>&1; then
    aws lambda update-alias --function-name "$FN" --name "$1" --function-version "$2" >/dev/null
  else
    aws lambda create-alias --function-name "$FN" --name "$1" --function-version "$2" >/dev/null
  fi
}

if [[ "${1:-deploy}" == "promote" ]]; then
  VERSION=$(aws lambda publish-version --function-name "$FN" --query Version --output text)
  upsert_alias prod "$VERSION"
  echo "prod now runs version $VERSION"
  exit 0
fi

# 1. The role the function runs as: logs, plus read/write under raw/ only (raw/test/ is inside it).
#    ListBucket is needed so a missing file is a clean 404 (without it S3 answers 403).
POLICY=$(cat <<EOP
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:PutObject","s3:GetObject"],
  "Resource":["arn:aws:s3:::${BUCKET}/raw/*"]},
 {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${BUCKET}"}]}
EOP
)

NEW_ROLE=0
if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROLE" --tags Key=project,Value=tb-scale-lakehouse \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  NEW_ROLE=1
fi
aws iam attach-role-policy --role-name "$ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam put-role-policy --role-name "$ROLE" --policy-name s3-raw-write --policy-document "$POLICY"
if [[ "$NEW_ROLE" == 1 ]]; then
  sleep 15  # let a brand-new role propagate before Lambda uses it
fi

ENVVARS="BUCKET=${BUCKET}"
if [[ -n "${SODA_APP_TOKEN:-}" ]]; then
  ENVVARS="${ENVVARS},SODA_APP_TOKEN=${SODA_APP_TOKEN}"
fi

# 2. Package and create/update the function (NOT in a VPC, so no NAT gateway charges).
#    10 GB of ephemeral storage: the yearly Citi Bike zips are downloaded to /tmp.
zip -j -q function.zip lambda_nyc.py
if aws lambda get-function --function-name "$FN" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FN" --zip-file fileb://function.zip >/dev/null
  aws lambda wait function-updated --function-name "$FN"
  aws lambda update-function-configuration --function-name "$FN" --timeout 900 --memory-size 2048 \
    --ephemeral-storage Size=10240 --environment "Variables={${ENVVARS}}" >/dev/null
  aws lambda wait function-updated --function-name "$FN"
else
  for attempt in 1 2 3 4 5; do
    aws lambda create-function --function-name "$FN" --runtime python3.12 \
      --handler lambda_nyc.handler --role "arn:aws:iam::${ACCOUNT}:role/${ROLE}" \
      --zip-file fileb://function.zip --timeout 900 --memory-size 2048 \
      --ephemeral-storage Size=10240 \
      --environment "Variables={${ENVVARS}}" --tags project=tb-scale-lakehouse >/dev/null && break
    echo "role not ready yet, retrying ($attempt/5)..."
    sleep 8
  done
  aws lambda wait function-active-v2 --function-name "$FN"
fi

# 3. Keep logs for 14 days instead of forever.
aws logs create-log-group --log-group-name "/aws/lambda/$FN" 2>/dev/null || true
aws logs put-retention-policy --log-group-name "/aws/lambda/$FN" --retention-in-days 14

# 4. `dev` always runs the newest code. `prod` only moves when you run: bash deploy_nyc_lambda.sh promote
upsert_alias dev '$LATEST'

echo "Deployed $FN (alias dev). Test one small unit:"
echo "  aws lambda invoke --function-name $FN:dev --cli-binary-format raw-in-base64-out \\"
echo "    --payload '{\"zone\":\"test\",\"source\":\"311\",\"day\":\"2024-01-05\"}' out.json && cat out.json"