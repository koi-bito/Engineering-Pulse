# Operations Runbook

Deployment, operations, and backup recovery procedures for **Engineering Pulse** on AWS.

## 1. One-Time Remote State Bootstrap (Terraform)

Create the S3 state bucket and DynamoDB lock table before running `terraform init`:

```bash
export AWS_REGION=us-east-1
export TF_STATE_BUCKET=engineering-pulse-tfstate-prod
export TF_LOCK_TABLE=engineering-pulse-tf-locks

aws s3api create-bucket --bucket "$TF_STATE_BUCKET" --region "$AWS_REGION"
aws s3api put-bucket-versioning --bucket "$TF_STATE_BUCKET" --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket "$TF_STATE_BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws dynamodb create-table \
  --table-name "$TF_LOCK_TABLE" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$AWS_REGION"
```

## 2. Provision AWS Infrastructure

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# Set globally unique raw_events_bucket_name and frontend_bucket_name in terraform.tfvars

terraform init \
  -backend-config="bucket=$TF_STATE_BUCKET" \
  -backend-config="region=$AWS_REGION" \
  -backend-config="dynamodb_table=$TF_LOCK_TABLE"

terraform plan -out=tfplan
terraform apply tfplan
```

Populate the runtime application secrets in AWS Secrets Manager (`app_secrets_arn` output), then start the stack on EC2 if initial user-data booted before secrets existed:

```bash
aws secretsmanager put-secret-value \
  --secret-id engineering-pulse/app-secrets \
  --secret-string '{
    "github_webhook_secret": "<REPLACE_GITHUB_SECRET>",
    "jira_webhook_secret": "<REPLACE_JIRA_SECRET>",
    "grafana_admin_password": "<REPLACE_GRAFANA_PASSWORD>"
  }'
```

## 3. Connect to EC2 & Manage Services

SSH port 22 is closed by default; use **AWS Systems Manager Session Manager**:

```bash
EC2_ID=$(terraform -chdir=infra output -raw ec2_instance_id)
aws ssm start-session --target "$EC2_ID"

# Inside EC2 session (or SSH if ssh_allowed_cidr was configured):
cd /opt/engineering-pulse
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs -f api github-consumer jira-consumer
```

Restart or deploy backend services manually:

```bash
cd /opt/engineering-pulse
git rev-parse HEAD > /tmp/last-good-sha
git pull --ff-only origin main
docker compose -f docker-compose.prod.yml run --rm api alembic upgrade head
docker compose -f docker-compose.prod.yml up -d --build
curl -fsS http://localhost:8000/health
```

## 4. Deploy Frontend to S3 + CloudFront

Vite content-hashes all JS/CSS bundles, so **only `/index.html` needs invalidation** (instant and free; avoids `/*` invalidation charges and propagation delays):

```bash
API_URL=$(terraform -chdir=infra output -raw api_url)
FRONTEND_BUCKET=$(terraform -chdir=infra output -raw frontend_bucket_name)
CF_DIST_ID=$(terraform -chdir=infra output -raw cloudfront_distribution_id)

cd frontend
VITE_API_URL="$API_URL" npm run build
aws s3 sync dist/ "s3://$FRONTEND_BUCKET/" --delete
aws cloudfront create-invalidation --distribution-id "$CF_DIST_ID" --paths "/index.html"
```

## 5. Verify RDS Backup & Test Restore Procedure

Run after initial production deploy (and periodically) to confirm backups are usable:

```bash
# 1. Identify latest automated snapshot (or take a manual snapshot)
aws rds describe-db-snapshots \
  --db-instance-identifier engineering-pulse-db \
  --query 'DBSnapshots | sort_by(@, &SnapshotCreateTime)[-1].DBSnapshotIdentifier' \
  --output text

# 2. Restore snapshot to a temporary private instance in the same subnet/SG
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier engineering-pulse-db-restore-test \
  --db-snapshot-identifier <SNAPSHOT_ID> \
  --db-instance-class db.t3.micro \
  --db-subnet-group-name engineering-pulse-db-subnets \
  --no-publicly-accessible

aws rds wait db-instance-available --db-instance-identifier engineering-pulse-db-restore-test

# 3. Connect from EC2 (SSM session) and verify row counts + Alembic revision
# psql "$TEMP_DATABASE_URL" -c "SELECT version_num FROM alembic_version; SELECT count(*) FROM pull_requests;"

# 4. Delete the temporary restore instance immediately after verification
aws rds delete-db-instance \
  --db-instance-identifier engineering-pulse-db-restore-test \
  --skip-final-snapshot
```

