# Remote Terraform state (pass -backend-config flags or a backend.hcl at init time).
# See RUNBOOK.md for S3 state bucket + DynamoDB lock table bootstrap commands.
terraform {
  backend "s3" {
    key     = "engineering-pulse/terraform.tfstate"
    encrypt = true
  }
}

