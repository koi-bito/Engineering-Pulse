output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "alb_dns_name" {
  description = "Public DNS name of the Application Load Balancer"
  value       = module.alb.alb_dns_name
}

output "api_url" {
  description = "Public API base URL"
  value       = var.alb_certificate_arn != null ? "https://${module.alb.alb_dns_name}" : "http://${module.alb.alb_dns_name}"
}

output "cloudfront_domain_name" {
  description = "CloudFront distribution domain serving the React dashboard"
  value       = module.cloudfront.domain_name
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID for cache invalidation"
  value       = module.cloudfront.distribution_id
}

output "frontend_bucket_name" {
  description = "S3 bucket name for frontend static builds"
  value       = module.s3.frontend_bucket_name
}

output "raw_events_bucket_name" {
  description = "S3 bucket name for raw webhook archive"
  value       = module.s3.raw_events_bucket_name
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = module.rds.db_endpoint
}

output "rds_secret_arn" {
  description = "AWS Secrets Manager ARN holding the RDS master credentials"
  value       = module.rds.master_user_secret_arn
}

output "app_secrets_arn" {
  description = "AWS Secrets Manager ARN holding webhook and Grafana runtime secrets"
  value       = module.rds.app_secrets_arn
}

output "ec2_instance_id" {
  description = "EC2 instance ID for SSM Session Manager access"
  value       = module.ec2.instance_id
}

