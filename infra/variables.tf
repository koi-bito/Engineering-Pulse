variable "project_name" {
  description = "Resource name prefix"
  type        = string
  default     = "engineering-pulse"
}

variable "environment" {
  description = "Deployment environment label"
  type        = string
  default     = "production"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "ec2_instance_type" {
  description = "EC2 instance size running Docker Compose stack"
  type        = string
  default     = "t3.large"
}

variable "ec2_volume_size_gb" {
  description = "Root EBS volume size in GB (>= 50GB for Kafka logs and Prometheus TSDB)"
  type        = number
  default     = 50
}

variable "ec2_key_name" {
  description = "Optional EC2 SSH key pair name (leave null to use AWS SSM Session Manager)"
  type        = string
  default     = null
}

variable "ssh_allowed_cidr" {
  description = "Optional CIDR allowed SSH (port 22) access to EC2 (null disables port 22)"
  type        = string
  default     = null
}

variable "repo_url" {
  description = "Git clone URL pulled by EC2 user data on bootstrap"
  type        = string
  default     = "https://github.com/koi-bito/Engineering-Pulse.git"
}

variable "repo_branch" {
  description = "Git branch checked out on EC2 bootstrap"
  type        = string
  default     = "main"
}

variable "db_instance_class" {
  description = "RDS PostgreSQL instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_name" {
  description = "Initial PostgreSQL database name"
  type        = string
  default     = "engineering_pulse"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "postgres"
}

variable "alb_certificate_arn" {
  description = "Optional ACM certificate ARN for HTTPS listener on ALB (null serves HTTP:80)"
  type        = string
  default     = null
}

variable "raw_events_bucket_name" {
  description = "Globally unique S3 bucket name for raw webhook payload archive"
  type        = string
}

variable "frontend_bucket_name" {
  description = "Globally unique S3 bucket name for static React dashboard assets"
  type        = string
}

