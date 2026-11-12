variable "project_name" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "ec2_security_group_id" {
  type = string
}

variable "instance_type" {
  type = string
}

variable "volume_size_gb" {
  type = number
}

variable "key_name" {
  type    = string
  default = null
}

variable "repo_url" {
  type = string
}

variable "repo_branch" {
  type = string
}

variable "raw_events_bucket_name" {
  type = string
}

variable "raw_events_bucket_arn" {
  type = string
}

variable "rds_endpoint" {
  type = string
}

variable "rds_resource_id" {
  type = string
}

variable "db_name" {
  type = string
}

variable "master_user_secret_arn" {
  type = string
}

variable "app_secrets_arn" {
  type = string
}

variable "cors_origin" {
  type = string
}

