output "db_endpoint" {
  value = aws_db_instance.this.endpoint
}

output "db_resource_id" {
  value = aws_db_instance.this.resource_id
}

output "master_user_secret_arn" {
  value = aws_db_instance.this.master_user_secret[0].secret_arn
}

output "app_secrets_arn" {
  value = aws_secretsmanager_secret.app.arn
}

