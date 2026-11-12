resource "aws_db_subnet_group" "this" {
  name       = "${var.project_name}-db-subnets"
  subnet_ids = var.private_subnet_ids

  tags = {
    Name = "${var.project_name}-db-subnets"
  }
}

resource "aws_db_instance" "this" {
  identifier                          = "${var.project_name}-db"
  engine                              = "postgres"
  engine_version                      = "16"
  instance_class                      = var.db_instance_class
  allocated_storage                   = 20
  storage_type                        = "gp3"
  storage_encrypted                   = true
  db_name                             = var.db_name
  username                            = var.db_username
  manage_master_user_password         = true
  iam_database_authentication_enabled = true
  db_subnet_group_name                = aws_db_subnet_group.this.name
  vpc_security_group_ids              = [var.rds_security_group_id]
  publicly_accessible                 = false
  multi_az                            = false
  backup_retention_period             = 7
  deletion_protection                 = false
  skip_final_snapshot                 = false
  final_snapshot_identifier           = "${var.project_name}-final-snapshot"
}

# Populated out-of-band via `aws secretsmanager put-secret-value` (never stored in Terraform state).
resource "aws_secretsmanager_secret" "app" {
  name        = "${var.project_name}/app-secrets"
  description = "Runtime webhook and Grafana secrets for Engineering Pulse"
}

