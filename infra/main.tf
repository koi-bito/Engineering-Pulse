module "vpc" {
  source = "./modules/vpc"

  project_name     = var.project_name
  vpc_cidr         = var.vpc_cidr
  ssh_allowed_cidr = var.ssh_allowed_cidr
}

module "s3" {
  source = "./modules/s3"

  raw_events_bucket_name = var.raw_events_bucket_name
  frontend_bucket_name   = var.frontend_bucket_name
}

module "rds" {
  source = "./modules/rds"

  project_name          = var.project_name
  private_subnet_ids    = module.vpc.private_subnet_ids
  rds_security_group_id = module.vpc.rds_security_group_id
  db_instance_class     = var.db_instance_class
  db_name               = var.db_name
  db_username           = var.db_username
}

module "cloudfront" {
  source = "./modules/cloudfront"

  project_name                    = var.project_name
  frontend_bucket_id              = module.s3.frontend_bucket_id
  frontend_bucket_arn             = module.s3.frontend_bucket_arn
  frontend_bucket_regional_domain = module.s3.frontend_bucket_regional_domain_name
}

module "ec2" {
  source = "./modules/ec2"

  project_name           = var.project_name
  aws_region             = var.aws_region
  subnet_id              = module.vpc.public_subnet_ids[0]
  ec2_security_group_id  = module.vpc.ec2_security_group_id
  instance_type          = var.ec2_instance_type
  volume_size_gb         = var.ec2_volume_size_gb
  key_name               = var.ec2_key_name
  repo_url               = var.repo_url
  repo_branch            = var.repo_branch
  raw_events_bucket_name = module.s3.raw_events_bucket_name
  raw_events_bucket_arn  = module.s3.raw_events_bucket_arn
  rds_endpoint           = module.rds.db_endpoint
  rds_resource_id        = module.rds.db_resource_id
  db_name                = var.db_name
  master_user_secret_arn = module.rds.master_user_secret_arn
  app_secrets_arn        = module.rds.app_secrets_arn
  cors_origin            = "https://${module.cloudfront.domain_name}"
}

module "alb" {
  source = "./modules/alb"

  project_name          = var.project_name
  vpc_id                = module.vpc.vpc_id
  public_subnet_ids     = module.vpc.public_subnet_ids
  alb_security_group_id = module.vpc.alb_security_group_id
  ec2_instance_id       = module.ec2.instance_id
  certificate_arn       = var.alb_certificate_arn
}
