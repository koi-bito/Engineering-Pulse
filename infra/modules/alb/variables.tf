variable "project_name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "alb_security_group_id" {
  type = string
}

variable "ec2_instance_id" {
  type = string
}

variable "certificate_arn" {
  type    = string
  default = null
}

