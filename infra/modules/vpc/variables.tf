variable "project_name" {
  type = string
}

variable "vpc_cidr" {
  type = string
}

variable "ssh_allowed_cidr" {
  type    = string
  default = null
}

