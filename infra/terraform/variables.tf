###############################################
# Variables
# project_id is provided via terraform.tfvars
# region and zone have defaults but can be changed
###############################################
variable "project_id" {
  type = string
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-b"
}

variable "allowed_ingress_cidr_blocks" {
  description = "CIDR blocks allowed to access exposed demo services. Use your own IP range, not 0.0.0.0/0, for portfolio deployments."
  type        = list(string)
}
