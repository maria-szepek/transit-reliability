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
