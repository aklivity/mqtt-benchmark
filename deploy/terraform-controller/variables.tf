variable "region" {
  description = "AWS region of the benchmark VPC"
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix of every named resource (VPC, security group, placement group, key pair, instances), so several deployments can share an AWS account and region"
  type        = string
  default     = "mqtt-benchmark"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,39}$", var.name_prefix))
    error_message = "name_prefix must start with a lowercase letter and contain only lowercase letters, digits and hyphens (at most 40 characters)."
  }
}

variable "owner" {
  description = "Value for the Owner tag on every resource, so stray resources can be traced"
  type        = string
}

variable "subnet_id" {
  description = "Benchmark subnet: the `network` output of deploy/terraform"
  type        = string
}

variable "security_group_id" {
  description = "Benchmark security group (allows all traffic between its members): the `network` output of deploy/terraform"
  type        = string
}

variable "ssh_private_key_file" {
  description = "Private key matching public_key_path of deploy/terraform. It is copied onto the controller, so use a key made for this benchmark."
  type        = string
  default     = "~/.ssh/mqtt-benchmark"
}

variable "max_lifetime_minutes" {
  description = "The controller shuts itself down, and so terminates, this many minutes after it boots"
  type        = number
  default     = 300
  validation {
    condition     = var.max_lifetime_minutes >= 1 && floor(var.max_lifetime_minutes) == var.max_lifetime_minutes
    error_message = "max_lifetime_minutes must be a whole number of minutes, at least 1."
  }
}

variable "bucket_name" {
  description = "Name of the S3 bucket for the source bundle, the Zilla image, run output and results (globally unique)"
  type        = string
}
