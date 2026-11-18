variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "owner" {
  description = "Value for the Owner tag on every resource, so stray instances can be traced"
  type        = string
}

variable "operator_cidrs" {
  description = "CIDRs allowed to reach SSH, Grafana (3000) and Prometheus (9090), e.g. [\"203.0.113.7/32\"]"
  type        = list(string)
}

variable "public_key_path" {
  description = "SSH public key installed on every host; the matching private key is SSH_KEY for bin/*.sh"
  type        = string
  default     = "~/.ssh/mqtt-benchmark.pub"
}

variable "arch" {
  description = "CPU architecture of the instance types below: amd64 or arm64"
  type        = string
  default     = "amd64"
}

# EMQ ran EMQX and Kafka on Alibaba Cloud c6.8xlarge (32 vCPU, 64 GB, Intel). c6i.8xlarge is the AWS equivalent.
variable "broker_instance_type" {
  type    = string
  default = "c6i.8xlarge"
}

variable "kafka_instance_type" {
  type    = string
  default = "c6i.8xlarge"
}

variable "kafka_data_gb" {
  description = "Kafka data volume. 100K msg/s x 1 KB is about 360 GB per hour at replication factor 1."
  type        = number
  default     = 1000
}

# EMQ doesn't say how many load generators it used. One client IP can open about 64K connections to one
# broker address and port, so 100K connections need at least 2 hosts (50K each). If client CPU goes above
# about 70% during a run (Prometheus, role="client"), add hosts so the load generators don't skew the results.
variable "client_count" {
  type    = number
  default = 2
}

variable "client_instance_type" {
  type    = string
  default = "c6i.2xlarge"
}

variable "monitor_instance_type" {
  type    = string
  default = "t3.medium"
}
