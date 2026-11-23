terraform {
  required_version = ">= 1.5"
  required_providers {
    aws   = { source = "hashicorp/aws", version = "~> 5.0" }
    local = { source = "hashicorp/local", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "mqtt-benchmark", Owner = var.owner }
  }
}

data "aws_availability_zones" "available" { state = "available" }

# Ubuntu 22.04 LTS for the CPU architecture of the instance types: the current official image ID, as Canonical
# publishes it in AWS Systems Manager. No owner ID or name pattern to keep up to date.
data "aws_ssm_parameter" "ubuntu_ami" {
  name = "/aws/service/canonical/ubuntu/server/22.04/stable/current/${var.arch == "arm64" ? "arm64" : "amd64"}/hvm/ebs-gp2/ami-id"
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "mqtt-benchmark" }
}

resource "aws_internet_gateway" "bench" { vpc_id = aws_vpc.bench.id }

# One subnet in one availability zone, as in the EMQ test (all hosts in one VPC subnet)
resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.0.0.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench" {
  vpc_id = aws_vpc.bench.id
  name   = "mqtt-benchmark"

  ingress {
    description = "all traffic between benchmark hosts"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  ingress {
    description = "SSH from the operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.operator_cidrs
  }
  ingress {
    description = "Grafana and Prometheus from the operator"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = var.operator_cidrs
  }
  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = var.operator_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_placement_group" "bench" {
  name     = "mqtt-benchmark"
  strategy = "cluster"
}

resource "aws_key_pair" "bench" {
  key_name   = "mqtt-benchmark"
  public_key = file(pathexpand(var.public_key_path))
}

locals {
  common = {
    ami                    = nonsensitive(data.aws_ssm_parameter.ubuntu_ami.value)
    subnet_id              = aws_subnet.bench.id
    vpc_security_group_ids = [aws_security_group.bench.id]
    key_name               = aws_key_pair.bench.key_name
  }
  # Hard stop for forgotten environments: a shutdown from inside the instance terminates it (see
  # instance_initiated_shutdown_behavior on every instance), whatever happens to the operator's session.
  shutdown_guard = <<-EOT
    #!/bin/bash
    shutdown -h +${var.max_lifetime_minutes}
  EOT
}

resource "aws_instance" "broker" {
  ami                    = local.common.ami
  instance_type          = var.broker_instance_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = aws_placement_group.bench.id

  instance_initiated_shutdown_behavior = "terminate"
  user_data                            = local.shutdown_guard

  # The broker host (Zilla or EMQX). Zilla caches the Kafka topics it bootstraps (including mqtt-devices) under
  # /var/run/zilla: at 100K msg/s of 1 KB that's about 360 GB per hour, so give it room and throughput (topic
  # retention keeps it bounded). EMQX keeps nothing on disk in this setup.
  root_block_device {
    volume_size = 500
    volume_type = "gp3"
    iops        = 16000
    throughput  = 1000
  }
  tags = { Name = "mqtt-benchmark-broker" }
  lifecycle {
    ignore_changes = [ami]
  }
}

resource "aws_instance" "kafka" {
  ami                    = local.common.ami
  instance_type          = var.kafka_instance_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = aws_placement_group.bench.id

  instance_initiated_shutdown_behavior = "terminate"
  user_data                            = local.shutdown_guard

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }
  # Kafka log directory, mounted at /data by Ansible
  ebs_block_device {
    device_name = "/dev/sdf"
    volume_size = var.kafka_data_gb
    volume_type = "gp3"
    iops        = 16000
    throughput  = 1000
  }
  tags = { Name = "mqtt-benchmark-kafka" }
  lifecycle {
    ignore_changes = [ami]
  }
}

resource "aws_instance" "client" {
  count                  = var.client_count
  ami                    = local.common.ami
  instance_type          = var.client_instance_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = aws_placement_group.bench.id

  instance_initiated_shutdown_behavior = "terminate"
  user_data                            = local.shutdown_guard

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
  tags = { Name = "mqtt-benchmark-client-${count.index}" }
  lifecycle {
    ignore_changes = [ami]
  }
}

resource "aws_instance" "monitor" {
  ami                    = local.common.ami
  instance_type          = var.monitor_instance_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name

  instance_initiated_shutdown_behavior = "terminate"
  user_data                            = local.shutdown_guard

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }
  tags = { Name = "mqtt-benchmark-monitor" }
  lifecycle {
    ignore_changes = [ami]
  }
}

# Ansible inventory used by deploy/ansible and bin/*.sh, reached over the instances' public addresses
resource "local_file" "inventory" {
  filename = "${path.module}/../inventory.ini"
  content = templatefile("${path.module}/inventory.tftpl", {
    address = "public_ip"
    broker  = aws_instance.broker
    kafka   = aws_instance.kafka
    clients = aws_instance.client
    monitor = aws_instance.monitor
  })
}

# The same inventory with private addresses, for the controller inside the VPC (docs/aws-no-ssh.md)
resource "local_file" "inventory_private" {
  filename = "${path.module}/../inventory-private.ini"
  content = templatefile("${path.module}/inventory.tftpl", {
    address = "private_ip"
    broker  = aws_instance.broker
    kafka   = aws_instance.kafka
    clients = aws_instance.client
    monitor = aws_instance.monitor
  })
}
