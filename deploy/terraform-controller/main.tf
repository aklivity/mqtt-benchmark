# The controller: one small instance in the benchmark subnet that runs Ansible and bin/*.sh over SSH on private
# addresses, driven from outside through Systems Manager (HTTPS). Applied separately from deploy/terraform because it
# creates IAM resources, which need elevated rights that the benchmark hosts' root does not. See docs/aws-no-ssh.md.
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.name_prefix, Owner = var.owner }
  }
}

# Same image source as deploy/terraform
data "aws_ssm_parameter" "ubuntu_ami" {
  name = "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id"
}

data "aws_subnet" "bench" {
  id = var.subnet_id
}

data "aws_vpc" "bench" {
  id = data.aws_subnet.bench.vpc_id
}

locals {
  # ssh_config patterns match addresses, not CIDRs: keep the octets the VPC CIDR fixes (10.0.0.0/16 -> 10.0.*)
  vpc_octets       = split(".", cidrhost(data.aws_vpc.bench.cidr_block, 0))
  vpc_fixed_octets = max(1, floor(tonumber(split("/", data.aws_vpc.bench.cidr_block)[1]) / 8))
  vpc_host_pattern = "${join(".", slice(local.vpc_octets, 0, local.vpc_fixed_octets))}.*"
}

resource "aws_s3_bucket" "exchange" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "exchange" {
  bucket                  = aws_s3_bucket.exchange.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_iam_role" "controller" {
  name = "${var.name_prefix}-ssm"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.controller.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "exchange" {
  name = "exchange-bucket"
  role = aws_iam_role.controller.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.exchange.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.exchange.arn
      }
    ]
  })
}

resource "aws_iam_instance_profile" "controller" {
  name = "${var.name_prefix}-ssm"
  role = aws_iam_role.controller.name
}

resource "aws_instance" "controller" {
  ami                    = nonsensitive(data.aws_ssm_parameter.ubuntu_ami.value)
  instance_type          = "t3.medium"
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = aws_iam_instance_profile.controller.name

  instance_initiated_shutdown_behavior = "terminate"
  # Holds the private key, so keep it out of plan output (the provider stores only a hash of user_data in state)
  user_data = sensitive(templatefile("${path.module}/controller-init.sh.tftpl", {
    max_lifetime_minutes = var.max_lifetime_minutes
    region               = var.region
    vpc_host_pattern     = local.vpc_host_pattern
    ssh_private_key_b64  = base64encode(file(pathexpand(var.ssh_private_key_file)))
  }))

  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
  tags = { Name = "${var.name_prefix}-controller" }
  lifecycle {
    ignore_changes = [ami]
  }
}
