output "broker" {
  value = { public_ip = aws_instance.broker.public_ip, private_ip = aws_instance.broker.private_ip }
}

output "kafka" {
  value = { public_ip = aws_instance.kafka.public_ip, private_ip = aws_instance.kafka.private_ip }
}

output "clients" {
  value = [for c in aws_instance.client : { public_ip = c.public_ip, private_ip = c.private_ip }]
}

output "grafana_url" {
  value = "http://${aws_instance.monitor.public_ip}:3000"
}

output "prometheus_url" {
  value = "http://${aws_instance.monitor.public_ip}:9090"
}

output "ami_id" {
  description = "Ubuntu 22.04 image the instances were created from (record it with the results)"
  value       = nonsensitive(data.aws_ssm_parameter.ubuntu_ami.value)
}

output "network" {
  description = "Where the controller (deploy/terraform-controller) is placed: terraform.tfvars values for its root"
  value = {
    vpc_id            = aws_vpc.bench.id
    subnet_id         = aws_subnet.bench.id
    security_group_id = aws_security_group.bench.id
  }
}
