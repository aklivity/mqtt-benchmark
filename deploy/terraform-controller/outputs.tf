output "controller_id" {
  description = "CONTROLLER_ID for bin/aws/*"
  value       = aws_instance.controller.id
}

output "bucket_name" {
  description = "BUCKET for bin/aws/*"
  value       = aws_s3_bucket.exchange.bucket
}
