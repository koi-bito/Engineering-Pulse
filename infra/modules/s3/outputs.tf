output "raw_events_bucket_name" {
  value = aws_s3_bucket.raw_events.bucket
}

output "raw_events_bucket_arn" {
  value = aws_s3_bucket.raw_events.arn
}

output "frontend_bucket_id" {
  value = aws_s3_bucket.frontend.id
}

output "frontend_bucket_name" {
  value = aws_s3_bucket.frontend.bucket
}

output "frontend_bucket_arn" {
  value = aws_s3_bucket.frontend.arn
}

output "frontend_bucket_regional_domain_name" {
  value = aws_s3_bucket.frontend.bucket_regional_domain_name
}

