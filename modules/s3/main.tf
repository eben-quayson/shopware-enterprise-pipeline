resource "aws_s3_bucket" "lakehouse_bucket" {
  bucket = var.lakehouse_bucket_name
}
