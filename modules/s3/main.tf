resource "aws_s3_bucket" "lakehouse_bucket" {
  bucket = var.lakehouse_bucket_name
}


resource "aws_s3_bucket_policy" "lakehouse_bucket_policy" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = "${aws_s3_bucket.lakehouse_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_s3_bucket" "staging_bucket" {
  bucket = "external-staging-bucket-shopware"
}
