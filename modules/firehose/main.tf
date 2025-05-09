data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}
data "aws_iam_policy_document" "firehose_s3_access" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::${var.lakehouse_bucket_name}",
      "arn:aws:s3:::${var.lakehouse_bucket_name}/*"
    ]
  }
}

resource "aws_iam_role_policy" "firehose_s3_policy" {
  name   = "firehose_s3_access"
  role   = aws_iam_role.firehose_role.id
  policy = data.aws_iam_policy_document.firehose_s3_access.json
}

resource "aws_iam_role" "firehose_role" {
  name               = "firehose_to_s3_role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
}

resource "aws_kinesis_firehose_delivery_stream" "crm_firehose_stream" {
  name        = var.crm_firehose_stream_name
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = "arn:aws:s3:::${var.lakehouse_bucket_name}"

    compression_format = "GZIP"

    prefix              = "bronze/crm/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "bronze/error/crm/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}/"
    buffering_size      = 1
    buffering_interval  = 60
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = "/aws/kinesisfirehose/crm-firehose-stream"
      log_stream_name = "S3Delivery"
    }
  }
}

resource "aws_kinesis_firehose_delivery_stream" "wtl_firehose_stream" {
  name        = var.wtl_firehose_stream_name
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = "arn:aws:s3:::${var.lakehouse_bucket_name}"
    compression_format  = "GZIP"
    prefix              = "bronze/wtl/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "bronze/wtl/error/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}/"
    buffering_size      = 1
    buffering_interval  = 60
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = "/aws/kinesisfirehose/wtl-firehose-stream"
      log_stream_name = "S3Delivery"
    }
  }
}

