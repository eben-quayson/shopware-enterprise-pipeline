resource "aws_cloudwatch_log_group" "crm_firehose_log_group" {
  name = "/aws/kinesisfirehose/crm-firehose-stream"
}

resource "aws_cloudwatch_log_stream" "crm_firehose_log_stream" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.crm_firehose_log_group.name
}

resource "aws_cloudwatch_log_group" "wtl_firehose_log_group" {
  name = "/aws/kinesisfirehose/wtl-firehose-stream"
}

resource "aws_cloudwatch_log_stream" "wtl_firehose_log_stream" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.wtl_firehose_log_group.name
}
