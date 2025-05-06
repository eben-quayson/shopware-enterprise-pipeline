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

# EventBridge Rule to trigger Lambda function every minute
resource "aws_cloudwatch_event_rule" "every_minute" {
  name                = "lambda-schedule"
  description         = "Trigger all producer Lambdas every 1 minute"
  schedule_expression = "rate(1 minute)"
  state               = "DISABLED" # Set to DISABLED to avoid
}

resource "aws_cloudwatch_event_target" "trigger_lambda" {
  for_each = var.lambda_producers

  rule      = aws_cloudwatch_event_rule.every_minute.name
  target_id = each.key
  arn       = each.value["arn"]
}

resource "aws_lambda_permission" "allow_eventbridge" {
  for_each = var.lambda_producers

  statement_id  = "AllowExecutionFromEventBridge-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = each.value["function_name"]
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_minute.arn
}


