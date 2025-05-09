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
  state               = "DISABLED"
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

# Event Rule to trigger sfn through lambda
resource "aws_cloudwatch_event_rule" "trigger_sfn_every_10_minutes" {
  name                = "trigger-sfn-every-10-min"
  schedule_expression = "rate(10 minutes)"
  description         = "Trigger Lambda to invoke Streaming Step Function every 10 mins"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "sfn_10_min_target" {
  rule      = aws_cloudwatch_event_rule.trigger_sfn_every_10_minutes.name
  target_id = "InvokeSfnEvery10Min"
  arn       = var.lambda_trigger_arn

  input = jsonencode({
    stepFunctionArn = var.sfns["stream"]["arn"]
  })
}

resource "aws_lambda_permission" "allow_10_min_event" {
  statement_id  = "Allow10MinEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_trigger_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger_sfn_every_10_minutes.arn
}

resource "aws_cloudwatch_event_rule" "trigger_sfn_daily" {
  name                = "trigger-sfn-daily"
  schedule_expression = "rate(1 day)"
  description         = "Trigger Lambda to invoke Step Function B daily"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "sfn_daily_target" {
  rule      = aws_cloudwatch_event_rule.trigger_sfn_daily.name
  target_id = "InvokeSfnDaily"
  arn       = var.lambda_trigger_arn

  input = jsonencode({
    stepFunctionArn = var.sfns["daily"]["arn"]
  })
}
resource "aws_lambda_permission" "allow_daily_event" {
  statement_id  = "AllowDailyEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_trigger_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger_sfn_daily.arn
}


