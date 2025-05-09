output "lambda_producers" {
  value = aws_lambda_function.producers
}

output "sfn_trigger_lambda_arn" {
  value = aws_lambda_function.sfn_trigger.arn
}

output "sfn_trigger_lambda_name" {
  value = aws_lambda_function.sfn_trigger.function_name
}
