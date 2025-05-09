data "archive_file" "lambda_producers" {
  for_each    = var.producers
  type        = "zip"
  source_file = "${path.module}/scripts/producers/${each.key}.py"
  output_path = "${path.module}/scripts/producers/${each.key}.zip"
}

data "archive_file" "sfn_trigger" {
  type        = "zip"
  source_file = "${path.module}/scripts/sfn_trigger.py"
  output_path = "${path.module}/scripts/sfn_trigger.zip"
}

# --- I am policy document allowing lambda to assume role ---
data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    effect  = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_execution_policy" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "firehose:PutRecord"
    ]
    resources = ["arn:aws:firehose:*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "sfn:StartExecution"
    ]
    resources = ["arn:aws:sfn:*"]
  }
}

resource "aws_iam_role" "lambda_execution_role" {
  name               = "lambda_shared_execution_role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
}

resource "aws_iam_role_policy" "lambda_logs_and_firehose" {
  name   = "lambda-logs-and-firehose"
  role   = aws_iam_role.lambda_execution_role.id
  policy = data.aws_iam_policy_document.lambda_execution_policy.json
}


resource "aws_iam_role_policy_attachment" "lambda_execution_policy" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonKinesisFirehoseFullAccess"
}



# --- Lambda functions ---
resource "aws_lambda_function" "producers" {
  for_each         = data.archive_file.lambda_producers
  function_name    = each.key
  role             = aws_iam_role.lambda_execution_role.arn
  handler          = "${each.key}.lambda_handler"
  runtime          = "python3.8"
  source_code_hash = each.value.output_base64sha256
  filename         = each.value.output_path
  timeout          = 60

  environment {
    variables = {
      FIREHOSE_STREAM_NAME = var.producers[each.key].firehose_stream_name
      EXTERNAL_API_URL     = var.producers[each.key].external_api_url
    }
  }
}

resource "aws_lambda_function" "sfn_trigger" {
  function_name    = "sfn_trigger"
  role             = aws_iam_role.lambda_execution_role.arn
  handler          = "sfn_trigger.lambda_handler"
  runtime          = "python3.8"
  source_code_hash = data.archive_file.sfn_trigger.output_base64sha256
  filename         = data.archive_file.sfn_trigger.output_path
  timeout          = 60
}

