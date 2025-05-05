data "archive_file" "lambda_producers" {
  for_each    = var.producers
  type        = "zip"
  source_file = "${path.module}/scripts/producers/${each.key}.py"
  output_path = "${path.module}/scripts/producers/${each.key}.zip"
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
}

resource "aws_iam_role" "lambda_execution_role" {
  name               = "lambda_shared_execution_role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
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


