variable "lambda_producers" {
  description = "Map of Lambda producer functions"
  type        = map(any)
}

variable "sfns" {
  type = map(object({
    arn = string
  }))
}

variable "lambda_trigger_name" {
  description = "Name of the Lambda function that triggers the Step Function"
  type        = string
}

variable "lambda_trigger_arn" {
  description = "ARN of the Lambda function that triggers the Step Function"
  type        = string
}
