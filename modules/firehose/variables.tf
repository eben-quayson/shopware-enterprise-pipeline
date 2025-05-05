variable "crm_firehose_stream_name" {
  description = "The name of the Kinesis Firehose stream"
  type        = string
}

variable "wtl_firehose_stream_name" {
  description = "The name of the Kinesis Firehose stream"
  type        = string
}

variable "lakehouse_bucket_name" {
  description = "The name of the S3 bucket for the lakehouse"
  type        = string
}
