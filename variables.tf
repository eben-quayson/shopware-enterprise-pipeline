variable "region" {
  type        = string
  description = "The AWS region to deploy resources in."
}

variable "lakehouse_bucket_name" {
  type = string
}


variable "shopware_glue_bucket_name" {
  type = string
}

variable "ingestion_bucket_name" {
  type = string
}

variable "crm_firehose_stream_name" {
  type        = string
  description = "The name of the Kinesis Firehose stream for CRM data."
  default     = "crm-firehose-stream"
}

variable "wtl_firehose_stream_name" {
  type        = string
  description = "The name of the Kinesis Firehose stream for web traffic logs."
  default     = "web-traffic-firehose-stream"
}

variable "sfns" {
  type = map(object({
    arn = string
  }))
}

