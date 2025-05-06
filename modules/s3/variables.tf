variable "lakehouse_bucket_name" {
  type        = string
  description = "The name of the S3 bucket to be created for the lakehouse."
}

variable "shopware_glue_bucket_name" {
  type        = string
  description = "The name of the S3 bucket to be created for Glue jobs."
}

variable "ingestion_bucket_name" {
  type        = string
  description = "The name of the S3 bucket to be created for external staging."
}
