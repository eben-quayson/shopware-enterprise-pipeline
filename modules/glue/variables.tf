variable "lakehouse_bucket_name" {
  description = "The source of the crawler"
  type        = string
}

variable "shopware_glue_bucket_name" {
  type = string
}

variable "ingestion_bucket_name" {
  type = string
}
