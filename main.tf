module "s3" {
  source = "./modules/s3"

  lakehouse_bucket_name = var.lakehouse_bucket_name
}

module "lambda" {
  source = "./modules/lambda"

  crm_firehose_stream_name = var.crm_firehose_stream_name
  wtl_firehose_stream_name = var.wtl_firehose_stream_name
}

module "iam" {
  source = "./modules/iam"
}

module "firehose" {
  source = "./modules/firehose"

  crm_firehose_stream_name = var.crm_firehose_stream_name
  wtl_firehose_stream_name = var.wtl_firehose_stream_name
  lakehouse_bucket_name    = var.lakehouse_bucket_name
}

module "cloudwatch" {
  source = "./modules/cloudwatch"
}
