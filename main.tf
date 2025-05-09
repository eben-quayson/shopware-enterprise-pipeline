module "s3" {
  source = "./modules/s3"

  lakehouse_bucket_name     = var.lakehouse_bucket_name
  shopware_glue_bucket_name = var.shopware_glue_bucket_name
  ingestion_bucket_name     = var.ingestion_bucket_name
}

module "lambda" {
  source = "./modules/lambda"

  crm_firehose_stream_name = var.crm_firehose_stream_name
  wtl_firehose_stream_name = var.wtl_firehose_stream_name
  sfns                     = var.sfns
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

  lambda_producers    = module.lambda.lambda_producers
  sfns                = var.sfns
  lambda_trigger_name = module.lambda.sfn_trigger_lambda_name
  lambda_trigger_arn  = module.lambda.sfn_trigger_lambda_arn
}

module "glue" {
  source = "./modules/glue"

  lakehouse_bucket_name     = var.lakehouse_bucket_name
  shopware_glue_bucket_name = var.shopware_glue_bucket_name
  ingestion_bucket_name     = var.ingestion_bucket_name
}
