module "s3" {
  source = "./modules/s3"

  lakehouse_bucket_name = var.lakehouse_bucket_name
}
