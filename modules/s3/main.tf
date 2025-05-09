resource "aws_s3_bucket" "lakehouse_bucket" {
  bucket = var.lakehouse_bucket_name
}



resource "aws_s3_bucket" "shopware_glue_bucket" {
  bucket = var.shopware_glue_bucket_name
}

resource "aws_s3_bucket_policy" "lakehouse_bucket_policy" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = "${aws_s3_bucket.lakehouse_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_s3_bucket" "staging_bucket" {
  bucket = var.ingestion_bucket_name
}

# resource "aws_s3_object" "move_pos_script" {
#   bucket = aws_s3_bucket.shopware_glue_bucket.id
#   key    = "scripts/move_pos_to_bronze.py"
#   source = "${path.module}/scripts/move_pos_to_bronze.py"
#   etag   = filemd5("${path.module}/scripts/move_pos_to_bronze.py")
# }

# resource "aws_s3_object" "move_inventory_script" {
#   bucket = aws_s3_bucket.shopware_glue_bucket.id
#   key    = "scripts/move_inventory_to_bronze.py"
#   source = "${path.module}/scripts/move_inventory_to_bronze.py"
#   etag   = filemd5("${path.module}/scripts/move_inventory_to_bronze.py")
# }

# resource "aws_s3_object" "transform_crm_to_silver" {
#   bucket = aws_s3_bucket.shopware_glue_bucket.id
#   key    = "scripts/transform_crm_to_silver.py"
#   source = "${path.module}/scripts/transform_crm_to_silver.py"
#   etag   = filemd5("${path.module}/scripts/transform_crm_to_silver.py")
# }

# resource "aws_s3_object" "transform_pos_to_silver" {
#   bucket = aws_s3_bucket.shopware_glue_bucket.id
#   key    = "scripts/transform_pos_to_silver.py"
#   source = "${path.module}/scripts/transform_pos_to_silver.py"
#   etag   = filemd5("${path.module}/scripts/transform_pos_to_silver.py")
# }

# resource "aws_s3_object" "transform_inventory_to_silver" {
#   bucket = aws_s3_bucket.shopware_glue_bucket.id
#   key    = "scripts/transform_inventory_to_silver.py"
#   source = "${path.module}/scripts/transform_inventory_to_silver.py"
#   etag   = filemd5("${path.module}/scripts/transform_inventory_to_silver.py")
# }

# resource "aws_s3_object" "transform_wtl_to_silver" {
#   bucket = aws_s3_bucket.shopware_glue_bucket.id
#   key    = "scripts/transform_wtl_to_silver.py"
#   source = "${path.module}/scripts/transform_wtl_to_silver.py"
#   etag   = filemd5("${path.module}/scripts/transform_wtl_to_silver.py")
# }

resource "aws_s3_object" "glue_script" {
  for_each = var.script_files
  bucket   = aws_s3_bucket.shopware_glue_bucket.id
  key      = "scripts/${each.value}"
  source   = "${path.module}/scripts/${each.value}"
  etag     = filemd5("${path.module}/scripts/${each.value}")
}
