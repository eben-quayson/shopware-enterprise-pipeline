resource "aws_iam_role" "glue_service_role" {
  name = "glue_service_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "glue-permissions"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# Glue Jobs
resource "aws_glue_job" "mov_pos_glue_job" {
  name     = "move_pos_to_bronze"
  role_arn = aws_iam_role.glue_service_role.arn
  command {
    # python shell
    name            = "pythonshell"
    script_location = "s3://${var.shopware_glue_bucket_name}/scripts/move_pos_to_bronze.py"
    python_version  = "3.9"
  }
  max_capacity = 0.0625

  default_arguments = {
    "--SOURCE_BUCKET"                    = var.ingestion_bucket_name
    "--SOURCE_PREFIX"                    = "pos/"
    "--DESTINATION_BUCKET"               = var.lakehouse_bucket_name
    "--DESTINATION_PREFIX"               = "bronze/pos/"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.shopware_glue_bucket_name}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
  }
}

resource "aws_glue_job" "move_inventory_glue_job" {
  name     = "move_inventory_to_bronze"
  role_arn = aws_iam_role.glue_service_role.arn
  command {
    # python shell
    name            = "pythonshell"
    script_location = "s3://${var.shopware_glue_bucket_name}/scripts/move_inventory_to_bronze.py"
    python_version  = "3.9"
  }
  max_capacity = 0.0625

  default_arguments = {
    "--SOURCE_BUCKET"                    = var.ingestion_bucket_name
    "--SOURCE_PREFIX"                    = "inventory/"
    "--DESTINATION_BUCKET"               = var.lakehouse_bucket_name
    "--DESTINATION_PREFIX"               = "bronze/inventory/"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.shopware_glue_bucket_name}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
  }

}

resource "aws_glue_catalog_database" "my_catalog_database" {
  name = "shopware"
}

resource "aws_glue_crawler" "bronze_crawler" {
  database_name = aws_glue_catalog_database.my_catalog_database.name
  name          = "crawl_bronze"
  role          = aws_iam_role.glue_service_role.arn
  table_prefix  = "bronze_"
  s3_target {
    path = "s3://${var.lakehouse_bucket_name}/bronze"
  }
}
