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

resource "aws_glue_job" "transform_to_silver" {
  for_each = var.silver_glue_jobs

  name     = "transform_${each.key}_to_silver"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    script_location = "s3://${var.shopware_glue_bucket_name}/scripts/${each.value.script_name}"
    name            = "glueetl" # Use glueetl for Python-based ETL jobs
    python_version  = "3"
  }

  default_arguments = {
    "--database_name"       = "shopware"
    "--silver_path"         = "s3://${var.lakehouse_bucket_name}/${each.value.silver_key}"
    "--table_name"          = each.value.table_name
    "--enable-metrics"      = "true" # Optional: Enable CloudWatch metrics
    "--job-language"        = "python"
    "--conf"                = var.glue_parameter_conf
    "--datalake-formats"    = "delta"
    "--enable-job-insights" = "true"
  }

  max_retries       = 0
  timeout           = 5      # Timeout in minutes
  glue_version      = "5.0"  # Use the latest Glue version that supports Iceberg
  worker_type       = "G.1X" # Adjust based on your workload
  number_of_workers = 2      # Adjust based on your workload
}

resource "aws_glue_job" "compute_to_gold" {
  for_each = var.gold_glue_jobs
  name     = each.key
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    script_location = "s3://${var.shopware_glue_bucket_name}/scripts/${each.value.script_name}"
    name            = "glueetl" # Use glueetl for Python-based ETL jobs
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"      = "true" # Optional: Enable CloudWatch metrics
    "--job-language"        = "python"
    "--BUCKET_NAME"         = var.lakehouse_bucket_name
    "--conf"                = var.glue_parameter_conf
    "--datalake-formats"    = "delta"
    "--enable-job-insights" = "true"
  }

  max_retries       = 0
  timeout           = 5      # Timeout in minutes
  glue_version      = "5.0"  # Use the latest Glue version that supports Iceberg
  worker_type       = "G.1X" # Adjust based on your workload
  number_of_workers = 2      # Adjust based on your workload
}


# Triggers
resource "aws_glue_trigger" "pos_glue_trigger" {
  name     = "trigger_pos_glue_job"
  type     = "SCHEDULED"
  schedule = "cron(0 2 * * ? *)" # Every day at 2 AM UTC

  actions {
    job_name = aws_glue_job.mov_pos_glue_job.name
  }

  enabled = false
}

resource "aws_glue_trigger" "inventory_glue_trigger" {
  name     = "trigger_inventory_glue_job"
  type     = "SCHEDULED"
  schedule = "cron(5 * * * ? *)" # 5 minutes past every hour

  actions {
    job_name = aws_glue_job.move_inventory_glue_job.name
  }

  enabled = false
}

# Glue Database and Crawler
resource "aws_glue_catalog_database" "my_catalog_database" {
  name = "shopware"
}

resource "aws_glue_crawler" "bronze_crawler" {
  database_name = aws_glue_catalog_database.my_catalog_database.name
  name          = "crawl_bronze"
  role          = aws_iam_role.glue_service_role.arn
  table_prefix  = "bronze_"

  classifiers = [aws_glue_classifier.json_classifier.name]

  s3_target {
    path        = "s3://${var.lakehouse_bucket_name}/bronze"
    exclusions  = []
    sample_size = 10
  }

  # Simplified configuration with only supported keys
  configuration = jsonencode({
    Version = 1
    # Optionally, add supported configuration options, e.g., CrawlerOutput
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })
}

resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.my_catalog_database.name
  name          = "crawl_silver"
  role          = aws_iam_role.glue_service_role.arn
  table_prefix  = "silver_"

  delta_target {
    delta_tables = [
      "s3://${var.lakehouse_bucket_name}/silver/customer_interaction/",
      "s3://${var.lakehouse_bucket_name}/silver/inventory/",
      "s3://${var.lakehouse_bucket_name}/silver/pos/",
      "s3://${var.lakehouse_bucket_name}/silver/wtl/",
    ]
    write_manifest = true
  }
}

resource "aws_glue_crawler" "gold_crawler" {
  database_name = aws_glue_catalog_database.my_catalog_database.name
  name          = "crawl_gold"
  role          = aws_iam_role.glue_service_role.arn
  table_prefix  = "gold_"

  s3_target {
    path        = "s3://${var.lakehouse_bucket_name}/gold"
    exclusions  = []
    sample_size = 10
  }

}

resource "aws_glue_classifier" "json_classifier" {
  name = "flatten_json_classifier"

  json_classifier {
    json_path = "$[*]" # This tells the classifier to expect an array of objects
  }
}
