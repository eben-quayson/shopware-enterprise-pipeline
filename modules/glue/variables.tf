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

variable "glue_parameter_conf" {
  type    = string
  default = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
}

variable "silver_glue_jobs" {
  description = "Map of Glue job configurations"
  type = map(object({
    silver_key  = string
    table_name  = string
    script_name = string
  }))
  default = {
    inventory = {
      silver_key  = "silver/inventory/"
      table_name  = "bronze_inventory"
      script_name = "transform_inventory_to_silver.py"
    },
    wtl = {
      silver_key  = "silver/wtl/"
      table_name  = "bronze_wtl"
      script_name = "transform_wtl_to_silver.py"
    },
    pos = {
      silver_key  = "silver/pos/"
      table_name  = "bronze_pos"
      script_name = "transform_pos_to_silver.py"
    },
    crm = {
      silver_key  = "silver/customer_interaction/"
      table_name  = "bronze_crm"
      script_name = "transform_crm_to_silver.py"
    }
  }
}

variable "gold_glue_jobs" {
  description = "Map of Glue job configurations"
  type = map(object({
    gold_key    = string
    table_name  = string
    script_name = string
  }))
  default = {
    customer_kpis = {
      gold_key    = "gold/customer_kpis/"
      table_name  = "customer_kpis"
      script_name = "compute_customer_kpis.py"
    },
    marketing_kpis = {
      gold_key    = "gold/marketing_kpis/"
      table_name  = "marketing_kpis"
      script_name = "compute_marketing_kpis.py"
    },
    sales_kpis = {
      gold_key    = "gold/sales_kpis/"
      table_name  = "sales_kpis"
      script_name = "compute_sales_kpis.py"
    }
  }

}

