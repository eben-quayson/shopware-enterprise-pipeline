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

variable "glue_jobs" {
  description = "Map of Glue job configurations"
  type = map(object({
    iceberg_table = string
    silver_key    = string
    table_name    = string
    script_name   = string
  }))
  default = {
    inventory = {
      iceberg_table = "glue_catalog.default.inventory_silver"
      silver_key    = "silver/inventory/"
      table_name    = "bronze_inventory"
      script_name   = "transform_inventory_to_silver.py"
    },
    wtl = {
      iceberg_table = "glue_catalog.default.wtl_silver"
      silver_key    = "silver/wtl/"
      table_name    = "bronze_wtl"
      script_name   = "transform_wtl_to_silver.py"
    },
    pos = {
      iceberg_table = "glue_catalog.default.pos_silver"
      silver_key    = "silver/pos/"
      table_name    = "bronze_pos"
      script_name   = "transform_pos_to_silver.py"
    },
    crm = {
      iceberg_table = "glue_catalog.default.customer_interaction_silver"
      silver_key    = "silver/customer_interaction/"
      table_name    = "bronze_crm"
      script_name   = "transform_crm_to_silver.py"
    }
  }
}


