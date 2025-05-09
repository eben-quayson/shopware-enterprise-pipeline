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
variable "script_files" {
  type        = map(string)
  description = "A map of script file names with their respective keys."
  default = {
    compute_customer_kpis         = "compute_customer_kpis.py"
    compute_marketing_kpis        = "compute_marketing_kpis.py"
    compute_sales_kpis            = "compute_sales_kpis.py"
    compute_operations_kpis       = "compute_operations_kpis.py"
    move_inventory_to_bronze      = "move_inventory_to_bronze.py"
    move_pos_to_bronze            = "move_pos_to_bronze.py"
    transform_crm_to_silver       = "transform_crm_to_silver.py"
    transform_inventory_to_silver = "transform_inventory_to_silver.py"
    transform_pos_to_silver       = "transform_pos_to_silver.py"
    transform_wtl_to_silver       = "transform_wtl_to_silver.py"
  }
}
