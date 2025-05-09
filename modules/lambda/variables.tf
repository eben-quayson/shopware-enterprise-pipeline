variable "producers" {
  type = map(object({
    firehose_stream_name = string
    external_api_url     = string
  }))

  default = {
    customer_interactions = {
      firehose_stream_name = "crm-firehose-stream"
      external_api_url     = "http://18.203.232.58:8000/api/customer-interaction/"
    }

    web_traffic_logs = {
      firehose_stream_name = "web-traffic-firehose-stream"
      external_api_url     = "http://18.203.232.58:8000/api/web-traffic/"
    }
  }
}


variable "crm_firehose_stream_name" {
  type        = string
  description = "The name of the Kinesis Firehose stream for CRM data."
}

variable "wtl_firehose_stream_name" {
  type        = string
  description = "The name of the Kinesis Firehose stream for web traffic logs."
}


variable "sfns" {
  type = map(object({
    arn = string
  }))
}
