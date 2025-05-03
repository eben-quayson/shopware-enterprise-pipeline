provider "aws" {
  region = var.region
  default_tags {
    tags = {
      "group"   = "group-1"
      "project" = "project-8"
    }
  }
}
