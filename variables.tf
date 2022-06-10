variable "workspace_to_environment_map" {
  type = map(string)
  default = {
    prod = "prod"
    stg  = "stg"
  }
}

variable "environment_to_gcp_project_map" {
  type = map(string)
  default = {
    prod = "nylas-datastreams-prod"
    stg  = "plucky-cascade-331119"
  }
}

variable "environment_to_gcp_projectid_map" {
  type = map(string)
  default = {
    prod = "nylas-datastreams-prod"
    stg  = "plucky-cascade-331119"
  }
}

variable "environment_to_dataproc_cluster_map" {
  type = map(string)
  default = {
    prod = "nylas-data-streams-prd-pipeline-2"
    stg  = "nylas-data-streams-stg-pipeline-1"
  }
}

variable "dataproc_cluster_region" {
  type    = string
  default = "us-west1"
}

variable "environment_to_transformation_topic_input_subscription_map" {
  type = map(string)
  default = {
    "stg"  = "projects/plucky-cascade-331119/subscriptions/staging-transformationtopic-with-mock-data-sub"
    "prod" = "value" #-----------> enter the value here
  }

}
