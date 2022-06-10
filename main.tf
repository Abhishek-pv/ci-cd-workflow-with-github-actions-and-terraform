terraform {
  backend "gcs" {
    bucket = "nylas-data-streams-stg"
    prefix = "terraform/backend/"
  }
}

locals {
  environment                             = lookup(var.workspace_to_environment_map, terraform.workspace, null)
  gcp_project                             = lookup(var.environment_to_gcp_project_map, terraform.workspace, null)
  gcp_project_id                          = lookup(var.environment_to_gcp_projectid_map, terraform.workspace, null)
  dataproc_cluster_name                   = lookup(var.environment_to_dataproc_cluster_map, terraform.workspace, null)
  transformation_topic_input_subscription = lookup(var.environment_to_transformation_topic_input_subscription_map, terraform.workspace, null)

}

resource "google_storage_bucket_object" "load_dataproc_scripts_to_gcs_bucket" {
  for_each = fileset("./dataproc/", "**.py")
  name     = "dataproc/jobs/${each.value}"
  source   = "./dataproc/${each.value}"
  bucket   = "nylas-data-streams-${terraform.workspace}"
}

resource "google_dataflow_flex_template_job" "transformation_topic_pubsub_to_gcs" {
  provider                = google-beta
  name                    = "${terraform.workspace}-transformation-topic-pubsub-to-gcs"
  container_spec_gcs_path = "gs://nylas-data-streams-stg/templates/flex_template/pubsub_to_gcs_metadata"
  project                 = local.gcp_project
  region                  = "us-west1"
  parameters = {
    "input_subscription" = local.transformation_topic_input_subscription
    "output_path"        = "gs://nylas-data-streams-${terraform.workspace}/data/type=transformation/region=us-central-1"
    "window_size"        = 1
    "num_shards"         = 100
    "included_fields"    = "date,grant_id,message_id,step,object,sync_category"
  }
}

resource "google_cloud_scheduler_job" "schedule_transformation_pipeline" {
  name             = "transformation_pipeline_${terraform.workspace}"
  description      = "Schedule transformation pipeline."
  schedule         = "0 * * * *"
  attempt_deadline = "320s"
  region           = "us-west1"
  project          = local.gcp_project_id

  retry_config {
    retry_count = 2
  }

  http_target {
    http_method = "POST"
    uri         = "https://dataproc.googleapis.com/v1/projects/${local.gcp_project_id}/regions/us-west1/jobs:submit/"
    body        = base64encode("{\"job\":{\"placement\":{\"clusterName\":\"${local.dataproc_cluster_name}\"},\"statusHistory\":[],\"pysparkJob\":{\"mainPythonFileUri\":\"gs://nylas-data-streams-${terraform.workspace}/dataproc/jobs/transformation-to-redshift_1.py\",\"properties\":{},\"jarFileUris\":[\"gs://nylas-data-streams-${terraform.workspace}/dataproc/jars/postgresql-42.3.1.jar\"],\"args\":[\"--bucket_region\",\"us-central1\",\"--type\",\"transformation\",\"--region\",\"us-central-1\",\"--start_date\",\"\",\"--end_date\",\"\",\"--start_hour\",\"\",\"--end_hour\",\"\",\"--region\",\"us-west1\"]}}}")
    oauth_token {
      service_account_email = "dataflow-stg@plucky-cascade-331119.iam.gserviceaccount.com"
    }
  }
}

resource "google_dataproc_cluster" "simplecluster" {
}
