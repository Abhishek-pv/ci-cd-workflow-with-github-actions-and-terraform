terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.22.0"
    }
  }
}

provider "google" {
  project = "nylas-datastreams-prod"
  region  = "us-west1"
  # alias   = "prod_project"
}

provider "google" {
  project = "plucky-cascade-331119"
  region  = "us-west1"
  alias   = "stg_project"
}
