terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "us-central1"

  credentials = file("../keys/credentials.json")
}

locals {
  gcs_bucket_name        = "${var.project_id}-transit-reliability-raw"
  raw_dataset_name       = "raw"
  analytics_dataset_name = "analytics"
  realtime_dataset_name  = "realtime"
}

# 1. Raw file storage bucket
# This bucket will hold source files such as downloaded GTFS zip archives.
resource "google_storage_bucket" "raw_file_bucket" {
  name          = local.gcs_bucket_name
  location      = "US"
  force_destroy = false

  # Use bucket-level IAM only. This is the recommended permission style.
  uniform_bucket_level_access = true

  # Keep previous versions of files if a file with the same name is uploaded.
  versioning {
    enabled = true
  }
}

# 2. Raw BigQuery dataset.
# This is where extracted GTFS source tables will land.
resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id                 = local.raw_dataset_name
  location                   = "US"
  delete_contents_on_destroy = false
}

# 3. Analytics BigQuery dataset.
# dbt models and final static reliability marts will live here.
resource "google_bigquery_dataset" "analytics_dataset" {
  dataset_id                 = local.analytics_dataset_name
  location                   = "US"
  delete_contents_on_destroy = false
}

# 4. Realtime BigQuery dataset.
# Realtime route/stop reliability aggregates will live here later.
resource "google_bigquery_dataset" "realtime_dataset" {
  dataset_id                 = local.realtime_dataset_name
  location                   = "US"
  delete_contents_on_destroy = false
}

# 5. Machine identity for this project.
#
# A service account is like a robot user. Later, local scripts or Docker
# containers can use this identity to access GCS and BigQuery.
#
# Terraform creates the service account and permissions, but it does not create
# a key file here. Keys should be handled carefully and kept out of git.
resource "google_service_account" "warehouse_client" {
  account_id   = "transit-reliability-warehouse"
  display_name = "Transit Reliability warehouse client"
}

# 6. Permission: allow the service account to manage files in the raw bucket.
#
# This lets future ingestion scripts upload GTFS files to GCS.
resource "google_storage_bucket_iam_member" "warehouse_client_bucket_access" {
  bucket = google_storage_bucket.raw_file_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.warehouse_client.email}"
}

# 7. Permission: allow the service account to edit raw BigQuery tables.
#
# This lets future loaders create/replace raw source tables.
resource "google_bigquery_dataset_iam_member" "warehouse_client_raw_access" {
  dataset_id = google_bigquery_dataset.raw_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.warehouse_client.email}"
}

# 8. Permission: allow the service account user to edit analytics BigQuery tables.
#
# This lets dbt create/replace analytics models and final marts.
resource "google_bigquery_dataset_iam_member" "warehouse_client_analytics_access" {
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.warehouse_client.email}"
}

# 9. Permission: allow the  service account to edit realtime BigQuery tables.
#
# This lets the future realtime pipeline write route/stop reliability aggregates.
resource "google_bigquery_dataset_iam_member" "warehouse_client_realtime_access" {
  dataset_id = google_bigquery_dataset.realtime_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.warehouse_client.email}"
}

# 10. Permission: allow the robot user to run BigQuery jobs.
#
# Creating tables is not enough. BigQuery also requires permission to run jobs:
# load jobs, query jobs, and dbt model builds all count as BigQuery jobs.
resource "google_project_iam_member" "warehouse_client_job_access" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.warehouse_client.email}"
}

output "gcs_bucket_name" {
  value = google_storage_bucket.raw_file_bucket.name
}

output "raw_dataset_name" {
  value = google_bigquery_dataset.raw_dataset.dataset_id
}

output "analytics_dataset_name" {
  value = google_bigquery_dataset.analytics_dataset.dataset_id
}

output "realtime_dataset_name" {
  value = google_bigquery_dataset.realtime_dataset.dataset_id
}

output "warehouse_client_service_account" {
  value = google_service_account.warehouse_client.email
}
