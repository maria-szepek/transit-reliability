###############################################
# Google Cloud Provider Configuration
# Uses project and region defined in variables.
###############################################
provider "google" {
  project = var.project_id
  region  = var.region
}

###############################################
# DATA LAKE
# Creates a Google Cloud Storage bucket
# This will store raw GTFS data and other files
###############################################
resource "google_storage_bucket" "lake" {
  name     = "${var.project_id}-transit-reliability-lake"
  location = var.region

  # Enforces uniform permissions (recommended)
  uniform_bucket_level_access = true
}

###############################################
# DATA WAREHOUSE
# Creates a BigQuery dataset
# Tables for analytics and realtime scoring
# will live inside this dataset
###############################################
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = "transit_reliability_warehouse"
  location   = var.region
}

###############################################
# COMPUTE VM
# This VM will run ALL containers:
# Kafka, Flink, API, Dashboard, Airflow, etc.
###############################################
resource "google_compute_instance" "vm" {
  name         = "transit-reliability-vm"
  machine_type = "e2-standard-2"   # "e2-standard-4"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 50
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash

    apt-get update
    apt-get install -y git curl

    # Install Docker (official)
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh

    # Install Docker Compose v2 plugin
    apt-get install -y docker-compose-plugin

    # Enable Docker
    systemctl enable docker
    systemctl start docker

    # Allow ubuntu user to run docker
    usermod -aG docker ubuntu
  EOF
}

###############################################
# FIREWALL RULE
# Opens ports so dashboard and API are reachable
###############################################
resource "google_compute_firewall" "allow_http" {
  name    = "transit-reliability-http"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = [
        "8000",  # FastAPI
        "8080",  # Airflow UI
        "8081",  # Flink UI
        "8501"   # Streamlit dashboard
    ]
  }

  # Allow access from anywhere
  source_ranges = ["0.0.0.0/0"]
}

###############################################
# OUTPUT
# Prints the VM external IP after deployment
# We will use this to access the dashboard
###############################################
output "vm_ip" {
  value = google_compute_instance.vm.network_interface[0].access_config[0].nat_ip
}