variable "project" { default = "group-gtly-bench" }
variable "benchmark_region" { default = "us-central1" }
variable "benchmark_zone" { default = "us-central1-a" }
variable "gitaly_benchmarking_instance_name" { }
variable "ssh_pubkey" { }
variable "os_image" { default = "ubuntu-os-cloud/ubuntu-2204-lts" }
variable "startup_script" {
  default = <<EOF
    set -e
    if [ -d /src/gitaly ] ; then exit; fi
  EOF
}
variable "gitaly_machine_type" { default = "t2d-standard-4" }
variable "client_machine_type" { default = "n1-standard-1" }
variable "boot_disk_size" { default = "20" }

provider "google" {
  project = var.project
  region  = var.benchmark_region
  zone    = var.benchmark_zone
}

resource "google_compute_firewall" "default" {
  name    = "gitaly-firewall"
  network = "default"

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["8075"]
  }

  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["gitaly"]
}

resource "google_compute_network" "default" {
  name = "test-network"
}

data "google_compute_disk" "repository-disk" {
  name = "git-repos"
  project = "wchandler-a1c8fd64"
}

resource "google_compute_disk" "repository-disk" {
  name = format("%s-repository-disk", var.gitaly_benchmarking_instance_name)
  type = "pd-balanced"
  image = format("projects/%s/global/images/git-repositories", var.project)
}

resource "google_compute_instance" "gitaly" {
  name         = format("%s-gitaly", var.gitaly_benchmarking_instance_name)
  machine_type = var.gitaly_machine_type

  boot_disk {
    initialize_params {
      image = var.os_image
      size = var.boot_disk_size
    }
  }

  attached_disk {
    source = google_compute_disk.repository-disk.name
    device_name = "repository-disk"
  }

  network_interface {
    network = "default"
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = format("git:%s", var.ssh_pubkey)
    startup-script = <<EOF
      ${var.startup_script}
      echo hello
    EOF
  }

  tags = ["gitaly"]

  lifecycle {
    ignore_changes = [attached_disk]
  }
}

resource "google_compute_instance" "client" {
  name         = format("%s-client", var.gitaly_benchmarking_instance_name)
  machine_type = var.client_machine_type

  boot_disk {
    initialize_params {
      image = var.os_image
      size = var.boot_disk_size
    }
  }

  network_interface {
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = format("git:%s", var.ssh_pubkey)
    startup-script = <<EOF
      echo hello
    EOF
  }
}

output "gitaly_internal_ip" {
  value = google_compute_instance.gitaly.network_interface[0].network_ip
}
output "gitaly_ssh_ip" {
  value = google_compute_instance.gitaly.network_interface[0].access_config[0].nat_ip
}

output "client_internal_ip" {
  value = google_compute_instance.client.network_interface[0].network_ip
}

output "client_ssh_ip" {
  value = google_compute_instance.client.network_interface[0].access_config[0].nat_ip
}
