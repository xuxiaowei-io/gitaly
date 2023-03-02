variable "gitaly_benchmarking_instance_name" {}
variable "ssh_pubkey" {}
variable "startup_script" {
  default = <<EOF
    set -e
    if [ -d /src/gitaly ] ; then exit; fi
  EOF
}

provider "google" {
  project = local.config.project
  region  = local.config.benchmark_region
  zone    = local.config.benchmark_zone
}

data "google_compute_disk" "repository-disk" {
  name    = "git-repos"
  project = local.config.project
}

resource "google_compute_disk" "repository-disk" {
  name  = format("%s-repository-disk", var.gitaly_benchmarking_instance_name)
  type  = local.config.repository_disk_type
  image = format("projects/%s/global/images/git-repositories", local.config.project)
}

resource "google_compute_region_disk" "repository-region-disk" {
  count         = local.config.use_regional_disk ? 1 : 0
  name          = format("%s-repository-region-disk", var.gitaly_benchmarking_instance_name)
  type          = local.config.repository_disk_type
  snapshot      = google_compute_snapshot.repository-disk[0].id
  replica_zones = local.config.regional_disk_replica_zones
}

resource "google_compute_snapshot" "repository-disk" {
  count       = local.config.use_regional_disk ? 1 : 0
  name        = format("%s-repository-snapshot", var.gitaly_benchmarking_instance_name)
  source_disk = google_compute_disk.repository-disk.name
  zone        = local.config.benchmark_zone
}

resource "google_compute_instance" "gitaly" {
  name         = format("%s-gitaly", var.gitaly_benchmarking_instance_name)
  machine_type = local.config.gitaly_machine_type

  boot_disk {
    initialize_params {
      image = local.config.os_image
      size  = local.config.boot_disk_size
    }
  }

  attached_disk {
    source      = local.config.use_regional_disk ? google_compute_region_disk.repository-region-disk[0].self_link : google_compute_disk.repository-disk.self_link
    device_name = "repository-disk"
  }

  network_interface {
    network    = "default"
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
    startup-script = <<EOF
      ${var.startup_script}
    EOF
  }

  tags = ["gitaly"]

  lifecycle {
    ignore_changes = [attached_disk]
  }
}

resource "google_compute_instance" "client" {
  name         = format("%s-client", var.gitaly_benchmarking_instance_name)
  machine_type = local.config.client_machine_type

  boot_disk {
    initialize_params {
      image = local.config.os_image
      size  = local.config.boot_disk_size
    }
  }

  network_interface {
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys       = format("gitaly_bench:%s", var.ssh_pubkey)
    startup-script = <<EOF
      ${var.startup_script}
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
