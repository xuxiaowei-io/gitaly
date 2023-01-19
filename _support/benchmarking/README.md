# Gitaly Benchmarking Tool

## What is this?

An Ansible script for running RPC-level benchmarks against Gitaly.

## Required tools

The following programs must be installed locally to run the script:

- Ansible 2.14 or above
- Terraform 1.2 or above
- gcloud

You must be a member of the `gitaly-benchmark-0150d6cf` GCP group.

## Steps for use

### 1. Create instance

```shell
./create-benchmark-instance
```

This will create a Gitaly node and a small client node to send requests to
Gitaly over gRPC. This will prompt for the Gitaly revision to be built,
instance name, and public SSH key to use for connections.

Use the `gitaly_bench` user to SSH into the instance:

```shell
ssh gitaly_bench@<INSTANCE_ADDRESS>
```

### 2. Configure instance

```shell
./configure-benchmark-instance
```

Build and install Gitaly from source with from desired reference and install
profiling tools like `perf` and `libbpf-tools`. A disk image containing the
test repositories will be mounted to `/mnt/git-repositories` on the Gitaly node.

### 3. Destroy instance

```shell
./destroy-benchmark-instance
```

All nodes will be destroyed. As GCP will frequently reuse public IP addresses,
the addresses of the now destroyed instances are automatically removed from
your ~/.ssh/known_hosts file to prevent connection failures on future runs.
