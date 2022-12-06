# Gitaly Benchmarking Tool

## What is this?

An Ansible script for running RPC-level benchmarks against Gitaly.
Currently five open source repositories are used:

- Git
- GitLab
- Linux
- Homebrew-Core
- Chromium

Requests are sent to Gitaly via [ghz](https://ghz.sh/), a gRPC benchmarking
tool. The details of the requests can be found in
`roles/client/files/queries/<RPC_NAME>/<REPO_NAME>`.

## Steps for use

### 1. Create instance

```
./create-benchmark-instance
```

This will create a Gitaly node and a small client node to
send requests to Gitaly over gRPC. This will ask for the Gitaly revision that
will be built.

### 2. Configure instance

```
./configure-benchmark-instance
```

Install Gitaly from source with from desired reference and install other
required tools. A disk image containing the test repositories will be mounted
to `/mnt/git-repositories` on the Gitaly node.

### 3. Run benchmarks

```
./run-benchmarks
```

Run all RPCs against all repos listed in the `rpc` and `repos` section of
`group_vars/all.yml`. By default the Gitaly node is profiled with `perf` and
several `libbpf-tools` binaries for generating flamegraphs and other
performance information. Profiling adds ~10% overhead, and can be disabled by
running the job with:

```
./run-benchmarks --extra-vars "profile=false"
```

The output of the job will be written to an archive titled
`benchmark-<GITALY_REV>-<EPOCH>.tar.gz` in the `results` directory.

### 4. Destroy instance

```
./destroy-benchmark-instance
```

All nodes will be destroyed.
