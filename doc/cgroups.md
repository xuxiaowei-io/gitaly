# Cgroups in Gitaly 

## High Level

Gitaly can be configured to run its git processes inside of cgroups, which prevent
shelled-out processes from hogging too much CPU and memory. See [these docs](https://man7.org/linux/man-pages/man7/cgroups.7.html) for a full specification on cgroups.

## Configuration

### Top Level

Here is the top level `[cgroups]` configuration:

```toml
[cgroups]
mountpoint = "/sys/fs/cgroup"
hierarchy_root = "gitaly"
```

**mountpoint** is the top level directory where cgroups will be created.
**hierarchy_root** is the parent cgroup under which Gitaly creates cgroups.
**memory_bytes** limits all processes created by Gitaly to a memory limit,
collectively.
**cpu_shares** limits all processes created by Gitaly to a cpu limit, collectively

### Repository Groups

Cgroups that have a repository-level isolation can also be defined:

```toml
[cgroups.repositories]
count = 10000
memory_bytes = 12884901888 // 12gb
cpu_shares = 512
```

**count** is the number of cgroups to create.
**memory_bytes** limits [memory](#memory-limits) for processes within one cgroup.
This number cannot exceed the top level memory limit.
**cpu_shares** limits [cpu](#cpu-limits) for processes within one cgroup. This
number cannot exceed the top level memory limit.

These cgroups will be created when Gitaly starts up. A circular hashing algorithm
is used to assign repositories to cgroups. So when  we reach the max number of
cgroups we set in `[cgroups.repositories]`, requests from subsequent repositories
will be assigned to an existing cgroup.

### Specific Git Commands

Some git operations are much more expensive than others, and would be useful to
allow a select few to hog up more memory than the general case. Each time we spawn a
git process for one of these commands for a given repository, it will be
isolated into its own cgroup.

```toml
[cgroups.git]]
count = 1000
```

**count** is the number of git command cgroups that will be created for managing
certain git commands that need to be not just isolated by repository but also by
git command.

```toml
[[cgroups.git.command]]
name = "repack"
memory_bytes = 21474836480 // 20gb
cpu_shares = 800

[[cgroups.git.command]]
name = "pack-objects"
memory_bytes = 21474836480 // 20gb
cpu_shares = 600
```

**name** is the git command to create a cgroup for.
**memory_bytes** limits the [memory](#memory-limits) for this git command for a
repository. This cannot exceed the top level memory limit.
**cpu_shares** limits the [cpu](#cpu-limits) for this git command for a repository.
This cannot exceed the top level memory limit.

NOTE: These git-command cgroups will not be created underneath its respective
repository-level cgroup. See the [hierarchy](#cgroups-hierarchy) below. The
purpose of these repository/git command cgroups is so admins can set a higher
limit for some git commands if they wish. If these were created underneath the
repsitories cgroups in the cgroup hierarchy, that means it would be controlled
by the limt set under `[cgroups.repositories]`.

Similar to repository level groups, these cgroups will also be created on
startup. The difference is however, these cgroups are dedicated for a single
process. In Gitaly, we will keep track of which process is being managed by which
git command cgroup. We will maintain a pool of these git command cgroups in
memory.

When a command is executed, we check what git command it is. If it matches one
of the ones configured under a `[[cgroups.git.command]]`, we will attempt to
assign it to a dedicated git command cgroup that will only be responsible for 
managing that command. Once the command finishes, this cgroup will be returned
back to the pool to be used by a subsequent git command. 

If no git command cgroups are left in the pool, the command will run without a
cgroup.

## Memory Limits

Each cgroup has a memory limit which in this example config, is 12gb. All
processes that are part of a cgroup are limited to 12gb of memory collectively.
This means that if there are 5 processes that collectively use up 10gb, and a
6th process is added to the cgroup and its memory slowly climbs beyond 2gb, the
OOM killer will target the process with the highest memory usage to be killed.

This is an oversimplification of how the OOM killer works. A more detailed
picture can be found [here](https://blog.crunchydata.com/blog/deep-postgresql-thoughts-the-linux-assassin#:~:text=CGroup%20Level%20OOM%20Killer%20Mechanics&text=First%20of%20all%2C%20the%20OOM,%2Fcgroup%2Fmemory%2Fmemory.)
and [here](https://lwn.net/Kernel/Index/#OOM_killer).

This provides baseline protection from processes running haywire, such as a
memory leak. However, this only provides limited protection because there are
some legitimate git processes that can take a huge amount of memory such as
`git-repack(1)`, or `git-upload-pack(1)`. For a large repository, these
operations can easily take 12gb of ram as seen in production systems.

Hence, we also need finer grained controls to allow certain expensive git
operations to have their own cgroups.

## CPU Limits

Each cgroup has cpu limits as defined by a concept called cpu shares. By
definition, full usage of a machine's CPU is 1024 shares. Anything lower than
that will be a fraction of the total CPU resources a machine has access to.

## Cgroup Hierarchy

```
/sys/fs/cgroup
|
|--memory
|    |--gitaly
|         |--gitaly-<pid>
|               |--repository(@hashed/00/000)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/001)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/002)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/003)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/004)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/005)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/006)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/007)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/008)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/009)
|               |     |--memory.limit_in_bytes
|               |--repository(@hashed/00/010)
|               |     |--memory.limit_in_bytes
|          	|--git-command(@hashed/00/010, repack)
|          	|      |--memory.limit_in_bytes
|          	|--git-command(@hashed/00/010, repack)
|          	|      |--memory.limit_in_bytes
|          	|--git-command(@hashed/00/010, repack)
|           	|      |--memory.limit_in_bytes
|
|-cpu
|  |--gitaly
|        |--gitaly-<pid>
|              |--repository(@hashed/00/000)
|              |     |--cpu.shares
|              |--repository(@hashed/00/001)
|              |     |--cpu.shares
|              |--repository(@hashed/00/002)
|              |     |--cpu.shares
|              |--repository(@hashed/00/003)
|              |     |--cpu.shares
|              |--repository(@hashed/00/004)
|              |     |--cpu.shares
|              |--repository(@hashed/00/005)
|              |     |--cpu.shares
|              |--repository(@hashed/00/006)
|              |     |--cpu.shares
|              |--repository(@hashed/00/007)
|              |     |--cpu.shares
|              |--repository(@hashed/00/008)
|              |     |--cpu.shares
|              |--repository(@hashed/00/009)
|              |     |--cpu.shares
|              |--repository(@hashed/00/010)
|              |     |--cpu.shares
|              |--git-command(@hashed/00/010, repack)
|              |      |--cpu.shares
|              |--git-command(@hashed/00/010, repack)
|              |      |--cpu.shares
|              |--git-command(@hashed/00/010, repack)
|              |      |--cpu.shares
```
