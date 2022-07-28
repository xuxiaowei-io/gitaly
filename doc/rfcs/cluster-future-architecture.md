# Gitaly Cluster future design

Gitaly Cluster has a few aspects in its architecture that make some things unfeasible or more difficult than they have to be. This document describes on a high level an alternative architecture that aims to address these shortcomings.

## Background

Gitaly Cluster currently consists of three components:

- Praefect for managing replication and routing
- Postgres for storing the metadata Praefect needs
- Gitaly for actual storage of repositories

Praefect exposes a storage to the clients like a regular Gitaly. The clients send requests to Praefect. Praefect consults the metadata stored in Postgres to proxy the requests to the correct replicas. Writes use a two-phase commit protocol, where each of the replicas perform the same operation and cast a vote on their result to Praefect. Praefect then instructs the replicas to commit or abort depending on whether a quorum is reached. Gitaly uses the reference transaction hook of Git to hash all the reference changes being made and uses the result as a vote to Praefect. The primary and half of the secondaries are required to agree before committing. Once a write is completed, it's recorded in the database by incrementing the generation number of the replicas that successfully applied the write and scheduling replication jobs to the nodes that didn't.

Reads follow the same general flow. Praefect consults the metadata to find a replica that is on the latest generation and proxies the read there. As reads are high throughput, the in-sync replica set is cached in-memory on Praefects. The cache is invalidated by asynchronous notifications from Postgres following a post-write metadata update.

There are various problems in the current solution that cause inconsistencies. Some problems specific to Gitaly Cluster are:

1. [#4375](https://gitlab.com/gitlab-org/gitaly/-/issues/4375) - The writes proxied to the replicas are not ordered in any way. This can result in a replica becoming out of sync even in normal operation. This can happen if Praefects are proxying two different writes to a repository concurrently.
1. [#3955](https://gitlab.com/gitlab-org/gitaly/-/issues/3955) - Writes are only recorded in the database by Praefect after they've been performed on the replicas. If Praefect crashes after a write is performed but before updating the metadata, the replicas can diverge without the cluster ever realizing it. If there are no further writes to the affected repositories, they may keep serving inconsistent reads from the replicas indefinitely. As the metadata was not updated, the write may get replicated over. It's possible that a client reads a write that ultimately gets discarded by the cluster.
1. [#4015](https://gitlab.com/gitlab-org/gitaly/-/issues/4015) - The cache used for distributing reads to in-sync replicas utilizes asynchronous notifications for invalidation. If the client sends a follow up request before the cache invalidation message is received by a Praefect, Praefect may proxy to a replica that doesn't have the write the client just performed.
1. [#4380](https://gitlab.com/gitlab-org/gitaly/-/issues/4380) - In-flight writes that have not yet been committed may be read by other transactions. This can happen as the generation number is incremented only after the write. Some replicas may already have the new write on the disk while some not.
1. Another form of inconsistency in the cluster is caused by indeterminism in the writes. While we've fixed the known cases of these, the architecture still allows for them and the fixes leak from the API. A good example of this is having to provide [commit timestamp in the request](https://gitlab.com/gitlab-org/gitaly/-/blob/942d8298610eded70b5204863048617ee1c136cd/proto/operations.proto#L652-654) so all of the replicas generate the same commit.

There are also some inconsistency issues that affect Gitaly. These contribute to the issues in Gitaly Cluster:

1. [#4374](https://gitlab.com/gitlab-org/gitaly/-/issues/4374) - If Git crashes during a reference transaction, the references may end up in a state that the client never pushed. The locks that are acquired on the disk are simply deleted by our housekeeping which can leave transactions partially applied if Git was in process of updating the references. It's not possible to derive from the state we have whether or not the updates in the locks should be committed or rolled back. The replicas may diverge from each other due to this.
1. [#4378](https://gitlab.com/gitlab-org/gitaly/-/issues/4378) - Git doesn't apply the reference updates atomically. If a client reads during a reference transaction that is being applied, it will receive an in-between state where some references have been updated already and some not.

Deploying a Gitaly Cluster is more difficult than it has to be. One needs to add one or more Praefects and a Postgres database. To create the necessary metadata, the repositories need to be migrated into the new cluster. Gitaly Cluster relies on the Postgres to be highly available but defers to third parties for that functionality. Gitaly Cluster is a separate system from Gitaly. This has slowed down the rollout of Gitaly Cluster as one can't simply upgrade Gitaly and have it support clustering.

Gitaly Cluster is heavily dependent on the metadata in the database. It's not possible to say from the on-disk replicas which of them contains the most recent data. As the writes are only recorded with a monotonically increasing counter, Praefect doesn't really know what the data is even supposed to look like, only which node is supposed to contain the latest data. This makes it difficult to verify that the metadata is correct and that the replica still contains the expected data. A solution would be to store checksums in the database for each write so Praefect could compare the checksum. However, it's not possible to checksum a repository in Gitaly Cluster without blocking writes to it. Concurrent writes could cause the checksum to include different references on different replicas causing a mismatch. Blocking writes would allow for calculating the correct checksum but calculating the checksum can be slow if a repository has a lot of references.

Solving the inconsistencies after crashes requires Gitaly Cluster to maintain a write-ahead log so any operation that was interrupted by a crash can be performed to completion while recovering from the crash. A write-ahead log is a data structure that persists each operation to be performed prior to performing it. If a crash happens while applying an operation, the system can still recover to a good state by reapplying the partially applied operations from the write-ahead log.

There are various solutions one could come up with for the other problems above. Looking at the bigger picture, changing the Gitaly Cluster's replication flow to be based on the write-ahead log can help solve all of the above problems. The primary replica would append updates to the write-ahead log. The secondaries would replicate the log and apply the operations from the log. This helps solve the previously mentioned issues:

1. Operations are ordered on the primary. As secondaries apply the log in the order decided by the primary, the replicas can't receive writes in different orders anymore.
1. Indetermininism in the incoming writes won't be an issue anymore as the primary decides the final value. The secondaries replicate the commits from the log and thus will store the same commit with the same timestamp.
1. Crash recovery is possible as the Gitalys can always reapply the writes from the log. This way partially applied writes won't leave the repositories inconsistent until another write arrives.
1. Less reliance on the metadata in the database as a replica's status can be determined from its last replicated position in the log.

While we could possibly plug in a write-ahead log to the current Gitaly Cluster architecture, we'd end up having to design a lot of the replication semantics ourselves including leader election logic and replica membership changes which are not covered by just having a log. This can be quite difficult as proven also by the shortcomings of our current solution. Fortunately, there exists a variety of existing algorithms we can use.

Distributed consensus algorithms like [Raft](https://raft.github.io) and the numerous [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) variants solve exactly the problem of replicating a log in a distributed manner. Out of the various algorithms, Raft seems like a good choice for Gitaly Cluster as it not only provides a solution for log replication but also for membership changes in the replication groups and is easier to understand than the other commonly used algorithms. The remaining of this document describes a Raft based architecture for Gitaly Cluster and outlines the ways it solves our existing problems and new challenges it introduces.

# New design

## Goals

As rearchitecting Gitaly Cluster is a large effort, we should list out the goals we set out to achieve to ensure the new architecture helps us get where we want. The goals include:

### Gitaly Cluster as the only storage backend

GitLab currently supports both Gitaly and Gitaly Cluster as storage backends. We want Gitaly Cluster to be the only storage backend for GitLab. This helps minimize the development and support complexity by only supporting a single system.

Each Gitaly should become a single node Gitaly Cluster through an upgrade. To support this, we want to keep the installation size minimum and remove Postgres and Praefect from the architecture.

### Strong consistency semantics

The changes should lead to a clear consistency story. This means:

- Clients should never read partially applied writes. Writes become atomically available to everyone at the same time only after they are fully committed.
- Reads are always served the latest write available when the read begins.
- Proper replication guarantees. Clients should be able to trust the writes are persisted on at least N/M nodes before it is acknowledged.

It must be clear from the design how the consistency guarantees are maintained throughout every operation.

### Storages as the single source of truth

Gitaly Cluster must hold enough state in the storages to completely rebuild any metadata it needs to operate. This ensures the cluster can recover from scenarios where the metadata is lost.

### Support for online data verification

Gitaly Cluster needs to support online verification of the repositories without blocking writes to them. This allows for periodic verification of the data and to identify changes that have occurred on the disk directly without going through the usual write flows.

### Horizontally scalable

The goal is to have a single Gitaly Cluster service an entire GitLab installation, including GitLab.com. To do so, we need to make sure Gitaly Cluster supports vastly larger clusters than what it currently does. For a more concrete goal, we should support at least 1000 Gitaly node clusters to ensure the design is scalable.

### Painless migration

The migration to the new architecture should happen step by step and be as painless as possible. Ideally as much as possible should happen automatically during upgrades.

## High-level Design

### Prerequisites

- Relative path removed from the API. Gitaly decides the paths, the clients refere to repositories by Gitaly Cluster minted ID.

- Object Pools are ignored for now. They are broken already in Gitaly Cluster and their fate should be decided separately. Whatever solution is picked for them, we should ensure it fits well here.

### Repository write-ahead logging

Gitaly currently has no concurrency controls in place for isolating in-flight reads and writes from each other. While Git ensures references are locked prior to being updated, the references are not updated atomically. This brings two problems with:

1. Inconsistent reads
1. Inconsistencies after crash

Inconsistent reads can occur as Git doesn't block reads on reference locks and doesn't apply reference updates atomically. When applying a reference transaction, Git acquires locks on all references being updated by writing a `<reference>.lock` file with the new object ID the reference should point to. Git then renames these locks to the target reference one by one. If a read occurs when Git has updated some of the references, some of the references will be updated but some not. The references are in an in-between state that were never pushed by the client.

Gitaly can't recover repositories from crashes.

### Data Replication

Gitaly Cluster needs to replicate repository data to multiple nodes for:

1. *Fault tolerance* - If a node goes down, the data is still available on some other node.
2. *Performance* - If we have multiple copies of the data on different nodes, it's possible to distribute the load by using multiple nodes to serve the data.

Doing replication correctly is difficult. There are number of inconsistencies and races one needs to take into account. To avoid these pitfalls, Gitaly Cluster should not invent its own replication algorithm but should utilize an existing and proven replication algorithm.

For the client, writing to a replicated repository should be exactly like writing to an unreplicated one. This ensures the complexity of the replication logic doesn't leak outside of Gitaly Cluster.

The repositories can be thought of as state machines that apply operations one by one. If they start from the same state and apply the same operations in the same order, they'll end up in in the same end state. There are various algorithms for replicating a state machine such as the Paxos variants and the more modern [Raft](https://raft.github.io). Raft is simpler to understand, has great documentation and implementations around making it a good choice for Gitaly Cluster.

#### Raft consensus

The [Raft whitepaper](https://raft.github.io/raft.pdf) does a great job at explaining the algorithm in more detail but here's a brief summary. Raft is an algorithm for replicating a log between group members in strongly consistent manner. The log entries are applied to a state machine which essentially contains an up to date view of the log. It ensures strong consistency by replicating each log entry to a quorum of followers prior to the log entry being considered committed. Only an up to date replica can be elected as a leader. The elected leader is the only node that performs reads and writes, the followers direct the client to the leader. It manages snapshotting and compacting the log so the write-ahead log won't keep growing forever. Additionally, it manages group membership changes with regards to adding and removing replicas.

#### Raft in Gitaly Cluster

As Raft group members are mirrors of each other, Gitaly Cluster needs to maintain a Raft group for each repository. This ensures the replication factor is configurable on a per-repository basis by adding or removing members from the Raft group.

Raft has strong membership meaning the replicas know who their peers are and the group stores the membership information. The question then is what should make up a group members's identity? The identity must be stable and never change to ensure the persistent identity remains consistent. This rules out use of any network related information as the address of the node hosting the replica may change. As network related information can't be used as the identity, there needs to be someway to resolve the group member's identity into an address that the requests can be sent to. Using directly just the replica's ID in the storage would mean that Gitaly Cluster needs to ensure replica IDs on storages are globally unique and be able to locate them. This would lead to excess overhead as a routing table listing every replica would need to be maintained. A good compromise is to identify the group member's by a composite key of (`storage_id`, `replica_id`). Storages need to have a stable identity so the other nodes know which storage to address the request to. Replica within a storage need to have a unique identity so the correct replica can be accessed. As both must be stable, they make up a good identity for a member replica of a repository's Raft group. Storages are also sufficiently coarse to use in the routing table. Each storage should be located on a separate disk. As the location resolving happens based on the storage, the admins have flexibility in relocating storages to other nodes as they see fit and have the cluster correctly pick them up.

The writes are serialized within a Raft group. Having a separate group for each repository also guarantees that problems writing into one repository do not block writes into another repository. There's no need to ensure consistency across repositories since they are accessed as independent units.

The state machine of the Raft group is the repository on the disk. Each operation that should be replicated needs to be appended to the group's log. As the leader of the group is the only one who appends to the log, all writes must go through the leader. A log entry is considered committed once it has been replicated by a majority of the replicas. Once a log entry is committed, the write can be acknowledged to the client and applied to the repository.

Not all of the operations in repositories need to be replicated. In order to keep the repositories well maintained and performant, Gitaly has various housekeeping operations it runs such as packing the references and objects. These are internal details and don't affect the correctness of the data as far as users are concerned. Such operations do not need to go through the log and can be independently managed by the replicas. The repositories stored on Gitalys contain a few different types of data that should be replicated as they can affect what the users reads or writes.

1. `config`
1. `info/attributes`
1. Custom Hooks
1. References
1. Objects

`config` and `info/attributes` may have an effect on the data written to and read from the repository for example via line ending normalization. They need to be replicated to ensure the operations behave similarly on all of the replicas. References and objects are the main user data pushed to the repository and need to be replicated. Custom hooks can also affect the behavior of a repository and thus need to be replicated.

Out of the data Gitaly Cluster needs to replicate, only data that requires strong ordering needs to go through the Raft log. For `config`, `info/attributes` and custom hooks, ordering is important as they may affect how writes and reads behave. Each replica needs to behave the same at a given log position to ensure consistency.

References require ordering. Updating references in wrong order could lead to an incorrect result if an older value overwrites the latest value of the reference. Updates to references can fail if the previous value of the reference does not match the expected previous value in the update. References must go through the log as well.

Objects are different and do not require strong ordering. Git store's objects based on their content by hashing the object and using the hash as the object's identifier. This makes the writing objects to the repository idempotent. The ordering is not important, only that the objects reachable from the the references in the log and the repository are present. This gives some liberties in how Gitaly Cluster replicates them.

#### Logging Writes

Raft relies on the log for ordering operations and replication. Gitaly needs to log the writes performed in the repositories.

Gitaly has four different types of writes targeting existing repositories:

1. `info/attributes` changes
1. Config changes
1. Custom hook changes
1. Reference changes - these are the majority, including pushes and various `OperationService` RPCs. They update, create or delete some references. They may or may not push or create new objects as part of the change.

They all need to be logged and replicated prior to being applied to the repository. We have an option to either log the requests and have all of the replicas perform the operation locally or just logging the final result.

If the operation performed is expensive to compute, having to perform the operation on all of the replicas is wasteful. Logging operations also has a problem with indeterminism. If the operation is indeterministic, the replicas may end up with different results. This has been a problem already with Git commit timestamps being different across replicas leading to different commit IDs for the same operation. This was patched around by having the clients include a [timestamp in the request](https://gitlab.com/gitlab-org/gitaly/-/blob/b731241d770f8cd1e8b81f48d8a011f0383b4b48/proto/operations.proto#L278-280) that all replicas use to generate the commit. This feels like a leaking implementation detail that forces the client to resolve the indeterminism.

To avoid inconsistencies from indeterminisn and multiplying the work by performing the operations on each of the replicas, Gitaly Cluster should log the final result and replicate that. Having the leader determine the result avoids the indeterminism and limits the work to the leader.

Taking a closer look at the individual changes:

`info/attributes` is changed only via [`ApplyGitattributes` RPC](https://gitlab.com/gitlab-org/gitaly/-/blob/993d944ebeea3e0ec8157481f125f9c70161ae4a/internal/gitaly/service/repository/apply_gitattributes.go#L124). The RPC is writes the `.gitattributes` file from a given revision directly to the `info/attributes` on disk.

`config` nowadays only has one RPC modifying it, the [`SetFullPath`](https://gitlab.com/gitlab-org/gitaly/-/blob/993d944ebeea3e0ec8157481f125f9c70161ae4a/internal/gitaly/service/repository/fullpath.go#L17). The RPC writes the repository's project path to the config using Git.

The only RPC that modifies custom hooks is [`RestoreCustomHooks`](https://gitlab.com/gitlab-org/gitaly/-/blob/993d944ebeea3e0ec8157481f125f9c70161ae4a/internal/gitaly/service/repository/restore_custom_hooks.go#L24). It unpacks custom hooks from an archive the client sends. This is a one off RPC for restoring hooks after a backup restoration.

The above three cases are fairly simple to log by performing the modifications and writing the resulting files into a log entry rather than the disk. Once the log entry has been committed, the file can be updated on the disk from the log.

Custom hooks have an additional peculiarity. They are generally installed by putting the hook files directly in the correct directory in the repository. This doesn't work well for replication as this is a write without a log entry. There is no clear ordering with the hooks and the other writes while the hooks are being installed. There needs to be clear ordering as the hooks may affect the behavior of writes. Gitaly also doesn't really know if the hooks were modified on the disk. To properly support custom hooks in Gitaly Cluster, we should provide an RPC to install and remove hooks from the repository. The RPC can then write the hooks to the log first and finally apply them on the disk once the log entry is committed.

Reference changes make up the majority of writes performed. They originate from pushes and various other RPC like the ones in [OperationService](https://gitlab.com/gitlab-org/gitaly/-/blob/993d944ebeea3e0ec8157481f125f9c70161ae4a/proto/operations.proto#L16). Reference changes may or may not include new objects to be replicated. There are no new objects to replicate if the reference pointed to an existing object in the repository or a reference is deleted. The reference changes can simply be written to the log and applied from there.

Keeping the references purely in the log is not safe. If the log contains references that the repository doesn't, there's a danger that the objects that are reachable from the logged references may be pruned. This would lead to data loss. To avoid this, the references in the log need to be written also to the repository prior to the log entry persisted on the disk. Once the log entry containing the references is compacted away, the internal references can also be dropped.

An alternative solution to keeping internal references for the logged references could be to include objects in the log entries. This is not ideal though as it would require a lot of space. The objects can be large. To avoid the data loss from pruning, one would have to keep the entire history reachable from the objects pointed to by the commits as there could be a later log entry that deletes all of the references in the repository. Due to this, it's better to keep the objects in the repository's object database rather than the log entries and maintain internal references to the objects referred to from the logs. This allows git to deduplicate the objects while safely pruning unneeded objects.

As the log entries do not contain the objects, the followers need to fetch the logged references from the leader to internal references prior to persisting the log entry on the disk. This ensures the followers have all of the objects in the repository. Afterwards the follower can persist the log entry and finally apply it to the repository once the leader instructs it has been committed.

#### Scaling Raft



##### Active and Inactive Groups

##### Deterministic Replica Placement

Gitaly nodes need to talk to each other if they contain replicas of the same repository. The replicas need to communicate with each other for reads, writes and Raft related traffic like hearbeats. Each Gitaly pair that need to communicate with each other require a network connection open between them. Keeping connections open is not free of cost. Having to talk with a wide variety of different nodes makes also other optimizations like request batching less effective as it's not possible to batch requests to different nodes. In the worst case, the repositories are spread between the cluster nodes evenly in a manner where every node needs to talk every other node. This leads to quadratic scaling of the network connections when nodes are added to the cluster. When a repository is created, Gitaly Cluster will have to choose where it is stored. If the repository replicas are distributed randomly on the Gitalys, eventually all of the Gitalys will have to talk to each other. In order to keep the cluster working efficiently with a larger number of nodes, say 1000 Gitaly nodes, we need to limit the number of nodes that need to communicate with each other.

As the Gitaly nodes only need to talk to the other Gitaly nodes if they host replicas of the same repository, the number of open connections needed can be controlled with careful placement of replicas. We can dynamically form shards of the Gitaly hosts and strive to keep the replicas of a repository on the same shard. This ensures that only the nodes part of that shard need to communicate with each other. The connection count then scales quadratically with number of nodes in a shard as opposed to the total number of nodes in a cluster. Each repository gets assigned to a Gitaly node when it is being created. A shard is formed by deterministically picking the other Gitaly nodes where the replicas of a repository assigned to a given Gitaly always land.

For example, consider a scenario with 10 Gitaly nodes:

```
0 1 2 3 4 5 6 7 8 9
```

Repository A is created with replication factor of three. It's assigned to Gitaly 5. To deterministically pick the locations of the other two replicas, they always go to the Gitaly nodes on the right:

```
          A a a
0 1 2 3 4 5 6 7 8 9
```

Now Repository B is being created with replication factor of 5. It gets assigned to Gitaly 2, so it's the other replicas land on Gitalys 3, 4, 5 and 6.

```
          A a a
    B b b b b
0 1 2 3 4 5 6 7 8 9
```

Finally, Repository C is created with Replication factor of 1. It gets assigned to Gitaly 9.

```
          A a a
    B b b b b
                  C
0 1 2 3 4 5 6 7 8 9
```

In the above scenario, only Gitalys 2 to 6 need to communicate with each other, as do Gitalys 5 to 7.

The process could go on and on but the number of nodes each Gitaly needs to communicate with is limited. It's determined by the largest replication factor of a repository hosted on the Gitaly. The shards are not explicitly configured but are rather dynamically formed through repository placement.

Below illustrates that the set of nodes that need to communicate with each other stays static with the deterministic replica placement.

```
A a a
  B b b
    C c c
      D d d
        E e e
          F f f
            G g g
              H h h
j               J j
k k               K
0 1 2 3 4 5 6 7 8 9
```

The maximum number of connections on a node in a cluster with this placement strategy is `(maximum_replication_factor - 1) * 2`. Nodes need to open a connection to all of the other replicas. In the above example, Gitaly 2 hosts replicas of repositories A, B and C. It needs to communicate with the other Gitaly nodes hosting replicas of them, which means Gitalys 0, 1, 3 and 4.

The nodes may need to communicate across shard boundaries temporarily for example when a repository is being moved to another shard. For example, repository A is assigned now to Gitaly 1 but during a rebalance it is reassigned to Gitaly 2. In order to move the replica from Gitaly 1 to Gitaly 4, the nodes need to temporarily hold open a connection to both Gitaly 1 and 4 until the move has been completed.

In the below example, Gitaly 1, 2 and 3 needed to initally only communicate with each other but have to temporarily communicate with Gitaly 4 to get to the desired state.

```
  A a a
    A a a
0 1 2 3 4 5 6 7 8 9
```

The deterministic placement of replicas has two downsides:

1. The replicas can't be balanced arbitarily across the nodes. The shard boundaries need to be upheld in order to keep the cross-talk in check.
1. It can create hotspots in the cluster during node failures.

The first issue is likely not a problem. Even if replicas can't be arbitarily relocated, one can assign the repository to a different shard to rebalance. This may require moving all of the replicas even if only one out of three nodes in the shard are overloaded. This is requires extra work but balancing the cluster is possible.

The second issue can be more of a problem. If a Gitaly node fails, the replicas on it need to be relocated on others nodes to maintain the replication factor of the repositories. This puts strain on the other nodes that co-host the repository along with the failed node as they need to replicate the data on new nodes. For example, if in the below scenario Gitaly 3 fails:

```
      A a a
  B b b
0 1 2 3 4 5 6 7 8 9
```

Replicas of A and B on Gitaly 3 needs to be relocated to other nodes. As Gitaly 1, 2, 4 and 5 are the only nodes with replicas of A and B, they'll do the work of replicating to a new node. This may be fine with only a small number of repositories on each node but in reality there will be tens or hundreds of thousands of repositories. This can put excessive load on the nodes in the same shard as Gitaly 3. To better balance the load in these scenarios, the size of the shards can be increased past what the replication factor requires the minimum size to be. A larger shard allows for spreading the replicas over larger number of nodes which can balance the load in recovering from a node failure.

For example, if the shard size is configured to be six and repositories A and B are assigned to Gitaly 1:

```
   A   a     a
   B b     b
0 [1 2 3 4 5 6] 7 8 9
```

Increasing the shard size past the minimum allows for spreading the replication load following a node failure across a wider set of node. As increasing the shard size also increases the number of connections each node needs to maintain, there's a trade-off between balancing the load and the number of connections each node needs to hold open. The shard size is what limits the cross-communication. Within a shard, the replicas can be balanced in a random fashion although they should be balanced based on the load on the nodes.

##### Request Routing

When a client wants to access a repository, the first problem it faces is where to send the request. The replicas of a given repository could be on any Gitaly node in the cluster. The cluster can be large which makes it inefficient for the clients to try and find a repository on the cluster by trial and error. Someway of routing requests to the correct nodes is required. As every request will need to find the location of a repository, the location data should cachable to avoid having to look up the location of a repository on every request. Having to update all of the location caches synchronously could be difficult so the caching should work gracefully with stale information.

One approach could be to proxy the client requests to the correct node. The client could access the cluster through a load balancer which routes the request to some Gitaly node. The request receiving Gitaly node could then proxy the request to the correct Gitaly node. The benefit with this approach would be that no separate router components are needed and the logic can be kept entirely on the Gitaly servers. Each Gitaly can route the request to the correct node or handle it themselves if they host the repository. The Gitalys themselves could find the location of repositories through the metadata group which contains the locations of every repository on the cluster. They could cache the location locally and handle misroutings caused by stale records gracefully. This approach has some downsides though.

The proxying adds an unnecessary network hop to most requests which leads to higher latencies and extra network traffic. Since the clients would always just access a random Gitaly, most requests would need to be proxied in a larger cluster. As each request involves more nodes, troubleshooting is more involved. Implementing the proxying also brings its own complexity.

To avoid the proxying related complications and the extra network load related to it, it would be ideal if the clients can access the correct Gitaly node directly. To do so, they need to be able to learn the locations of repositories on the cluster. To avoid excess load on the metadata group, the clients should cache the location information.

The first hurdle is where can the client's find the address of the metadata group?

In the most common case, the client accesses an existing repository for reading.



The clients taking part in the routing logic has downsides as well. We have to provide client implementations that support the routing logic. We already do provide client implementations to support the Gitaly specific dialing schemes so we'd mostly have to add support for the routing logic. The other concern is that this puts trusts in the client doing the right thing. If the clients don't cache the location information correctly, they could put more strain on the cluster. If there is a bug in the client implementation, it can lead to problems that are more difficult to fix as we'd have to update all of the clients. Overall, this should likely be fine. Gitaly Cluster is an internal component called by trusted services. It's not exposed directly to the public, so the client's could be expected to behave well.

### Cluster Management

Cluster management with Gitaly Cluster should be straightforward and safe.

Adding nodes may be necessary to increase capacity of the cluster, or to replicate data to a wider array of geographical locations for resilience in the face of disasters. Nodes may need to be removed also for variety of reasons for example due to upgrading to better hardware or removing faulty nodes. The cluster should be dynamically configurable without requiring restarts of other nodes.

The Gitaly nodes themselves don't need an identity but the storages do. As the replicas of a repository are identified by `(storage_id, replica_id)`, the Gitaly nodes need to be able to resolve the address of a node hosting a given storage. If two storages would have the same ID, that would result in ambiguity with regards to which of the storages are being referred to. The storages need an ID that is guaranteed to be unique.

The replicas of a repository are identified by `(storage_id, replica_id)`. This is different on each of the storages and the keys will change with replicas being created and deleted. The repositories need and externally stable ID the clients can use to access the repositories regardless of replica changes. These also need to be guaranteed to be unique.

#### Metadata Group

Repositories need a stable external identity the clients can use to access them. Clients can't use the replica IDs to access the repositories as the replica IDs of a repository may change due to the replicas being moved, deleted or added. The repositories need a separate stable ID. The IDs must be guaranteed to be unique so there's no ambiguity in which repository is being accesed. Gitaly Cluster needs a central location to mint repository IDs in order to guarantee their uniqueness.

Request routing also needs to have a central location to consult for the repository locations to guarantee new repositories become immediately visible to any router. In order to route requests, the Gitaly nodes also must find the other storages on the network.

Gitaly Cluster needs a dedicated Raft group for managing central metadata in the cluster. A subset of Gitaly nodes should be selected to host the metadata group to keep the quorum small for efficiency.

Its responsibilities will specifically be:

1. Allocate IDs for repositories being created.
1. Allocate IDs for storages joining the cluster.
1. Maintain mappings from storage ID to a network address.
1. Maintain mappings from external repository ID to the replicas.

The metadata group's leader serves as a central authority in the cluster for functionality and data needed by all of the Gitaly nodes.

#### Finding the Metadata Group

When the Gitaly's first start up, they don't know the addresses of the other Gitalys. They'd consult the metadata group for this information but they also don't know the location of the metadata group yet. How should the Gitaly's find the metadata group? The metadata group members also need to be able to find each other so they can elect a leader and form a quorum.

One option could be to provide the addresses of one or more members of the metadata group to Gitaly nodes manually on start up. If a member of the metadata group fails, the group needs to be replicated to a new node and the old member kicked out. The group may thus move around in the cluster. This makes maintaining static configuration inconvenient.

A more convenient option could be the use the broadcast address to broadcast the location of the metadata group from the members of the group to the network. While this would support the dynamic changes, network broadcasting doesn't really work across subnetworks. The Gitaly nodes are likely to be located in separate subnetworks, for example in different availability zones. This makes using network broadcasting infeasible. If broadcasting from the group members is not possible, how could the metadata group members share their location to the other Gitaly nodes?

Gossip protocols are a good fit here and in particular [SWIM](https://en.wikipedia.org/wiki/SWIM_Protocol). The article describing the algorithm (DOI: 10.1145/383962.384010) is unfortunately paywalled but one can find other descriptions of the algorithm online. There's also an open-source implementation with some [extensions](https://arxiv.org/abs/1707.00788) added called [`memberlist`](https://github.com/hashicorp/memberlist). `memberlist` is the underlying gossip library used in [Consul](https://www.consul.io).

SWIM forms a network of nodes that disseminate information throughout the network in a decentralized manner. New nodes can join the network by contacting one of the other nodes that are part of the network. The joins are disseminated to the other peers so knowing the address of only one member is enough. Each member health checks a subset of other members in a randomized order in a given time interval. If a health check fails, the node that issued the health check will double check by asking other nodes to also perform a health check against the unavailable node. This ensures connection issues between just two nodes don't result nodes being declared faulty. If none of the health checks succeeded, the node is declared faulty. The information of the faulty node is disseminated throughout the network and the node is dropped.

For Gitaly Cluster, SWIM provides the following features:

1. Dynamic discovery of the rest of the nodes by knowing at least one member.
1. Decentralized health checking of the nodes.
1. Time-bounded, decentralized dissemination of information throughout the cluster.

With the above, the Gitalys have the means to discover the metadata group. When a Gitaly node is started, it should be provided the network address of at least one member of the cluster through which it joins the gossip network. The Gitaly nodes that contain metadata group's state on their storages gossip the storage IDs on the network as they join. This allows the metadata group members to find each other, form a quorum and elect a leader. The leader of the group then disseminates the location of itself and the other group members through out the network. This allows the other Gitaly nodes to find the leader and the followers of the metadata group.

Each time there is a leader or a configuration change in the metadata group, the leader of the group must gossip the new location of the metadata group. The leader should include the current term and log position so the other Gitaly nodes can ignore gossip messages with old state in them. The gossip protocol maintains only ephemeral state that is created and removed as nodes join and leave the network.

Q: How to ensure replicas previously removed from the metadata group don't come back, form a second quorum and announce their existence? This would cause a split-brain.

#### Bootstrapping the Cluster

When Gitaly Cluster is being setup for the first time, there is no metadata group yet. As the other synchronization in the cluster relies on the metadata group, it needs to be created prior to the cluster being functional.

To create a new cluster, an initial Gitaly node needs to be started up. The Gitaly node doesn't know if it's joining an existing cluster or if this is a new cluster being created. The Gitaly node needs to be separately told to initialize the metadata group.

We'll need a manual command, for example `gitaly init`, to initialize the metadata group. It creates the metadata Raft group with only itself as the member and elects itself as the leader. The Gitaly node will then announce the location of the metadata group in the gossip network. As new nodes join, the leader will ensure the metadata group adds enough members to keep it available in case of a node failure.

This is a one-time setup that only needs to be done when the cluster is being created for the first time. On following restarts, the Gitalys can look in their storages to determine whether they are members of the metadata group and follow the process of announcing their locations to form a quorum.

#### Adding a Node

Adding a new Gitaly node to the cluster is easy.

After setting up the Gitaly node, the administrator starts it up and provides it with an address of at least one existing cluster member. The Gitaly joins the gossip network through one of the provided cluster members. It then waits until it receives gossip about the location of the metadata group. Once it receives it, it contacts the metadata group to get storage IDs allocated for its storages. It persists the storage IDs in the storages and contacts the metadata group again to tell that it is serving the given storages. Once the metadata group has acknowledged persisting the information where the storages are being served from, the Gitaly node is ready to serve traffic. Every other Gitaly node can now find the storage through the metadata group and the new node can find all other repositories and storages in the cluster through the metadata group.

#### Rejoining a Previous Node

The rejoining process of a node that already was part of the cluster is similar to the scenario where a new node is joining. A node that previously was part of the cluster already has IDs allocated for its storages.

A rejoining node joins the gossip network through one of its existing members. When it hears the location of the metadata group, it contacts it to record which storages it is serving. Once the metadata has been updated with the new addresses of the storages, the rejoining node is ready to serve traffic.

It could be that a rejoining node was part of the metadata group. The node will know this by looking into the storages for persisted state of the metadata group. If it finds it, it will announce the storage containing the metadata state on the gossip network. It also listens for incoming gossip from the other nodes that contain storages hosting the metadata group. If it finds the members, it connects to them and attempts elect a leader on the group.

There may also be an already elected leader of the metadata group. If it hears the gossip of a member of the group, it will connect to the rejoined node and inform it about being the leader as per the usual Raft methods.

#### Removing a Node Temporarily

A node may be removed temporarily from the cluster by just shutting it down. The scenario is overall pretty much the same as if the node crashed. If the node is shutting down, it should handle in-flight requests gracefully but stop accepting new requests. The clients should not rely on this though and should have logic to retry requests that failed. Raft guarantees the consistency of the repository data, so there's nothing special to keep in mind with that. If this is a crash, the health checks performed by the gossip network will notice the node being gone and disseminate information of this node being down. If the node is shutting down, it should gossip information that it's leaving the network so the extra round of failing health checks is not needed.

#### Removing a Node Permanently

If the node is dropped off the gossip network for a long enough time, it should be considered permanently gone. The cluster should relocate all of the replicas to the other nodes to maintain their desired replication factor. The administrator may also manually want to trigger this process to decommission a node.





# Scenario Walkthroughs

There are a number of independent components in the cluster and getting tying various aspects together can be difficult. Below are walkthroughs of number of scenarios that show

## Create Repository




#### Create Repository



#### Read from Repository

#### Write into Repository

#### Delete Repository



#### Log Storage




#### Optimizing Raft

Raft relies on heartbeats























```protobuf
package gitaly

// LogEntry represents a single log entry in the Raft log of a repository.
message LogEntry {
    // term is the Raft election term during which this LogEntry was created.
    int64 term = 1;

    message UpdateReferences {
        message ReferenceUpdate {
            bytes reference_name = 1;
            string old_oid       = 2;
            string new_oid       = 3;
        }

        repeated ReferenceUpdate reference_updates = 1;
    }

    message

    oneof operation {
        UpdateReferences update_references = 2;
    }
}
```

#### Read flow



### GARBAGE COLLECTION




-----


## What to do?

https://www.git-scm.com/docs/reftable


# Replication

# CRUD

# Raft

## Active and Inactive Groups

### Repository Creation

### Repository Deletion

# Reads

# Writes

# Routing

## Client-side Routing

## Server-side Routing

# Node Discovery

# Sharding




- Complications of SWIM
  - Partitions by joining from different members
  - Needing at least one member to join

===

Q: What to do with object pooling?

## Alternatives
