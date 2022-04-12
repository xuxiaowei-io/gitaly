package cgroups

// Config is a struct for cgroups config
type Config struct {
	// Mountpoint is where the cgroup filesystem is mounted, usually under /sys/fs/cgroup/
	Mountpoint string `toml:"mountpoint"`
	// HierarchyRoot is the parent cgroup under which Gitaly creates <Count> of cgroups.
	// A system administrator is expected to create such cgroup/directory under <Mountpoint>/memory
	// and/or <Mountpoint>/cpu depending on which resource is enabled. HierarchyRoot is expected to
	// be owned by the user and group Gitaly runs as.
	HierarchyRoot string       `toml:"hierarchy_root"`
	Repositories  Repositories `toml:"repositories"`
	Git           Git          `toml:"git"`
}

// Repositories configures cgroups to be created that are isolated by repository.
type Repositories struct {
	Count       uint   `toml:"count"`
	MemoryBytes int64  `toml:"memory_bytes"`
	CPUShares   uint64 `toml:"cpu_shares"`
}

type Git struct {
	Count    uint      `toml:"count"`
	Commands []Command `toml:"command"`
}

// GitCommand configures cgroups that are isolated by repository and git command.
type Command struct {
	Name        string `toml:"name"`
	MemoryBytes int64  `toml:"memory_bytes"`
	CPUShares   uint64 `toml:"cpu_shares"`
}
