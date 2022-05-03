//go:build linux
// +build linux

package internalgitaly

import (
	"os"
	"syscall"
	"time"
)

func getCtime(fi os.FileInfo) time.Time {
	sysStat := fi.Sys().(*syscall.Stat_t)
	ctime := time.Unix(int64(sysStat.Ctim.Sec), int64(sysStat.Ctim.Nsec))

	return ctime
}
