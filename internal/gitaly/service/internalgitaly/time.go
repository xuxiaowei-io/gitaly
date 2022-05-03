//go:build darwin
// +build darwin

package internalgitaly

import (
	"os"
	"syscall"
	"time"
)

func getCtime(fi os.FileInfo) time.Time {
	sysStat := fi.Sys().(*syscall.Stat_t)
	ctime := time.Unix(sysStat.Ctimespec.Sec, sysStat.Ctimespec.Nsec)

	return ctime
}
