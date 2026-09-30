//go:build linux

package local

import (
	"os"
	"syscall"
	"time"
)

// changedBefore reports whether the file's inode last changed before t. ctime
// is updated by hard-linking and renaming, so fresh copies never look old.
func changedBefore(info os.FileInfo, t time.Time) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return info.ModTime().Before(t)
	}
	return time.Unix(st.Ctim.Sec, st.Ctim.Nsec).Before(t)
}
