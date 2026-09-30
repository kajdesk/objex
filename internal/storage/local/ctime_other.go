//go:build !darwin && !freebsd && !netbsd && !linux

package local

import (
	"os"
	"time"
)

func changedBefore(info os.FileInfo, t time.Time) bool { return info.ModTime().Before(t) }
