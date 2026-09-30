//go:build darwin

package local

import (
	"os"

	"golang.org/x/sys/unix"
)

// Go's os.File.Sync uses F_FULLFSYNC on macOS. Blob writes use regular fsync;
// the durable bbolt metadata commit that follows performs the final full flush,
// preserving ordering while allowing many blobs to share that expensive step.
func syncFile(file *os.File) error {
	return unix.Fsync(int(file.Fd()))
}
