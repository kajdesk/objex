//go:build !darwin

package local

import "os"

func syncFile(file *os.File) error {
	return file.Sync()
}
