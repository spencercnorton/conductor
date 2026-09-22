//go:build darwin

package dvr

import "golang.org/x/sys/unix"

func atomicExchangeArtifactPaths(first, second string) error {
	return unix.RenameatxNp(
		unix.AT_FDCWD, first,
		unix.AT_FDCWD, second,
		unix.RENAME_SWAP,
	)
}
