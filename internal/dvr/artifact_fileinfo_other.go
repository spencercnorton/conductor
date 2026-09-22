//go:build !linux && !darwin

package dvr

import "os"

type artifactChangeVersion struct{}

func artifactFilesystemID(os.FileInfo) (string, bool) {
	return "", false
}

func artifactChangeVersionOf(os.FileInfo) (artifactChangeVersion, bool) {
	return artifactChangeVersion{}, false
}
