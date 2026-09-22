//go:build !linux && !darwin

package dvr

import "errors"

func atomicExchangeArtifactPaths(_, _ string) error {
	return errors.New("atomic DVR artifact path exchange is unsupported on this platform")
}
