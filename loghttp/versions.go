package loghttp

import (
	"errors"
	"strings"
)

var ErrUnsupportedAPIVersion = errors.New("unsupported API version - must be /api/v1")

// Version holds a loghttp version
type Version int

// Valid Version values
const (
	VersionLegacy = Version(iota)
	VersionV1
)

// GetVersion returns the loghttp version for a given path.
func GetVersion(uri string) Version {
	if strings.Contains(strings.ToLower(uri), "/loki/api/v1") {
		return VersionV1
	}

	return VersionLegacy
}

func EnsureIsV1(v Version) error {
	if v == VersionV1 {
		return nil
	}
	return ErrUnsupportedAPIVersion
}
func EnsureHasV1(uri string) error {
	return EnsureIsV1(GetVersion(uri))
}
