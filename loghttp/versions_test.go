package loghttp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GetVersion(t *testing.T) {
	require.Equal(t, VersionV1, GetVersion("/loki/api/v1/query_range"))
	require.Equal(t, VersionV1, GetVersion("/loki/api/v1/query"))
	require.Equal(t, VersionV1, GetVersion("/loki/api/v1/labels"))
	require.Equal(t, VersionV1, GetVersion("/loki/api/v1/label/{name}/values"))
	require.Equal(t, VersionV1, GetVersion("/loki/api/v1/tail"))

	require.Equal(t, VersionLegacy, GetVersion("/api/prom/query"))
	require.Equal(t, VersionLegacy, GetVersion("/api/prom/label"))
	require.Equal(t, VersionLegacy, GetVersion("/api/prom/label/{name}/values"))
	require.Equal(t, VersionLegacy, GetVersion("/api/prom/tail"))

	require.Equal(t, VersionV1, GetVersion("/LOKI/api/v1/query_range"))
	require.Equal(t, VersionV1, GetVersion("/LOKI/api/v1/query"))
	require.Equal(t, VersionV1, GetVersion("/LOKI/api/v1/labels"))
	require.Equal(t, VersionV1, GetVersion("/LOKI/api/v1/label/{name}/values"))
	require.Equal(t, VersionV1, GetVersion("/LOKI/api/v1/tail"))

	require.Equal(t, VersionLegacy, GetVersion("/API/prom/query"))
	require.Equal(t, VersionLegacy, GetVersion("/API/prom/label"))
	require.Equal(t, VersionLegacy, GetVersion("/API/prom/label/{name}/values"))
	require.Equal(t, VersionLegacy, GetVersion("/API/prom/tail"))

	require.NotEqual(t, int64(VersionV1), int64(VersionLegacy))
}
