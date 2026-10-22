//go:build unit

package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBillingRecoveryUpdateCacheIsBoundToFork(t *testing.T) {
	cache := &updateServiceCacheStub{}
	svc := NewUpdateService(cache, &updateServiceGitHubClientStub{}, "0.2.17", "release")
	svc.saveToCache(context.Background(), &UpdateInfo{LatestVersion: "0.2.17000101"})
	require.Contains(t, cache.data, `"repository":"fjiangming/sub2api"`)
	info, err := svc.getFromCache(context.Background())
	require.NoError(t, err)
	require.True(t, info.HasUpdate)
	cache.data = `{"latest":"99.9.9","timestamp":9999999999,"repository":"Wei-Shaw/sub2api"}`
	_, err = svc.getFromCache(context.Background())
	require.ErrorContains(t, err, "channel changed")
}
