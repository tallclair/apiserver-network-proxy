package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadiness(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	proxy, server, cleanup, err := runGRPCProxyServerWithServerCount(1)
	require.NoError(t, err)
	defer cleanup()

	ready, _ := server.Readiness.Ready()
	require.False(t, ready)

	clientset := runAgent(proxy.agent, stopCh)
	waitForConnectedServerCount(t, 1, clientset)

	ready, _ = server.Readiness.Ready()
	require.True(t, ready)
}
