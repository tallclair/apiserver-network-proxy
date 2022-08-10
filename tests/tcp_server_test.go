package tests

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/pkg/client"
)

func echo(conn net.Conn) {
	var data [256]byte

	for {
		n, err := conn.Read(data[:])
		if err != nil {
			klog.Info(err)
			return
		}

		_, err = conn.Write(data[:n])
		if err != nil {
			klog.Info(err)
			return
		}
	}
}

func TestEchoServer(t *testing.T) {
	ctx := context.Background()
	ln, err := net.Listen("tcp", "")
	assert.NoError(t, err)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				klog.Info(err)
				break
			}
			go echo(conn)
		}
	}()

	stopCh := make(chan struct{})
	defer close(stopCh)

	proxy, cleanup, err := runGRPCProxyServer()
	require.NoError(t, err)
	defer cleanup()

	clientset := runAgent(proxy.agent, stopCh)
	waitForConnectedServerCount(t, 1, clientset)

	// run test client
	tunnel, err := client.CreateSingleUseGrpcTunnel(ctx, proxy.front, grpc.WithInsecure())
	require.NoError(t, err)

	conn, err := tunnel.DialContext(ctx, "tcp", ln.Addr().String())
	assert.NoError(t, err)

	msg := "1234567890123456789012345"
	n, err := conn.Write([]byte(msg))
	assert.NoError(t, err)
	assert.Equalf(t, len(msg), n, "written bytes")

	var data [10]byte

	n, err = conn.Read(data[:])
	assert.NoError(t, err)
	assert.EqualValues(t, msg[:10], data[:n])

	n, err = conn.Read(data[:])
	assert.NoError(t, err)
	assert.EqualValues(t, msg[10:20], data[:n])

	msg2 := "1234567"
	n, err = conn.Write([]byte(msg2))
	assert.NoError(t, err)
	assert.Equalf(t, len(msg2), n, "written bytes")

	n, err = conn.Read(data[:])
	assert.NoError(t, err)
	assert.EqualValues(t, msg[20:], data[:n])

	n, err = conn.Read(data[:])
	assert.NoError(t, err)
	assert.EqualValues(t, msg2, data[:n])

	assert.NoError(t, conn.Close())
}
