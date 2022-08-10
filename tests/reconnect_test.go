package tests

import (
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
	agentproto "sigs.k8s.io/apiserver-network-proxy/proto/agent"
	"sigs.k8s.io/apiserver-network-proxy/proto/header"
)

func TestClientReconnects(t *testing.T) {
	connections := make(chan struct{})
	s := &testAgentServerImpl{
		onConnect: func(stream agent.AgentService_ConnectServer) error {
			stream.SetHeader(metadata.New(map[string]string{
				header.ServerID:    uuid.Must(uuid.NewRandom()).String(),
				header.ServerCount: "1",
			}))
			connections <- struct{}{}
			return nil
		},
	}

	svr := grpc.NewServer()
	agentproto.RegisterAgentServiceServer(svr, s)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		if err := svr.Serve(lis); err != nil {
			panic(err)
		}
	}()

	stopCh := make(chan struct{})
	defer close(stopCh)
	runAgentWithID("test-id", lis.Addr().String(), stopCh)

	select {
	case <-connections:
		// Expected
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Timed out waiting for agent to connect")
	}
	svr.Stop()

	lis2, err := net.Listen("tcp", lis.Addr().String())
	require.NoError(t, err)
	svr2 := grpc.NewServer()
	agentproto.RegisterAgentServiceServer(svr2, s)
	go func() {
		if err := svr2.Serve(lis2); err != nil {
			panic(err)
		}
	}()
	defer svr2.Stop()

	select {
	case <-connections:
		// Expected
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Timed out waiting for agent to reconnect")
	}
}

type testAgentServerImpl struct {
	onConnect func(agent.AgentService_ConnectServer) error
}

func (t *testAgentServerImpl) Connect(svr agent.AgentService_ConnectServer) error {
	return t.onConnect(svr)
}
