/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tunnel

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

type GrpcClient interface {
	Send(*client.Packet) error
}

type Tunnel interface {
	RecvFrontend(*client.Packet)
	FrontendClosed()

	RecvBackend(*client.Packet)
	BackendClosed()
}

type grpcTunnel struct {
	frontend  connection
	backend   connection
	startTime time.Time

	agentID string
	address string
	_dialID int64

	atomicConnectID int64

	// stateMu guards the tunnel and connection states
	stateMu sync.RWMutex // TODO use this
	state   TunnelState
}

type TunnelState int

const (
	TunnelUninitialized TunnelState = iota
	TunnelDialing
	TunnelConnected
	TunnelShutdown
)

func (s TunnelState) String() string {
	switch s {
	case TunnelUninitialized:
		return "uninitialized"
	case TunnelDialing:
		return "dialing"
	case TunnelConnected:
		return "connected"
	case TunnelShutdown:
		return "shutdown"
	default:
		return fmt.Sprintf("unknown_%d", s)
	}
}

type ConnectionState int

const (
	ConnectionUninitialized ConnectionState = iota
	ConnectionDialing
	ConnectionEstablished
	ConnectionClosing
	ConnectionClosed
)

func (s ConnectionState) String() string {
	switch s {
	case ConnectionUninitialized:
		return "uninitialized"
	case ConnectionDialing:
		return "dialing"
	case ConnectionEstablished:
		return "established"
	case ConnectionClosing:
		return "closing"
	case ConnectionClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown_%d", s)
	}
}

func NewGrpcTunnel(frontend, backend GrpcClient, agentID string, dialID int64, address string) Tunnel {
	return &grpcTunnel{
		frontend: connection{
			GrpcClient: frontend,
			logger:     klog.NewKlogr().WithName("frontend"),
		},
		backend: connection{
			GrpcClient: backend,
			logger:     klog.NewKlogr().WithName("backend").WithValues("agentID", agentID),
		},
		_dialID:   dialID,
		agentID:   agentID,
		startTime: time.Now(),
	}
}

func (t *grpcTunnel) RecvFrontend(pkt *client.Packet) {
	switch pkt.Type {
	case client.PacketType_DIAL_REQ:
		if !t.transitionState(TunnelUninitialized, TunnelDialing) {
			klog.ErrorS(nil, "DIAL_REQ received for tunnel in wrong state", "state", t.state)
			return
		}
		t.frontend.setState(ConnectionDialing)
		t.backend.setState(ConnectionDialing)
		if err := t.backend.Send(pkt); err != nil {
			klog.ErrorS(err, "DIAL_REQ to Backend failed", "dialID", t.dialID())
			t.BackendClosed() // Initial dial failed, treat the backend as closed.
		} else {
			klog.V(5).InfoS("DIAL_REQ sent to backend", "dialID", t.dialID(), "agentID", t.agentID)
		}

		////// HEEEEEEEEEEERE

	case client.PacketType_CLOSE_REQ:
		connID := pkt.GetCloseRequest().ConnectID
		klog.V(5).InfoS("Received CLOSE_REQ", "connectionID", connID)
		if backend == nil {
			klog.V(2).InfoS("Backend has not been initialized for requested connection. Client should send a Dial Request first",
				"serverID", s.serverID, "connectionID", connID)
			continue
		}
		if err := backend.Send(pkt); err != nil {
			// TODO: retry with other backends connecting to this agent.
			klog.ErrorS(err, "CLOSE_REQ to Backend failed", "serverID", s.serverID, "connectionID", connID)
		} else {
			klog.V(5).InfoS("CLOSE_REQ sent to backend", "serverID", s.serverID, "connectionID", connID)
		}

	case client.PacketType_DIAL_CLS:
		random := pkt.GetCloseDial().Random
		klog.V(5).InfoS("Received DIAL_CLOSE", "serverID", s.serverID, "dialID", random)
		// Currently not worrying about backend as we do not have an established connection,
		s.PendingDial.Remove(random)
		klog.V(5).InfoS("Removing pending dial request", "serverID", s.serverID, "dialID", random)

	case client.PacketType_DATA:
		connID := pkt.GetData().ConnectID
		data := pkt.GetData().Data
		klog.V(5).InfoS("Received data from connection", "bytes", len(data), "connectionID", connID)
		if firstConnID == 0 {
			firstConnID = connID
		} else if firstConnID != connID {
			klog.V(5).InfoS("Data does not match first connection id", "fistConnectionID", firstConnID, "connectionID", connID)
		}

		if backend == nil {
			klog.V(2).InfoS("Backend has not been initialized for the connection. Client should send a Dial Request first", "connectionID", connID)
			continue
		}
		if err := backend.Send(pkt); err != nil {
			// TODO: retry with other backends connecting to this agent.
			klog.ErrorS(err, "DATA to Backend failed", "serverID", s.serverID, "connectionID", connID)
			continue
		}
		klog.V(5).Infoln("DATA sent to Backend")

	default:
		klog.V(5).InfoS("Ignore packet coming from frontend",
			"type", pkt.Type, "serverID", s.serverID, "connectionID", firstConnID)
	}
}

func (t *grpcTunnel) RecvBackend(pkt *client.Packet) {
	// TODO
}

func (t *grpcTunnel) FrontendClosed() {
	t.stateMu.Lock()
	t.state = TunnelShutdown
	t.stateMu.Unlock()

	t.frontend.setState(ConnectionClosed)
	t.backend.close()
}

func (t *grpcTunnel) BackendClosed() {
	t.stateMu.Lock()
	t.state = TunnelShutdown
	t.stateMu.Unlock()

	t.backend.setState(ConnectionClosed)
	t.frontend.close()
}

func (t *grpcTunnel) setState(s TunnelState) {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	t.state = s
}

func (t *grpcTunnel) transitionState(requiredState, newState TunnelState) bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	if t.state != requiredState {
		return false
	}
	t.state = newState
	return true
}

type connection struct {
	GrpcClient

	mu     sync.Mutex
	logger logr.Logger
	state  ConnectionState
	id     connID
}

func (c *connection) setState(s ConnectionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
}

func (c *connection) close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var closeReq *client.Packet
	switch c.state {
	case ConnectionDialing:
		closeReq = &client.Packet{
			Type: client.PacketType_DIAL_CLS,
			Payload: &client.Packet_CloseDial{
				CloseDial: &client.CloseDial{
					Random: c.id.dialID(),
				},
			},
		}
	case ConnectionEstablished:
		closeReq = &client.Packet{
			Type: client.PacketType_CLOSE_REQ,
			Payload: &client.Packet_CloseRequest{
				CloseRequest: &client.CloseRequest{
					ConnectID: c.id.connectID(),
				},
			},
		}
	default:
		c.logger.V(5).Info("Ignoring close: non transition state",
			"state", c.state.String(),
			"dialID", c.id.dialID(),
			"connectID", c.id.connectID())
		return
	}

	if err := c.client.Send(closeReq); err != nil {
		c.logger.Error(err, "Failed to deliver close request",
			"type", closeReq.Type,
			"dialID", c.id.dialID(),
			"connectID", c.id.connectID(),
			"state", c.state.String())
		c.state = ConnectionClosed // Assume that the connection has already been closed in this case.
		return
	}

	c.state = ConnectionClosing
}

type connID interface {
	connectID() int64
	dialID() int64
}

func (t *grpcTunnel) connectID() int64 {
	return atomic.LoadInt64(&t.atomicConnectID)
}

func (t *grpcTunnel) setConnectID(id int64) {
	atomic.StoreInt64(&t.atomicConnectID, id)
}

func (t *grpcTunnel) dialID() int64 {
	return t._dialID
}
