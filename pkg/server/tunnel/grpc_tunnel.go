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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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

	logger klog.Logger

	agentID         string
	address         string
	_dialID         int64
	tunnelUID       string // tunnel UID is generated on tunnel creation, and used for tracing logs
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
	uid := uuid.New().String()
	logger := klog.NewKlogr().WithValues("tunnelUID", uid)
	return &grpcTunnel{
		frontend: connection{
			GrpcClient: frontend,
			logger:     logger.WithName("frontend"),
		},
		backend: connection{
			GrpcClient: backend,
			logger:     logger.WithName("backend").WithValues("agentID", agentID),
		},
		logger:    logger,
		_dialID:   dialID,
		agentID:   agentID,
		startTime: time.Now(),
		tunnelUID: uid,
	}
}

func (t *grpcTunnel) RecvFrontend(pkt *client.Packet) {
	switch pkt.Type {
	case client.PacketType_DIAL_REQ:
		if !t.transitionState(TunnelUninitialized, TunnelDialing) {
			t.logger.Error(nil, "DIAL_REQ received for tunnel in wrong state", "state", t.state)
			return
		}
		t.frontend.setState(ConnectionDialing)
		t.backend.setState(ConnectionDialing)
		if err := t.backend.Send(pkt); err != nil {
			t.logger.Error(err, "DIAL_REQ to Backend failed", "dialID", t.dialID())
			t.BackendClosed() // Initial dial failed, treat the backend as closed.
		} else {
			t.logger.V(5).Info("DIAL_REQ sent to backend", "dialID", t.dialID(), "agentID", t.agentID)
		}

	case client.PacketType_DIAL_CLS:
		if s := t.getState(); s != TunnelDialing {
			t.logger.Error(nil, "Received DIAL_CLS from frontend in unexpected state. Proceeding to shutdown anyway.",
				"state", s,
				"dialID", t.dialID())
		}
		t.setState(TunnelShutdown)
		t.frontend.setState(ConnectionClosing)
		t.backend.close()

	case client.PacketType_CLOSE_REQ:
		if s := t.getState(); s != TunnelConnected {
			t.logger.Error(nil, "Received CLOSE_REQ from frontend in unexpected state. Proceeding to shutdown anyway.",
				"state", s,
				"connectID", t.connectID())
		}
		t.setState(TunnelShutdown)
		t.frontend.setState(ConnectionClosing)
		t.backend.setState(ConnectionClosing)
		t.backend.Send(pkt)

	case client.PacketType_DATA:
		if s := t.getState(); s != TunnelConnected {
			t.logger.V(5).Info("Ignoring DATA packet from frontend: invalid state.",
				"state", s,
				"connectID", t.connectID())
			return
		}
		if err := t.backend.Send(pkt); err != nil {
			t.logger.Error(err, "Failed to deliver DATA packet to backend",
				"connectID", t.connectID(),
				"agentID", t.agentID)
		}

	case client.PacketType_DIAL_RSP, client.PacketType_CLOSE_RSP:
		t.logger.V(2).Info("Invalid packet from frontend", "type", pkt.Type.String())

	default:
		t.logger.V(5).Info("Ignore unknown packet coming from frontend",
			"type", pkt.Type.String(), "dialID", t.dialID(), "connectID", t.connectID())
	}
}

func (t *grpcTunnel) RecvBackend(pkt *client.Packet) {
	switch pkt.Type {
	case client.PacketType_DIAL_RSP:
		if s := t.getState(); s != TunnelDialing {
			t.logger.Error(nil, fmt.Sprintf("DIAL_RSP received in state %q", s.String()))
			// How we handle this situation depends on whether it was an error response.
		}
		rsp := pkt.GetDialResponse()
		if t.connectID() == 0 && rsp.ConnectID != 0 {
			t.setConnectID(rsp.ConnectID)
		} else if t.connectID() != rsp.ConnectID {
			t.logger.Error(nil, "Received DIAL_RSP with mismatched connection ID",
				"state", t.getState(),
				"tunnelConnectID", t.connectID(),
				"responseConnectID", rsp.ConnectID,
				"agentID", t.agentID)
			// Something went very wrong here. Best to just shutdown.
			t.setState(TunnelShutdown)
			t.backend.close()
			t.frontend.close()
			return
		}

		if rsp.Error != "" {
			t.handleDialError(pkt)
		} else {
			t.handleDialSuccess(pkt)
		}

	case client.PacketType_DIAL_CLS:
		if s := t.getState(); s != TunnelDialing {
			t.logger.Error(nil, "Received DIAL_CLS from backend in unexpected state. Proceeding to shutdown anyway.",
				"state", s,
				"dialID", t.dialID(),
				"agentID", t.agentID)
		}
		t.setState(TunnelShutdown)
		t.backend.setState(ConnectionClosing)
		t.frontend.close()

	case client.PacketType_CLOSE_RSP:
		if s := t.getState(); s != TunnelConnected {
			t.logger.Error(nil, "Received CLOSE_RSP from backend in unexpected state. Proceeding to shutdown anyway.",
				"state", s,
				"connectID", t.connectID(),
				"agentID", t.agentID)
		}
		t.setState(TunnelShutdown)
		t.backend.setState(ConnectionClosing)
		t.frontend.setState(ConnectionClosing)
		if err := t.frontend.Send(pkt); err != nil {
			t.logger.Error(err, "Failed to deliver CLOSE_RSP to frontend",
				"connectID", t.connectID())
		}

	case client.PacketType_DATA:
		if s := t.getState(); s != TunnelConnected {
			t.logger.V(5).Info("Ignoring DATA packet from backend: invalid state.",
				"state", s,
				"connectID", t.connectID())
			return
		}
		if err := t.frontend.Send(pkt); err != nil {
			t.logger.Error(err, "Failed to deliver DATA packet to frontend",
				"connectID", t.connectID(),
				"agentID", t.agentID)
		}

	case client.PacketType_DIAL_REQ, client.PacketType_CLOSE_REQ:
		t.logger.V(2).Info("Invalid packet from backend", "type", pkt.Type.String())

	default:
		t.logger.V(5).Info("Ignore unknown packet coming from backend",
			"type", pkt.Type.String(), "dialID", t.dialID(), "connectID", t.connectID(), "agentID", t.agentID)
	}
}

func (t *grpcTunnel) handleDialError(pkt *client.Packet) {
	t.setState(TunnelShutdown)

	rsp := pkt.GetDialResponse()
	t.logger.Error(errors.New(rsp.Error), "DIAL_RSP contains failure", "dialID", t.dialID(), "agentID", t.agentID)
	t.backend.setState(ConnectionClosing)

	if t.frontend.getState() == ConnectionClosing {
		// Frontend is already shutting down, nothing more to do.
		return
	}

	if err := t.frontend.Send(pkt); err != nil {
		t.logger.Error(err, "DIAL_RSP send to frontend stream failure", "dialID", t.dialID())
		t.frontend.close() // Last ditch effort to close the frontend.
	}
	t.frontend.setState(ConnectionClosing)
}

func (t *grpcTunnel) handleDialSuccess(pkt *client.Packet) {
	t.setState(TunnelConnected)
	t.backend.setState(ConnectionEstablished)

	if err := t.frontend.Send(pkt); err != nil {
		t.logger.Error(err, "DIAL_RSP send to frontend stream failure", "dialID", t.dialID())
		// Failed to establish the connection, so we need to shutdown.
		t.setState(TunnelShutdown)
		t.frontend.close() // Last ditch effort to close the frontend.
		t.backend.close()
		return
	}
	t.frontend.setState(ConnectionEstablished)
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

func (t *grpcTunnel) getState() TunnelState {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.state
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
	logger klog.Logger
	state  ConnectionState
	id     connID
}

func (c *connection) setState(s ConnectionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
}

func (c *connection) getState() ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
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
		// FIXME - CLOSE_REQ for backend, CLOSE_RSP for frontend :(
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

	if err := c.Send(closeReq); err != nil {
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
