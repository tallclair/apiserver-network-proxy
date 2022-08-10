package tests

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/pkg/client"
)

type tcpLB struct {
	t        *testing.T
	mu       sync.RWMutex
	backends []string
}

func copy(wc io.WriteCloser, r io.Reader) {
	defer wc.Close()
	io.Copy(wc, r)
}

func (lb *tcpLB) handleConnection(in net.Conn, backend string) {
	out, err := net.Dial("tcp", backend)
	if err != nil {
		lb.t.Log(err)
		return
	}
	go copy(out, in)
	go copy(in, out)
}

func (lb *tcpLB) serve(stopCh chan struct{}) string {
	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		log.Fatalf("failed to bind: %s", err)
	}

	go func() {
		<-stopCh
		ln.Close()
	}()

	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("failed to accept: %s", err)
				continue
			}
			// go lb.handleConnection(conn, lb.randomBackend())
			back := lb.randomBackend()
			go lb.handleConnection(conn, back)
		}
	}()

	return ln.Addr().String()
}

func (lb *tcpLB) addBackend(backend string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.backends = append(lb.backends, backend)
}

func (lb *tcpLB) removeBackend(backend string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for i := range lb.backends {
		if lb.backends[i] == backend {
			lb.backends = append(lb.backends[:i], lb.backends[i+1:]...)
			return
		}
	}
}

func (lb *tcpLB) randomBackend() string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	i := rand.Intn(len(lb.backends)) /* #nosec G404 */
	return lb.backends[i]
}

const haServerCount = 3

func setupHAProxyServer(t *testing.T) ([]proxy, []func()) {
	proxy1, _, cleanup1, err := runGRPCProxyServerWithServerCount(haServerCount)
	require.NoError(t, err)

	proxy2, _, cleanup2, err := runGRPCProxyServerWithServerCount(haServerCount)
	require.NoError(t, err)

	proxy3, _, cleanup3, err := runGRPCProxyServerWithServerCount(haServerCount)
	require.NoError(t, err)
	return []proxy{proxy1, proxy2, proxy3}, []func(){cleanup1, cleanup2, cleanup3}
}

func TestBasicHAProxyServer_GRPC(t *testing.T) {
	server := httptest.NewServer(newEchoServer("hello"))
	defer server.Close()

	stopCh := make(chan struct{})
	defer close(stopCh)

	proxy, cleanups := setupHAProxyServer(t)

	lb := tcpLB{
		backends: []string{
			proxy[0].agent,
			proxy[1].agent,
			proxy[2].agent,
		},
		t: t,
	}
	lbAddr := lb.serve(stopCh)

	clientset := runAgent(lbAddr, stopCh)
	waitForConnectedServerCount(t, 3, clientset)
	require.Equalf(t, 3, clientset.ClientsCount(), "ClientsCount")

	// run test client
	testProxyServer(t, proxy[0].front, server.URL)
	testProxyServer(t, proxy[1].front, server.URL)
	testProxyServer(t, proxy[2].front, server.URL)

	t.Logf("basic HA proxy server test passed")

	// interrupt the HA server
	lb.removeBackend(proxy[0].agent)
	cleanups[0]()

	// give the agent some time to detect the disconnection
	waitForConnectedServerCount(t, 2, clientset)

	proxy4, _, cleanup4, err := runGRPCProxyServerWithServerCount(haServerCount)
	require.NoError(t, err)
	lb.addBackend(proxy4.agent)
	defer func() {
		cleanups[1]()
		cleanups[2]()
		cleanup4()
	}()

	// wait for the new server to be connected.
	waitForConnectedServerCount(t, 3, clientset)
	require.Containsf(t, []int{3, 4}, clientset.ClientsCount(), "Expected ClientsCount to be 3 or 4")

	// run test client
	testProxyServer(t, proxy[1].front, server.URL)
	testProxyServer(t, proxy[2].front, server.URL)
	testProxyServer(t, proxy4.front, server.URL)
}

func testProxyServer(t *testing.T, front string, target string) {
	ctx := context.Background()
	tunnel, err := client.CreateSingleUseGrpcTunnel(ctx, front, grpc.WithInsecure())
	require.NoError(t, err)

	c := &http.Client{
		Transport: &http.Transport{
			DialContext: tunnel.DialContext,
		},
		Timeout: 1 * time.Second,
	}

	r, err := c.Get(target)
	require.NoError(t, err)

	data, err := ioutil.ReadAll(r.Body)
	assert.NoError(t, err)

	assert.EqualValues(t, "hello", data)
}
