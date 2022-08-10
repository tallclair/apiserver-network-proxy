package tests

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/util"
)

func TestCustomALPN(t *testing.T) {
	const proto = "test-proto"
	protoUsed := int32(0)

	svr := httptest.NewUnstartedServer(http.DefaultServeMux)
	svr.TLS = &tls.Config{NextProtos: []string{proto}, MinVersion: tls.VersionTLS13}
	svr.Config.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){
		proto: func(svr *http.Server, conn *tls.Conn, handle http.Handler) {
			atomic.AddInt32(&protoUsed, 1)
		},
	}
	svr.StartTLS()

	ca, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer ca.Close()
	defer os.Remove(ca.Name())

	err = pem.Encode(ca, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: svr.TLS.Certificates[0].Certificate[0],
	})
	require.NoError(t, err)
	ca.Close()

	tlsConfig, err := util.GetClientTLSConfig(ca.Name(), "", "", "", []string{proto})
	require.NoError(t, err)

	addr := strings.TrimPrefix(svr.URL, "https://")
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	require.NoError(t, err)
	grpcClient := client.NewProxyServiceClient(conn)

	grpcClient.Proxy(context.Background())
	assert.EqualValuesf(t, 1, atomic.LoadInt32(&protoUsed), "expected custom ALPN protocol to have been used")
}
