package spireapi

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffegrpc/grpccredentials"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestDialMTLSWithFiles(t *testing.T) {
	td := spiffeid.RequireTrustDomainFromString("example.test")
	serverID := spiffeid.RequireFromPath(td, "/spire/server")
	clientID := spiffeid.RequireFromPath(td, "/controller/workload-cluster")

	caCert, caKey := newTestCA(t)
	serverSVID := newTestSVID(t, caCert, caKey, serverID)
	clientSVID := newTestSVID(t, caCert, caKey, clientID)

	bundle := x509bundle.FromX509Authorities(td, []*x509.Certificate{caCert})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	server := grpc.NewServer(grpc.Creds(grpccredentials.MTLSServerCredentials(
		staticX509SVIDSource{svid: serverSVID},
		bundle,
		tlsconfig.AuthorizeID(clientID),
	)))
	entryv1.RegisterEntryServer(server, &entryServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	tempDir := t.TempDir()
	certPath, keyPath := writeTestSVID(t, tempDir, clientSVID)
	bundlePath := writeTestBundle(t, tempDir, caCert)

	client, err := DialMTLS(context.Background(), listener.Addr().String(), &MTLSConfig{
		CertPath:       certPath,
		KeyPath:        keyPath,
		BundlePath:     bundlePath,
		ServerSPIFFEID: serverID.String(),
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	entries, err := client.ListEntries(ctx)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDialMTLSWithWorkloadAPI(t *testing.T) {
	td := spiffeid.RequireTrustDomainFromString("example.test")
	serverID := spiffeid.RequireFromPath(td, "/spire/server")
	clientID := spiffeid.RequireFromPath(td, "/controller/workload-cluster")

	caCert, caKey := newTestCA(t)
	serverSVID := newTestSVID(t, caCert, caKey, serverID)
	clientSVID := newTestSVID(t, caCert, caKey, clientID)

	bundle := x509bundle.FromX509Authorities(td, []*x509.Certificate{caCert})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	server := grpc.NewServer(grpc.Creds(grpccredentials.MTLSServerCredentials(
		staticX509SVIDSource{svid: serverSVID},
		bundle,
		tlsconfig.AuthorizeID(clientID),
	)))
	entryv1.RegisterEntryServer(server, &entryServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	workloadAPISocketPath := runTestWorkloadAPI(t, caCert, clientSVID)

	client, err := DialMTLS(context.Background(), listener.Addr().String(), &MTLSConfig{
		WorkloadAPISocketPath: workloadAPISocketPath,
		ServerSPIFFEID:        serverID.String(),
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	entries, err := client.ListEntries(ctx)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDialMTLSRequiresConfiguration(t *testing.T) {
	_, err := DialMTLS(context.Background(), "", &MTLSConfig{}, nil)
	require.EqualError(t, err, "remote SPIRE Server address is required")

	_, err = DialMTLS(context.Background(), "127.0.0.1:8081", nil, nil)
	require.EqualError(t, err, "remote SPIRE Server mTLS configuration is required")

	_, err = DialMTLS(context.Background(), "127.0.0.1:8081", &MTLSConfig{}, nil)
	require.EqualError(t, err, "remote SPIRE Server mTLS serverSPIFFEID is required")

	_, err = DialMTLS(context.Background(), "127.0.0.1:8081", &MTLSConfig{
		ServerSPIFFEID: "spiffe://example.test/spire/server",
	}, nil)
	require.EqualError(t, err, "remote SPIRE Server mTLS certPath is required")

	_, err = DialMTLS(context.Background(), "127.0.0.1:8081", &MTLSConfig{
		WorkloadAPISocketPath: "unix:///tmp/agent.sock",
		CertPath:              "/tmp/svid.pem",
		ServerSPIFFEID:        "spiffe://example.test/spire/server",
	}, nil)
	require.EqualError(t, err, "remote SPIRE Server mTLS workloadAPISocketPath can not be combined with certPath, keyPath, or bundlePath")
}

func newTestCA(t *testing.T) (*x509.Certificate, crypto.Signer) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	cert := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	raw, err := x509.CreateCertificate(rand.Reader, cert, cert, key.Public(), key)
	require.NoError(t, err)

	parsed, err := x509.ParseCertificate(raw)
	require.NoError(t, err)

	return parsed, key
}

func newTestSVID(t *testing.T, caCert *x509.Certificate, caKey crypto.Signer, id spiffeid.ID) *x509svid.SVID {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: id.String()},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		URIs:         []*url.URL{id.URL()},
	}

	raw, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, key.Public(), caKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(raw)
	require.NoError(t, err)

	return &x509svid.SVID{
		ID:           id,
		Certificates: []*x509.Certificate{cert},
		PrivateKey:   key,
	}
}

func writeTestSVID(t *testing.T, dir string, svid *x509svid.SVID) (string, string) {
	t.Helper()

	certPEM, keyPEM, err := svid.Marshal()
	require.NoError(t, err)

	certPath := filepath.Join(dir, "svid.pem")
	keyPath := filepath.Join(dir, "svid.key")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))

	return certPath, keyPath
}

func writeTestBundle(t *testing.T, dir string, caCert *x509.Certificate) string {
	t.Helper()

	bundlePath := filepath.Join(dir, "bundle.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw})
	require.NoError(t, os.WriteFile(bundlePath, certPEM, 0600))

	return bundlePath
}

func runTestWorkloadAPI(t *testing.T, caCert *x509.Certificate, svid *x509svid.SVID) string {
	t.Helper()

	socketPath := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	server := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(server, &testWorkloadAPIServer{
		x509SVIDResponse: newTestWorkloadAPISVIDResponse(t, caCert, svid),
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	return "unix://" + socketPath
}

func newTestWorkloadAPISVIDResponse(t *testing.T, caCert *x509.Certificate, svid *x509svid.SVID) *workload.X509SVIDResponse {
	t.Helper()

	keyDER, err := x509.MarshalPKCS8PrivateKey(svid.PrivateKey)
	require.NoError(t, err)

	return &workload.X509SVIDResponse{
		Svids: []*workload.X509SVID{
			{
				SpiffeId:    svid.ID.String(),
				X509Svid:    concatRawCerts(svid.Certificates),
				X509SvidKey: keyDER,
				Bundle:      caCert.Raw,
			},
		},
	}
}

func concatRawCerts(certs []*x509.Certificate) []byte {
	var raw []byte
	for _, cert := range certs {
		raw = append(raw, cert.Raw...)
	}
	return raw
}

type testWorkloadAPIServer struct {
	workload.UnimplementedSpiffeWorkloadAPIServer
	x509SVIDResponse *workload.X509SVIDResponse
}

func (s *testWorkloadAPIServer) FetchX509SVID(_ *workload.X509SVIDRequest, stream workload.SpiffeWorkloadAPI_FetchX509SVIDServer) error {
	if err := stream.Send(s.x509SVIDResponse); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
