/*
Copyright 2021 SPIRE Authors.

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

package spireapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffegrpc/grpccredentials"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	EntryClient
	TrustDomainClient
	SVIDClient
	BundleClient
	io.Closer
}

type GrpcConfig struct {
	// MaxCallRecvMsgSize is the maximum message size the controller manager will receive.
	MaxCallRecvMsgSize int `json:"maxCallRecvMsgSize,omitempty"`
}

type MTLSConfig struct {
	// WorkloadAPISocketPath is the SPIRE Agent Workload API address used to source the client X509-SVID and trust bundle.
	// It is mutually exclusive with CertPath, KeyPath, and BundlePath.
	WorkloadAPISocketPath string `json:"workloadAPISocketPath,omitempty"`

	// CertPath is the PEM encoded client X509-SVID certificate chain path.
	CertPath string `json:"certPath,omitempty"`

	// KeyPath is the PEM encoded client X509-SVID private key path.
	KeyPath string `json:"keyPath,omitempty"`

	// BundlePath is the PEM encoded trust bundle path used to verify the server X509-SVID.
	BundlePath string `json:"bundlePath,omitempty"`

	// ServerSPIFFEID is the SPIFFE ID expected in the remote SPIRE Server X509-SVID.
	ServerSPIFFEID string `json:"serverSPIFFEID"`
}

func DialSocket(path string, grpcConfig *GrpcConfig) (Client, error) {
	var target string
	if filepath.IsAbs(path) {
		target = "unix://" + path
	} else {
		target = "unix:" + path
	}

	client, err := dial(target, insecure.NewCredentials(), grpcConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial API socket: %w", err)
	}

	return client, nil
}

func DialMTLS(ctx context.Context, address string, mtlsConfig *MTLSConfig, grpcConfig *GrpcConfig) (Client, error) {
	if address == "" {
		return nil, errors.New("remote SPIRE Server address is required")
	}
	if mtlsConfig == nil {
		return nil, errors.New("remote SPIRE Server mTLS configuration is required")
	}
	if mtlsConfig.ServerSPIFFEID == "" {
		return nil, errors.New("remote SPIRE Server mTLS serverSPIFFEID is required")
	}

	serverID, err := spiffeid.FromString(mtlsConfig.ServerSPIFFEID)
	if err != nil {
		return nil, fmt.Errorf("invalid remote SPIRE Server SPIFFE ID: %w", err)
	}

	creds, closer, err := getMTLSCredentials(ctx, mtlsConfig, serverID)
	if err != nil {
		return nil, err
	}

	client, err := dial(address, creds, grpcConfig)
	if err != nil {
		if closer != nil {
			_ = closer.Close()
		}
		return nil, fmt.Errorf("failed to dial remote SPIRE Server: %w", err)
	}

	if closer != nil {
		client = &clientWithCloser{Client: client, closer: closer}
	}

	return client, nil
}

func getMTLSCredentials(ctx context.Context, mtlsConfig *MTLSConfig, serverID spiffeid.ID) (credentials.TransportCredentials, io.Closer, error) {
	if mtlsConfig.WorkloadAPISocketPath != "" {
		if mtlsConfig.CertPath != "" || mtlsConfig.KeyPath != "" || mtlsConfig.BundlePath != "" {
			return nil, nil, errors.New("remote SPIRE Server mTLS workloadAPISocketPath can not be combined with certPath, keyPath, or bundlePath")
		}
		source, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(mtlsConfig.WorkloadAPISocketPath)))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Workload API X509 source: %w", err)
		}
		return grpccredentials.MTLSClientCredentials(
			source,
			source,
			tlsconfig.AuthorizeID(serverID),
		), source, nil
	}

	if mtlsConfig.CertPath == "" {
		return nil, nil, errors.New("remote SPIRE Server mTLS certPath is required")
	}
	if mtlsConfig.KeyPath == "" {
		return nil, nil, errors.New("remote SPIRE Server mTLS keyPath is required")
	}
	if mtlsConfig.BundlePath == "" {
		return nil, nil, errors.New("remote SPIRE Server mTLS bundlePath is required")
	}

	clientSVID, err := x509svid.Load(mtlsConfig.CertPath, mtlsConfig.KeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load remote SPIRE Server client X509-SVID: %w", err)
	}

	bundle, err := x509bundle.Load(serverID.TrustDomain(), mtlsConfig.BundlePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load remote SPIRE Server trust bundle: %w", err)
	}

	creds := grpccredentials.MTLSClientCredentials(
		staticX509SVIDSource{svid: clientSVID},
		bundle,
		tlsconfig.AuthorizeID(serverID),
	)
	return creds, nil, nil
}

func dial(target string, transportCredentials credentials.TransportCredentials, grpcConfig *GrpcConfig) (Client, error) {
	grpcOptions := append(getGrpcConfig(grpcConfig, transportCredentials), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	grpcClient, err := grpc.NewClient(target, grpcOptions...)
	if err != nil {
		return nil, err
	}

	return struct {
		EntryClient
		TrustDomainClient
		SVIDClient
		BundleClient
		io.Closer
	}{
		EntryClient:       NewEntryClient(grpcClient),
		TrustDomainClient: NewTrustDomainClient(grpcClient),
		SVIDClient:        NewSVIDClient(grpcClient),
		BundleClient:      NewBundleClient(grpcClient),
		Closer:            grpcClient,
	}, nil
}

func getGrpcConfig(grpcConfig *GrpcConfig, transportCredentials credentials.TransportCredentials) []grpc.DialOption {
	grpcOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
	}

	if grpcConfig != nil {
		callOptions := []grpc.CallOption{}
		if grpcConfig.MaxCallRecvMsgSize > 0 {
			callOptions = append(callOptions, grpc.MaxCallRecvMsgSize(grpcConfig.MaxCallRecvMsgSize))
		}
		if len(callOptions) > 0 {
			grpcOptions = append(grpcOptions, grpc.WithDefaultCallOptions(callOptions...))
		}
	}

	return grpcOptions
}

type staticX509SVIDSource struct {
	svid *x509svid.SVID
}

func (s staticX509SVIDSource) GetX509SVID() (*x509svid.SVID, error) {
	return s.svid, nil
}

type clientWithCloser struct {
	Client
	closer io.Closer
}

func (c *clientWithCloser) Close() error {
	return errors.Join(c.Client.Close(), c.closer.Close())
}
