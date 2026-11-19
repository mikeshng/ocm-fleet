package oidc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

const (
	// defaultFetchTimeout bounds one discovery fetch when neither the
	// caller's context nor [DiscoveryClient.FetchTimeout] sets a
	// shorter one. The discovery call runs inside durable work with no
	// timeout of its own, and an issuer that accepts the connection and
	// then stalls would otherwise block it forever.
	defaultFetchTimeout = 10 * time.Second

	// maxDiscoveryDocumentBytes bounds the document body. Provider
	// metadata is a few kilobytes; the bound keeps an oversized answer
	// from being buffered whole.
	maxDiscoveryDocumentBytes = 1 << 20
)

// DiscoveryClient implements [domain.OIDCDiscoveryClient] by fetching
// the OpenID Connect discovery document via HTTP.
type DiscoveryClient struct {
	HTTP *http.Client

	// FetchTimeout bounds one fetch. Zero uses [defaultFetchTimeout];
	// a caller's earlier deadline still wins.
	FetchTimeout time.Duration
}

// NewDiscoveryClient creates a discovery client using the given HTTP client.
// A nil client is replaced with one bounded by [defaultFetchTimeout];
// [http.DefaultClient] carries no timeout of its own.
func NewDiscoveryClient(client *http.Client) *DiscoveryClient {
	if client == nil {
		client = &http.Client{Timeout: defaultFetchTimeout}
	}
	return &DiscoveryClient{HTTP: client}
}

// discoveryDocument is the subset of OpenID Provider Metadata this client consumes.
type discoveryDocument struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
}

// FetchMetadata retrieves the OIDC discovery document from the issuer's
// well-known endpoint. The document issuer must equal issuerURL, and
// authorization, token, and JWKS endpoints are required.
func (c *DiscoveryClient) FetchMetadata(ctx context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	timeout := c.FetchTimeout
	if timeout <= 0 {
		timeout = defaultFetchTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	endpoint := string(issuerURL) + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("fetch discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery endpoint returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDiscoveryDocumentBytes+1))
	if err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("read discovery document: %w", err)
	}
	if len(body) > maxDiscoveryDocumentBytes {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery document exceeds %d bytes", maxDiscoveryDocumentBytes)
	}

	var doc discoveryDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("decode discovery document: %w", err)
	}

	if doc.Issuer != string(issuerURL) {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery issuer %q does not match requested issuer %q", doc.Issuer, issuerURL)
	}
	if doc.AuthorizationEndpoint == "" {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery document missing authorization_endpoint")
	}
	if doc.TokenEndpoint == "" {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery document missing token_endpoint")
	}
	if doc.JWKSURI == "" {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery document missing jwks_uri")
	}

	return domain.OIDCMetadata{
		Issuer:                domain.IssuerURL(doc.Issuer),
		AuthorizationEndpoint: domain.EndpointURL(doc.AuthorizationEndpoint),
		TokenEndpoint:         domain.EndpointURL(doc.TokenEndpoint),
		JWKSURI:               domain.EndpointURL(doc.JWKSURI),
	}, nil
}
