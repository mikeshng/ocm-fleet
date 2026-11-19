package enrollmentagent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// InCluster is the [TokenSource] of an agent running in a pod. It
// talks to the cluster's API with the pod's own service account, which
// needs one right: to create a token for the access account.
type InCluster struct {
	// Dir is the service account directory; empty means the standard
	// mount.
	Dir string
	// APIServer is the cluster API's URL; empty means the address the
	// kubelet gives every pod.
	APIServer string
}

func (s InCluster) dir() string {
	if s.Dir != "" {
		return s.Dir
	}
	return serviceAccountDir
}

// ClusterCA implements [TokenSource].
func (s InCluster) ClusterCA() ([]byte, error) {
	pem, err := os.ReadFile(filepath.Join(s.dir(), "ca.crt"))
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(pem)) == 0 {
		return nil, errors.New("the cluster CA file is empty")
	}
	return pem, nil
}

// Namespace implements [TokenSource].
func (s InCluster) Namespace() (string, error) {
	raw, err := os.ReadFile(filepath.Join(s.dir(), "namespace"))
	if err != nil {
		return "", err
	}
	ns := strings.TrimSpace(string(raw))
	if ns == "" {
		return "", errors.New("the namespace file is empty")
	}
	return ns, nil
}

func (s InCluster) apiServer() (string, error) {
	if s.APIServer != "" {
		return strings.TrimRight(s.APIServer, "/"), nil
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return "", errors.New("not running in a cluster: KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT are not set")
	}
	return "https://" + net.JoinHostPort(host, port), nil
}

// AccessToken implements [TokenSource] with the TokenRequest API.
func (s InCluster) AccessToken(ctx context.Context, namespace, name string, lifetime time.Duration) (string, error) {
	api, err := s.apiServer()
	if err != nil {
		return "", err
	}
	ca, err := s.ClusterCA()
	if err != nil {
		return "", fmt.Errorf("read cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return "", errors.New("the cluster CA file holds no certificate")
	}
	// The pod's own token is rotated by the kubelet: read it each time.
	own, err := os.ReadFile(filepath.Join(s.dir(), "token"))
	if err != nil {
		return "", fmt.Errorf("read own service account token: %w", err)
	}

	body, err := json.Marshal(map[string]any{
		"apiVersion": "authentication.k8s.io/v1",
		"kind":       "TokenRequest",
		"spec":       map[string]any{"expirationSeconds": int64(lifetime / time.Second)},
	})
	if err != nil {
		return "", err
	}
	endpoint := fmt.Sprintf("%s/api/v1/namespaces/%s/serviceaccounts/%s/token", api, url.PathEscape(namespace), url.PathEscape(name))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(own)))

	client := &http.Client{
		Timeout:   requestTimeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		var status struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &status)
		if status.Message == "" {
			status.Message = http.StatusText(resp.StatusCode)
		}
		return "", fmt.Errorf("the cluster answered %d: %s", resp.StatusCode, status.Message)
	}
	var out struct {
		Status struct {
			Token string `json:"token"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("the cluster's answer is not understood: %w", err)
	}
	if out.Status.Token == "" {
		return "", errors.New("the cluster returned an empty token")
	}
	return out.Status.Token, nil
}
