package enrollmentagent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// podDir writes the files a pod finds in its service account mount.
func podDir(t *testing.T, ca []byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range map[string]string{"ca.crt": string(ca), "token": "own-pod-token\n", "namespace": "ocm-fleet-system\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

func TestInCluster_AccessTokenUsesTheTokenRequestAPI(t *testing.T) {
	var path, auth string
	var body struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Spec       struct {
			ExpirationSeconds int64 `json:"expirationSeconds"`
		} `json:"spec"`
	}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, auth = r.URL.Path, r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"kind":"TokenRequest","status":{"token":"minted-access-token","expirationTimestamp":"2026-01-01T12:00:00Z"}}`))
	}))
	defer api.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})
	source := InCluster{Dir: podDir(t, ca), APIServer: api.URL + "/"}

	token, err := source.AccessToken(context.Background(), "ocm-fleet-system", "ocm-fleet-access", 2*time.Hour)
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if token != "minted-access-token" {
		t.Errorf("token = %q", token)
	}
	if path != "/api/v1/namespaces/ocm-fleet-system/serviceaccounts/ocm-fleet-access/token" {
		t.Errorf("path = %q", path)
	}
	if auth != "Bearer own-pod-token" {
		t.Errorf("Authorization = %q, want the pod's own token", auth)
	}
	if body.Kind != "TokenRequest" || body.APIVersion != "authentication.k8s.io/v1" || body.Spec.ExpirationSeconds != 7200 {
		t.Errorf("request body = %+v", body)
	}

	if ns, err := source.Namespace(); err != nil || ns != "ocm-fleet-system" {
		t.Errorf("Namespace = %q, %v", ns, err)
	}
	if got, err := source.ClusterCA(); err != nil || string(got) != string(ca) {
		t.Errorf("ClusterCA = %q, %v", got, err)
	}
}

func TestInCluster_AccessTokenFailures(t *testing.T) {
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/serviceaccounts/empty/") {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"status":{}}`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"kind":"Status","message":"serviceaccounts \"other\" is forbidden"}`))
	}))
	defer api.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})
	source := InCluster{Dir: podDir(t, ca), APIServer: api.URL}

	_, err := source.AccessToken(context.Background(), "ns", "other", time.Hour)
	if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "is forbidden") {
		t.Errorf("forbidden request = %v, want the cluster's answer", err)
	}
	if _, err := source.AccessToken(context.Background(), "ns", "empty", time.Hour); err == nil || !strings.Contains(err.Error(), "empty token") {
		t.Errorf("empty token = %v", err)
	}

	// Outside a cluster the agent says so instead of dialing nowhere.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := (InCluster{Dir: podDir(t, ca)}).AccessToken(context.Background(), "ns", "sa", time.Hour); err == nil || !strings.Contains(err.Error(), "not running in a cluster") {
		t.Errorf("outside a cluster = %v", err)
	}
	// A CA file without a certificate is an error, not an unverified call.
	if _, err := (InCluster{Dir: podDir(t, []byte("not a certificate")), APIServer: api.URL}).AccessToken(context.Background(), "ns", "sa", time.Hour); err == nil || !strings.Contains(err.Error(), "no certificate") {
		t.Errorf("bad CA = %v", err)
	}
	if _, err := (InCluster{Dir: t.TempDir()}).Namespace(); err == nil {
		t.Errorf("Namespace without a mount succeeded")
	}
	if _, err := (InCluster{Dir: podDir(t, []byte("  "))}).ClusterCA(); err == nil {
		t.Errorf("ClusterCA with an empty file succeeded")
	}
}
