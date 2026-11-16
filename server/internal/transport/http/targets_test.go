package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
	transporthttp "github.com/stolostron/ocm-fleet/server/internal/transport/http"
)

type staticTargetLister struct {
	targets []domain.TargetInfo
	err     error
}

func (l staticTargetLister) List(context.Context) ([]domain.TargetInfo, error) {
	return l.targets, l.err
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestTargetsHandler_ListsTargetsWithoutSecrets(t *testing.T) {
	lister := staticTargetLister{targets: []domain.TargetInfo{
		domain.NewTargetInfo("k8s-hub1.ext", "kubernetes", "hub1.ext", domain.TargetStateReady,
			map[string]string{"env": "dev"},
			map[string]string{
				"api_server":                "https://ext:6443",
				"ca_cert":                   "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
				"service_account_token":     "direct-credential",
				"service_account_token_ref": "targets/k8s-hub1.ext/sa-token",
				"trust_bundle":              `[{"issuer_url":"https://issuer"}]`,
				"cluster_resource_name":     "clusters/hub1.ext",
				"origin":                    "enrolled",
				"access_form":               "direct",
				"delivery_credential":       "platform",
				"something_new":             "unknown properties stay private",
			},
			[]domain.ManifestType{"kubernetes"}),
		domain.NewTargetInfo("kind-local", "kind", "Local Kind Provider", "", nil, nil, []domain.ManifestType{"api.kind.cluster"}),
	}}
	rec := httptest.NewRecorder()
	transporthttp.NewTargetsHandler(lister, discardLogger()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, transporthttp.TargetsPath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: every answer on this route is uncached", got)
	}
	body := rec.Body.String()
	for _, secret := range []string{"direct-credential", "sa-token", "BEGIN CERTIFICATE", "issuer_url", "unknown properties stay private"} {
		if strings.Contains(body, secret) {
			t.Errorf("response contains %q", secret)
		}
	}

	var out struct {
		Targets []struct {
			ID, Name, Type, State, Origin, Cluster string
			AccessForm, DeliveryCredential         string
			Labels, Properties                     map[string]string
			AcceptedManifestTypes                  []string
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Targets) != 2 || out.Targets[0].ID != "k8s-hub1.ext" || out.Targets[1].ID != "kind-local" {
		t.Fatalf("targets = %+v, want both, sorted by id", out.Targets)
	}
	enrolled := out.Targets[0]
	if enrolled.Origin != "enrolled" || enrolled.Cluster != "clusters/hub1.ext" || enrolled.AccessForm != "direct" ||
		enrolled.DeliveryCredential != "platform" || enrolled.State != "ready" || enrolled.Labels["env"] != "dev" ||
		enrolled.Properties["api_server"] != "https://ext:6443" || len(enrolled.AcceptedManifestTypes) != 1 {
		t.Errorf("enrolled target = %+v", enrolled)
	}
	provider := out.Targets[1]
	if provider.Origin != "provisioned" || provider.State != "ready" || provider.Labels == nil || provider.Properties == nil {
		t.Errorf("provider target = %+v (empty state reads as ready; maps are never null)", provider)
	}
}

func TestTargetsHandler_ListError(t *testing.T) {
	rec := httptest.NewRecorder()
	transporthttp.NewTargetsHandler(staticTargetLister{err: errors.New("db down at 10.0.0.1")}, discardLogger()).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, transporthttp.TargetsPath, nil))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "10.0.0.1") {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: the failure answer is uncached too", got)
	}
}
