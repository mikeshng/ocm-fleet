package http

import (
	"context"
	"log/slog"
	"net/http"
	"sort"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// TargetsPath is the read-only listing of registered targets.
const TargetsPath = "/apis/open-cluster-management.io/v1alpha/targets"

// TargetLister lists registered targets.
type TargetLister interface {
	List(ctx context.Context) ([]domain.TargetInfo, error)
}

// publicTargetProperties are the target properties that describe a
// target and hold nothing secret. Everything else (CA material, trust
// settings, credential references, direct credentials) stays out of
// the listing: properties are an open map that provisioners write, so
// the listing shows what is known to be safe rather than hiding what is
// known to be secret.
var publicTargetProperties = map[string]struct{}{
	"api_server":            {},
	"cluster_resource_name": {},
	"origin":                {},
	"access_form":           {},
	"access_profile":        {},
	"delivery_credential":   {},
	"delivery_grant_by":     {},
	"delivery_grant_at":     {},
	"service_account":       {},
	"enrolled_by":           {},
	"enrolled_at":           {},
	"enrollment_bundle":     {},
	"enrollment_source":     {},
}

type targetJSON struct {
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Type                  string            `json:"type"`
	State                 string            `json:"state"`
	Labels                map[string]string `json:"labels"`
	AcceptedManifestTypes []string          `json:"acceptedManifestTypes"`
	// Origin is how the target came to exist: the "origin" property
	// when the registrar set one, otherwise "provisioned".
	Origin string `json:"origin"`
	// Cluster is the cluster record the target's objects index under.
	Cluster            string            `json:"cluster,omitempty"`
	AccessForm         string            `json:"accessForm,omitempty"`
	DeliveryCredential string            `json:"deliveryCredential,omitempty"`
	Properties         map[string]string `json:"properties"`
}

// NewTargetsHandler serves GET [TargetsPath]: every registered target
// with its labels, state, and descriptive properties.
func NewTargetsHandler(lister TargetLister, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targets, err := lister.List(r.Context())
		if err != nil {
			logger.ErrorContext(r.Context(), "list targets", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "list targets"})
			return
		}
		out := make([]targetJSON, 0, len(targets))
		for _, t := range targets {
			props := map[string]string{}
			for k, v := range t.Properties() {
				if _, ok := publicTargetProperties[k]; ok {
					props[k] = v
				}
			}
			labels := map[string]string{}
			for k, v := range t.Labels() {
				labels[k] = v
			}
			types := make([]string, 0, len(t.AcceptedManifestTypes()))
			for _, mt := range t.AcceptedManifestTypes() {
				types = append(types, string(mt))
			}
			state := string(t.State())
			if state == "" {
				state = string(domain.TargetStateReady)
			}
			origin := props["origin"]
			if origin == "" {
				origin = "provisioned"
			}
			out = append(out, targetJSON{
				ID:                    string(t.ID()),
				Name:                  t.Name(),
				Type:                  string(t.Type()),
				State:                 state,
				Labels:                labels,
				AcceptedManifestTypes: types,
				Origin:                origin,
				Cluster:               props["cluster_resource_name"],
				AccessForm:            props["access_form"],
				DeliveryCredential:    props["delivery_credential"],
				Properties:            props,
			})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, map[string]any{"targets": out})
	})
}
