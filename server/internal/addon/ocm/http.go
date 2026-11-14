package ocm

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// APIPrefix is where the integration's handlers are mounted. Reads of
// hub records are served under the same prefix by the dynamic
// inventory API; these handlers add the verbs.
const APIPrefix = "/apis/" + string(AddonID) + "/v1alpha"

// EnrollmentAddonRequest is the input of [Service.EnrollmentAddon].
type EnrollmentAddonRequest struct {
	HubID                 string
	Profile               string
	ClusterRole           string
	GrantPlatformDelivery bool
	Labels                map[string]string
	TokenLifetime         time.Duration
	TTL                   time.Duration
	RemoveAccessWithAddon bool
	Endpoint              kubernetesaddon.EnrollmentEndpoint
	Actor                 string
}

// EnrollmentAddonResult is a new hub-scoped bundle and the add-on that
// carries it.
type EnrollmentAddonResult struct {
	Bundle kubernetesaddon.BundleObservation
	Addon  EnrollmentAddon
}

// EnrollmentAddon creates an enrollment bundle that covers the
// clusters of one connected hub and renders the add-on that a hub's
// administrator applies to roll enrollment out with the hub's own
// add-on mechanism.
func (s *Service) EnrollmentAddon(ctx context.Context, req EnrollmentAddonRequest) (EnrollmentAddonResult, error) {
	if s.enrollment == nil {
		return EnrollmentAddonResult{}, fmt.Errorf("%w: this resource manager has no enrollment capability (the kubernetes add-on is not enabled)", domain.ErrInvalidArgument)
	}
	if err := ValidateHubID(req.HubID); err != nil {
		return EnrollmentAddonResult{}, err
	}
	if s.hub(req.HubID) == nil {
		return EnrollmentAddonResult{}, fmt.Errorf("%w: hub %q is not connected", domain.ErrNotFound, req.HubID)
	}
	if req.Endpoint.AgentImage == "" || req.Endpoint.ServerURL == "" {
		return EnrollmentAddonResult{}, fmt.Errorf("%w: this resource manager has no enrollment agent image or enrollment server address configured", domain.ErrInvalidArgument)
	}
	profile, clusterRole, err := kubernetesaddon.NormalizeAccessProfile(req.Profile, req.ClusterRole, kubernetesaddon.AccessProfileWorkloads)
	if err != nil {
		return EnrollmentAddonResult{}, err
	}
	bundle, token, err := s.enrollment.CreateBundle(ctx, kubernetesaddon.CreateBundleRequest{
		Route:                 kubernetesaddon.BundleRouteAddon,
		Scope:                 kubernetesaddon.BundleScope{Verifier: VerifierName, VerifierScope: req.HubID},
		Profile:               profile,
		ClusterRole:           clusterRole,
		GrantPlatformDelivery: req.GrantPlatformDelivery,
		Labels:                req.Labels,
		TokenLifetime:         req.TokenLifetime,
		TTL:                   req.TTL,
		Actor:                 req.Actor,
	})
	if err != nil {
		return EnrollmentAddonResult{}, err
	}
	addon, err := BuildEnrollmentAddon(EnrollmentAddonOptions{
		Profile:               bundle.Profile,
		ClusterRole:           bundle.ClusterRole,
		RemoveAccessWithAddon: req.RemoveAccessWithAddon,
		Endpoint:              req.Endpoint,
		BundleToken:           token,
		TokenLifetime:         bundle.TokenLifetime(),
	})
	if err != nil {
		_ = s.enrollment.RevokeBundle(ctx, bundle.ID, req.Actor)
		return EnrollmentAddonResult{}, err
	}
	return EnrollmentAddonResult{Bundle: bundle, Addon: addon}, nil
}

// HTTP serves the integration's verbs as plain JSON.
type HTTP struct {
	svc      *Service
	endpoint kubernetesaddon.EnrollmentEndpoint
	caller   kubernetesaddon.CallerFunc
	logger   *slog.Logger
}

// NewHTTP builds the handlers. endpoint is where enrolled clusters
// present themselves; it goes into the add-on the handlers render.
func NewHTTP(svc *Service, endpoint kubernetesaddon.EnrollmentEndpoint, caller kubernetesaddon.CallerFunc, logger *slog.Logger) *HTTP {
	if caller == nil {
		caller = func(context.Context) string { return "" }
	}
	return &HTTP{svc: svc, endpoint: endpoint, caller: caller, logger: logger.With("component", "ocm-integration-http")}
}

// Register mounts the handlers behind the platform's authentication.
func (h *HTTP) Register(handle, _ func(pattern string, handler http.Handler)) {
	handle("GET "+APIPrefix+"/hubAccessManifest", http.HandlerFunc(h.hubAccessManifest))
	handle("POST "+APIPrefix+"/hubs:connect", http.HandlerFunc(h.connect))
	handle("POST "+APIPrefix+"/hubs/{idAndVerb}", http.HandlerFunc(h.hubVerb))
}

func (h *HTTP) writeError(w http.ResponseWriter, r *http.Request, err error) {
	kubernetesaddon.WriteEnrollmentError(w, r, h.logger, err)
}

// hubAccessManifest returns what a hub's administrator applies before
// the hub is connected, so that it can be read before it is used.
func (h *HTTP) hubAccessManifest(w http.ResponseWriter, r *http.Request) {
	manifest, err := kubernetesaddon.RenderManifestYAML(HubAccessManifest())
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	kubernetesaddon.WriteJSONResponse(w, http.StatusOK, map[string]any{
		"manifest":     string(manifest),
		"instructions": HubAccessInstructions(),
	})
}

type connectRequestJSON struct {
	HubID       string `json:"hubId"`
	DisplayName string `json:"displayName"`
	// Kubeconfig carries the address, CA, and token in one document;
	// otherwise the three are given one by one.
	Kubeconfig string `json:"kubeconfig"`
	APIServer  string `json:"apiServer"`
	CACert     string `json:"caCert"`
	Token      string `json:"token"`
}

func (h *HTTP) connect(w http.ResponseWriter, r *http.Request) {
	var in connectRequestJSON
	if !kubernetesaddon.DecodeJSONBody(w, r, &in) {
		return
	}
	conn := HubConnection{APIServer: in.APIServer, CACert: in.CACert, Token: in.Token}
	if in.Kubeconfig != "" {
		if in.APIServer != "" || in.CACert != "" || in.Token != "" {
			h.writeError(w, r, fmt.Errorf("%w: give either a kubeconfig or an API address, CA certificate, and token, not both", domain.ErrInvalidArgument))
			return
		}
		var err error
		if conn, err = ConnectionFromKubeconfig([]byte(in.Kubeconfig)); err != nil {
			h.writeError(w, r, err)
			return
		}
	}
	rec, err := h.svc.Connect(r.Context(), ConnectRequest{
		HubID:       in.HubID,
		DisplayName: in.DisplayName,
		Connection:  conn,
		Actor:       h.caller(r.Context()),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	kubernetesaddon.WriteJSONResponse(w, http.StatusOK, map[string]any{"hub": rec})
}

type enrollmentAddonRequestJSON struct {
	Profile               string            `json:"profile"`
	ClusterRole           string            `json:"clusterRole"`
	GrantPlatformDelivery bool              `json:"grantPlatformDelivery"`
	Labels                map[string]string `json:"labels"`
	TokenLifetimeSeconds  int64             `json:"tokenLifetimeSeconds"`
	TTLSeconds            int64             `json:"ttlSeconds"`
	RemoveAccessWithAddon bool              `json:"removeAccessWithAddon"`
}

// hubVerb serves POST .../hubs/{id}:{verb}. A path wildcard is a whole
// segment, so the verb is split off here.
func (h *HTTP) hubVerb(w http.ResponseWriter, r *http.Request) {
	id, verb, ok := strings.Cut(r.PathValue("idAndVerb"), ":")
	if !ok || id == "" {
		http.NotFound(w, r)
		return
	}
	switch verb {
	case "disconnect":
		if err := h.svc.Disconnect(r.Context(), id, h.caller(r.Context())); err != nil {
			h.writeError(w, r, err)
			return
		}
		kubernetesaddon.WriteJSONResponse(w, http.StatusOK, map[string]any{})
	case "enrollmentAddon":
		var in enrollmentAddonRequestJSON
		if !kubernetesaddon.DecodeJSONBody(w, r, &in) {
			return
		}
		res, err := h.svc.EnrollmentAddon(r.Context(), EnrollmentAddonRequest{
			HubID:                 id,
			Profile:               in.Profile,
			ClusterRole:           in.ClusterRole,
			GrantPlatformDelivery: in.GrantPlatformDelivery,
			Labels:                in.Labels,
			TokenLifetime:         time.Duration(in.TokenLifetimeSeconds) * time.Second,
			TTL:                   time.Duration(in.TTLSeconds) * time.Second,
			RemoveAccessWithAddon: in.RemoveAccessWithAddon,
			Endpoint:              h.endpoint,
			Actor:                 h.caller(r.Context()),
		})
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		manifest, err := kubernetesaddon.RenderManifestYAML(res.Addon.Hub)
		var enable []byte
		if err == nil {
			enable, err = kubernetesaddon.RenderManifestYAML([]map[string]any{res.Addon.Enable})
		}
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		kubernetesaddon.WriteJSONResponse(w, http.StatusOK, map[string]any{
			"bundle":        res.Bundle,
			"manifest":      string(manifest),
			"enableExample": string(enable),
			"instructions":  EnrollmentAddonInstructions(id, in.RemoveAccessWithAddon),
		})
	default:
		http.NotFound(w, r)
	}
}
