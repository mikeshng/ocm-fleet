package ocm

import (
	"context"
	"errors"
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
	// Rollout says which clusters of the hub get the add-on. Nil keeps
	// the rollout of the add-on generated before for the hub, or
	// "manual" when there was none; see [ResolveRollout].
	Rollout  *EnrollmentRollout
	Endpoint kubernetesaddon.EnrollmentEndpoint
	Actor    string
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
	// Serialized with Connect and Disconnect: the hub must still be
	// connected when the bundle is written, and a disconnect that
	// removes the hub either happens before this check (so no bundle is
	// made) or after the bundle exists (so its sweep removes it).
	s.lockLifecycle(LifecycleEnrollmentAddon)
	defer s.opMu.Unlock()
	hw := s.hub(req.HubID)
	if hw == nil {
		return EnrollmentAddonResult{}, fmt.Errorf("%w: hub %q is not connected", domain.ErrNotFound, req.HubID)
	}
	if req.Endpoint.AgentImage == "" || req.Endpoint.ServerURL == "" {
		return EnrollmentAddonResult{}, fmt.Errorf("%w: this resource manager has no enrollment agent image or enrollment server address configured", domain.ErrInvalidArgument)
	}
	rollout, err := ResolveRollout(req.Rollout, hw.observation().EnrollmentRollout)
	if err != nil {
		return EnrollmentAddonResult{}, err
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
		Rollout:               rollout,
	})
	if err != nil {
		_ = s.enrollment.RevokeBundle(ctx, bundle.ID, req.Actor)
		return EnrollmentAddonResult{}, err
	}
	// The hub's record keeps the rollout, so that the next manifest is
	// made for the same clusters unless it is asked otherwise.
	hw.noteEnrollmentAddon(rollout, s.enrollmentSecrets(ctx, req.HubID))
	return EnrollmentAddonResult{Bundle: bundle, Addon: addon}, nil
}

// enrollmentSecrets counts the valid enrollment bundles that cover the
// clusters of a hub. It returns -1 when the bundles cannot be read.
func (s *Service) enrollmentSecrets(ctx context.Context, hubID string) int {
	if s.enrollment == nil {
		return 0
	}
	bundles, err := s.enrollment.ListBundles(ctx)
	if err != nil {
		s.logger.Warn("could not count the enrollment secrets of a hub", "hub", hubID, "error", err)
		return -1
	}
	now := s.now()
	n := 0
	for _, b := range bundles {
		if b.Scope.Verifier != VerifierName || b.Scope.VerifierScope != hubID {
			continue
		}
		if b.ExpiresAt != nil && !now.Before(*b.ExpiresAt) {
			continue
		}
		n++
	}
	return n
}

// HTTP serves the integration's verbs as plain JSON.
type HTTP struct {
	svc      *Service
	endpoint kubernetesaddon.EnrollmentEndpoint
	caller   kubernetesaddon.CallerFunc
	logger   *slog.Logger

	// requestBudget bounds the handlers that call out to a hub; see
	// [kubernetesaddon.LongRequestBudget].
	requestBudget time.Duration
}

// NewHTTP builds the handlers. endpoint is where enrolled clusters
// present themselves; it goes into the add-on the handlers render.
func NewHTTP(svc *Service, endpoint kubernetesaddon.EnrollmentEndpoint, caller kubernetesaddon.CallerFunc, logger *slog.Logger) *HTTP {
	if caller == nil {
		caller = func(context.Context) string { return "" }
	}
	return &HTTP{
		svc:           svc,
		endpoint:      endpoint,
		caller:        caller,
		logger:        logger.With("component", "ocm-integration-http"),
		requestBudget: kubernetesaddon.LongRequestBudget,
	}
}

// Register mounts the handlers behind the platform's authentication.
func (h *HTTP) Register(handle, _ func(pattern string, handler http.Handler)) {
	handle("GET "+APIPrefix+"/hubAccessManifest", http.HandlerFunc(h.hubAccessManifest))
	handle("GET "+APIPrefix+"/hubs/{id}/observation", http.HandlerFunc(h.hubObservation))
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

// hubObservation returns a connected hub's observation as its watch
// answers now. The hub's record is written at most every few minutes
// while only the last-heard time changes, so a view that shows that time
// reads it here; a hub that is not connected answers as the record reads
// do.
func (h *HTTP) hubObservation(w http.ResponseWriter, r *http.Request) {
	obs, err := h.svc.GetHubObservation(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		h.writeError(w, r, err)
		return
	}
	kubernetesaddon.WriteJSONResponse(w, http.StatusOK, obs)
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
	// The connection probe calls the hub; the budget makes a hub that
	// accepts the connection and then stalls yield this route's own
	// error answer before the server's write deadline.
	ctx, cancel := context.WithTimeout(r.Context(), h.requestBudget)
	defer cancel()
	rec, err := h.svc.Connect(ctx, ConnectRequest{
		HubID:       in.HubID,
		DisplayName: in.DisplayName,
		Connection:  conn,
		Actor:       h.caller(ctx),
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
	// Rollout is "manual", "every-cluster", or "placement" (with the
	// two fields that name the Placement). Left out, the rollout of the
	// add-on generated before for the hub is kept.
	Rollout            string `json:"rollout"`
	PlacementNamespace string `json:"placementNamespace"`
	PlacementName      string `json:"placementName"`
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
	case "generateEnrollmentAddon":
		var in enrollmentAddonRequestJSON
		if !kubernetesaddon.DecodeJSONBody(w, r, &in) {
			return
		}
		if err := kubernetesaddon.ValidateBundleSeconds(in.TokenLifetimeSeconds, in.TTLSeconds); err != nil {
			h.writeError(w, r, err)
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
			Rollout:               &EnrollmentRollout{Mode: in.Rollout, PlacementNamespace: in.PlacementNamespace, PlacementName: in.PlacementName},
			Endpoint:              h.endpoint,
			Actor:                 h.caller(r.Context()),
		})
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		manifest, err := kubernetesaddon.RenderManifestYAML(res.Addon.Hub)
		var enable []byte
		if err == nil && res.Addon.Enable != nil {
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
			"rollout":       res.Addon.Rollout,
			"instructions":  EnrollmentAddonInstructions(id, in.RemoveAccessWithAddon, res.Addon.Rollout),
		})
	case "adoptCluster":
		var in struct {
			// Cluster is the id of the enrolled cluster.
			Cluster string `json:"cluster"`
		}
		if !kubernetesaddon.DecodeJSONBody(w, r, &in) {
			return
		}
		name, err := h.svc.AdoptCluster(r.Context(), id, in.Cluster, h.caller(r.Context()))
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		kubernetesaddon.WriteJSONResponse(w, http.StatusOK, map[string]any{"hub": id, "cluster": in.Cluster, "managedCluster": name})
	default:
		http.NotFound(w, r)
	}
}
