package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// EnrollmentAPIPrefix is where the enrollment handlers are mounted. The
// reads of cluster and bundle records are served under the same prefix
// by the dynamic inventory API; these handlers add the verbs.
const EnrollmentAPIPrefix = "/apis/" + string(AddonID) + "/v1alpha"

// maxWithdrawReasonBytes bounds the free-form withdraw reason. The
// reason is kept in the record, copied into two condition messages,
// and logged, so it is bounded where it enters.
const maxWithdrawReasonBytes = 500

// LongRequestBudget bounds how long a handler that calls out to a
// cluster or a hub may work before it answers, across all of its
// requests. It sits below the server's write deadline: net/http arms
// that deadline once when the request starts, and a peer that accepts
// the connection but never answers would otherwise hold a synchronous
// handler past it, cutting the answer instead of letting the handler
// write its documented error.
const LongRequestBudget = 45 * time.Second

// CallerFunc returns the authenticated subject of a request, or "" for
// an anonymous one. Server composition supplies it, so that this
// package does not depend on how requests are authenticated.
type CallerFunc func(ctx context.Context) string

// EnrollmentEndpoint tells generated manifests where a cluster's side
// presents itself and how it verifies that address.
type EnrollmentEndpoint struct {
	// ServerURL is the resource manager's address as clusters address
	// it: the name its certificate carries and its gateway accepts.
	ServerURL string
	// ServerCA is the PEM CA that signs that address's certificate;
	// empty means the cluster's system roots.
	ServerCA []byte
	// ConnectAddress is the host:port a cluster dials instead of
	// resolving ServerURL's host, for when that name does not resolve
	// from inside the cluster. The request still names ServerURL.
	ConnectAddress string
	// AgentImage is the enrollment agent's image.
	AgentImage string
}

// EnrollmentHTTP serves the enrollment verbs as plain JSON.
type EnrollmentHTTP struct {
	svc      *EnrollmentService
	endpoint EnrollmentEndpoint
	caller   CallerFunc
	logger   *slog.Logger
	failures *failureLimiter

	// requestBudget bounds the handlers that call out to a cluster; see
	// [LongRequestBudget].
	requestBudget time.Duration
}

// NewEnrollmentHTTP builds the handlers.
func NewEnrollmentHTTP(svc *EnrollmentService, endpoint EnrollmentEndpoint, caller CallerFunc, logger *slog.Logger) *EnrollmentHTTP {
	if caller == nil {
		caller = func(context.Context) string { return "" }
	}
	return &EnrollmentHTTP{
		svc:           svc,
		endpoint:      endpoint,
		caller:        caller,
		logger:        logger.With("component", "enrollment-http"),
		failures:      newFailureLimiter(20, time.Minute),
		requestBudget: LongRequestBudget,
	}
}

// Register mounts the handlers. handle wraps a handler with the
// platform's user authentication; handlePublic does not: the one route
// mounted that way authenticates the request by its bundle secret.
func (h *EnrollmentHTTP) Register(handle, handlePublic func(pattern string, handler http.Handler)) {
	handle("POST "+EnrollmentAPIPrefix+"/clusters:enroll", http.HandlerFunc(h.enroll))
	handle("POST "+EnrollmentAPIPrefix+"/clusters/{idAndVerb}", http.HandlerFunc(h.clusterVerb))
	handle("DELETE "+EnrollmentAPIPrefix+"/clusters/{id}", http.HandlerFunc(h.forget))
	handle("POST "+EnrollmentAPIPrefix+"/enrollmentBundles", http.HandlerFunc(h.createBundle))
	handle("DELETE "+EnrollmentAPIPrefix+"/enrollmentBundles/{id}", http.HandlerFunc(h.revokeBundle))
	handle("GET "+EnrollmentAPIPrefix+"/accessManifest", http.HandlerFunc(h.accessManifest))
	handlePublic("POST "+EnrollmentAPIPrefix+"/enrollments:present", http.HandlerFunc(h.present))
}

type serviceAccountJSON struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type enrollRequestJSON struct {
	ClusterID             string             `json:"clusterId"`
	APIServer             string             `json:"apiServer"`
	CACert                string             `json:"caCert"`
	Token                 string             `json:"token"`
	ServiceAccount        serviceAccountJSON `json:"serviceAccount"`
	Labels                map[string]string  `json:"labels"`
	GrantPlatformDelivery bool               `json:"grantPlatformDelivery"`
	Profile               string             `json:"profile"`
}

func (h *EnrollmentHTTP) enroll(w http.ResponseWriter, r *http.Request) {
	var in enrollRequestJSON
	if !decodeJSONBody(w, r, &in) {
		return
	}
	// The probe calls the cluster the request names; the budget makes a
	// cluster that accepts the connection and then stalls yield this
	// route's own error answer before the server's write deadline.
	ctx, cancel := context.WithTimeout(r.Context(), h.requestBudget)
	defer cancel()
	rec, err := h.svc.Enroll(ctx, EnrollRequest{
		ClusterID:             in.ClusterID,
		APIServer:             in.APIServer,
		CACert:                []byte(in.CACert),
		Token:                 in.Token,
		ServiceAccount:        ServiceAccountRef(in.ServiceAccount),
		Labels:                in.Labels,
		GrantPlatformDelivery: in.GrantPlatformDelivery,
		Profile:               in.Profile,
		Actor:                 h.caller(ctx),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{"cluster": rec})
}

// clusterVerb serves POST .../clusters/{id}:{verb}. A path wildcard is
// a whole segment, so the verb is split off here.
func (h *EnrollmentHTTP) clusterVerb(w http.ResponseWriter, r *http.Request) {
	id, verb, ok := strings.Cut(r.PathValue("idAndVerb"), ":")
	if !ok || id == "" {
		http.NotFound(w, r)
		return
	}
	switch verb {
	case "withdraw":
		var in struct {
			Reason           string `json:"reason"`
			LeaveDeployments bool   `json:"leaveDeployments"`
		}
		if !decodeJSONBody(w, r, &in) {
			return
		}
		if len(in.Reason) > maxWithdrawReasonBytes {
			h.writeError(w, r, fmt.Errorf("%w: reason must be at most %d bytes", domain.ErrInvalidArgument, maxWithdrawReasonBytes))
			return
		}
		rec, err := h.svc.Withdraw(r.Context(), id, WithdrawRequest{
			Actor: h.caller(r.Context()), Reason: in.Reason, LeaveDeployments: in.LeaveDeployments,
		})
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		writeJSONResponse(w, http.StatusOK, map[string]any{"cluster": rec})
	case "renewCredential":
		// The renewal asks the cluster for a token; the budget bounds
		// the wait for a cluster that stalls.
		ctx, cancel := context.WithTimeout(r.Context(), h.requestBudget)
		defer cancel()
		rec, err := h.svc.RenewCredential(ctx, id)
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		writeJSONResponse(w, http.StatusOK, map[string]any{"cluster": rec})
	default:
		http.NotFound(w, r)
	}
}

func (h *EnrollmentHTTP) forget(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Forget(r.Context(), r.PathValue("id")); err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{})
}

type createBundleRequestJSON struct {
	ClusterID             string            `json:"clusterId"`
	APIServer             string            `json:"apiServer"`
	Profile               string            `json:"profile"`
	ClusterRole           string            `json:"clusterRole"`
	GrantPlatformDelivery bool              `json:"grantPlatformDelivery"`
	Labels                map[string]string `json:"labels"`
	TTLSeconds            int64             `json:"ttlSeconds"`
	TokenLifetimeSeconds  int64             `json:"tokenLifetimeSeconds"`
}

// ValidateBundleSeconds checks a requested token lifetime and bundle
// TTL, in seconds, before they are converted to durations: a value
// large enough to overflow a time.Duration would wrap to a short one.
func ValidateBundleSeconds(tokenLifetimeSeconds, ttlSeconds int64) error {
	if tokenLifetimeSeconds < 0 || tokenLifetimeSeconds > int64(MaxTokenLifetime/time.Second) {
		return fmt.Errorf("%w: tokenLifetimeSeconds must be between 0 and %d", domain.ErrInvalidArgument, int64(MaxTokenLifetime/time.Second))
	}
	if ttlSeconds < 0 || ttlSeconds > int64(MaxBundleTTL/time.Second) {
		return fmt.Errorf("%w: ttlSeconds must be between 0 and %d", domain.ErrInvalidArgument, int64(MaxBundleTTL/time.Second))
	}
	return nil
}

// createBundle creates a bundle for one cluster and returns the
// manifest that someone applies to that cluster. The bundle secret
// appears only inside the manifest.
func (h *EnrollmentHTTP) createBundle(w http.ResponseWriter, r *http.Request) {
	// The bundle id is the caller's choice, under the API's camelCase
	// query spelling (AIP-133, AIP-127). A snake_case spelling is refused
	// rather than ignored: a client that sent one would get a generated
	// id and, on a retry, a second working secret.
	if r.URL.Query().Has("bundle_id") {
		h.writeError(w, r, fmt.Errorf("%w: unknown query parameter bundle_id: create takes bundleId", domain.ErrInvalidArgument))
		return
	}
	var in createBundleRequestJSON
	if !decodeJSONBody(w, r, &in) {
		return
	}
	if err := ValidateBundleSeconds(in.TokenLifetimeSeconds, in.TTLSeconds); err != nil {
		h.writeError(w, r, err)
		return
	}
	// Check that a manifest can be generated before a bundle exists.
	profile, clusterRole, err := NormalizeAccessProfile(in.Profile, in.ClusterRole, AccessProfileWorkloads)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	if h.endpoint.AgentImage == "" || h.endpoint.ServerURL == "" {
		h.writeError(w, r, fmt.Errorf("%w: this resource manager has no enrollment agent image or enrollment server address configured", domain.ErrInvalidArgument))
		return
	}
	bundle, token, err := h.svc.CreateBundle(r.Context(), CreateBundleRequest{
		ID:                    r.URL.Query().Get("bundleId"),
		Route:                 BundleRouteManifest,
		Scope:                 BundleScope{ClusterID: in.ClusterID, APIServer: in.APIServer},
		Profile:               profile,
		ClusterRole:           clusterRole,
		GrantPlatformDelivery: in.GrantPlatformDelivery,
		Labels:                in.Labels,
		TokenLifetime:         time.Duration(in.TokenLifetimeSeconds) * time.Second,
		TTL:                   time.Duration(in.TTLSeconds) * time.Second,
		Actor:                 h.caller(r.Context()),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	piece, err := BuildAccessPiece(AccessPieceOptions{
		Profile:        bundle.Profile,
		ClusterRole:    bundle.ClusterRole,
		Mode:           AgentModeJob,
		Image:          h.endpoint.AgentImage,
		ServerURL:      h.endpoint.ServerURL,
		ServerCA:       h.endpoint.ServerCA,
		ConnectAddress: h.endpoint.ConnectAddress,
		BundleToken:    token,
		TokenLifetime:  bundle.TokenLifetime(),
	})
	var manifest []byte
	if err == nil {
		manifest, err = RenderManifestYAML(piece.All())
	}
	if err != nil {
		_ = h.svc.RevokeBundle(r.Context(), bundle.ID, h.caller(r.Context()))
		h.writeError(w, r, err)
		return
	}
	out := map[string]any{
		"name":     string(bundleRecordName(bundle.ID)),
		"bundle":   bundle,
		"manifest": string(manifest),
		"instructions": []string{
			"Apply the manifest to the cluster as its administrator: kubectl apply -f <file>",
			fmt.Sprintf("When the job %s/%s has run, the cluster is enrolled as %s with target %s.",
				AccessNamespace, enrollmentJobName, ClusterRecordName(bundle.Scope.ClusterID), TargetIDForCluster(bundle.Scope.ClusterID)),
			fmt.Sprintf("The agent's own objects can then be removed: kubectl -n %s delete job,secret,serviceaccount,role,rolebinding -l %s=%s",
				AccessNamespace, componentLabel, componentEnrollment),
			fmt.Sprintf("To withdraw from the cluster's side, delete namespace %s and the cluster role bindings labeled %s=%s.",
				AccessNamespace, partOfLabel, partOfValue),
		},
	}
	if bundle.ExpiresAt != nil {
		out["expiresAt"] = bundle.ExpiresAt
	}
	writeJSONResponse(w, http.StatusOK, out)
}

func (h *EnrollmentHTTP) revokeBundle(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.RevokeBundle(r.Context(), r.PathValue("id"), h.caller(r.Context())); err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{})
}

// accessManifest returns the lasting part of the access piece alone,
// for an enrollment by hand: a person applies it, requests the
// account's token, and calls clusters:enroll.
func (h *EnrollmentHTTP) accessManifest(w http.ResponseWriter, r *http.Request) {
	piece, err := BuildAccessPiece(AccessPieceOptions{
		Profile:     r.URL.Query().Get("profile"),
		ClusterRole: r.URL.Query().Get("clusterRole"),
		AccessOnly:  true,
	})
	var manifest []byte
	if err == nil {
		manifest, err = RenderManifestYAML(piece.Access)
	}
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{
		"manifest": string(manifest),
		"instructions": []string{
			"Apply the manifest to the cluster as its administrator: kubectl apply -f <file>",
			fmt.Sprintf("Request a credential for the access account: kubectl -n %s create token %s --duration=%s",
				AccessNamespace, AccessServiceAccount, DefaultTokenLifetime),
			"Enroll the cluster with its API address, its CA certificate, and that credential (POST " + EnrollmentAPIPrefix + "/clusters:enroll).",
		},
	})
}

type presentRequestJSON struct {
	ClusterName    string             `json:"clusterName"`
	CACert         string             `json:"caCert"`
	Token          string             `json:"token"`
	ServiceAccount serviceAccountJSON `json:"serviceAccount"`
}

// present is the one route without user authentication: a cluster's
// side shows its bundle secret as the bearer value.
//
// Failures are limited under two keys, and neither is a bundle id
// alone: a bundle id is not a secret — it is in the bundle record, and
// in the value a hub's add-on template carries — so keying failures on
// it would let anyone who can read one stop every cluster presenting
// under that bundle without knowing the secret. A presentation that
// fails before the secret checked out (missing, malformed, unknown,
// wrong, or expired) counts against the address the request came from.
// A presentation that fails after the secret checked out counts only
// against that one cluster's name under the bundle, so one cluster that
// cannot be verified does not shut out every other cluster sharing the
// bundle. Wrong secrets aimed at one existing bundle are counted and
// logged as a sign that someone is guessing, but they never deny a
// presentation.
func (h *EnrollmentHTTP) present(w http.ResponseWriter, r *http.Request) {
	bearer := bearerValue(r)
	src := sourceKey(r)
	tooMany := func() {
		w.Header().Set("Retry-After", "60")
		writeJSONResponse(w, http.StatusTooManyRequests, map[string]any{
			"error": "too many failed enrollment attempts; try again later", "reason": "rate-limited",
		})
	}
	if h.failures.blocked(src) {
		tooMany()
		return
	}
	if bearer == "" {
		h.failures.fail(src)
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeJSONResponse(w, http.StatusUnauthorized, map[string]any{
			"error": "an enrollment bundle secret is required", "reason": "bundle-missing",
		})
		return
	}
	var in presentRequestJSON
	if !decodeJSONBody(w, r, &in) {
		return
	}
	// The name becomes part of a cluster id and of the failure
	// limiter's key: refuse a name that cannot be one before either.
	if in.ClusterName != "" {
		if err := ValidateClusterID(in.ClusterName); err != nil {
			h.writeError(w, r, err)
			return
		}
	}
	id, _, _ := strings.Cut(bearer, ".")
	if !bundleIDPattern.MatchString(id) {
		id = ""
	}
	clusterKey := id + "/" + in.ClusterName
	if id != "" && h.failures.blocked(clusterKey) {
		tooMany()
		return
	}
	// The presentation probes the cluster; the budget makes a cluster
	// that accepts the connection and then stalls yield this route's own
	// error answer before the server's write deadline.
	ctx, cancel := context.WithTimeout(r.Context(), h.requestBudget)
	defer cancel()
	res, err := h.svc.Present(ctx, bearer, PresentRequest{
		ClusterName:    in.ClusterName,
		CACert:         []byte(in.CACert),
		Token:          in.Token,
		ServiceAccount: ServiceAccountRef(in.ServiceAccount),
	})
	if err != nil {
		if errors.Is(err, ErrEnrollmentUnauthenticated) {
			h.failures.fail(src)
			// Count wrong secrets aimed at a bundle that does exist,
			// and say so once a window: it is the one failure that
			// suggests someone is guessing at that bundle. The count
			// is never consulted for a refusal.
			var ee *EnrollmentError
			if id != "" && errors.As(err, &ee) && ee.Reason == reasonBundleSecretMismatch {
				if n := h.failures.fail("bundle:" + id); n == h.failures.max {
					h.logger.Warn("enrollment presentations under one bundle are failing repeatedly",
						"bundle", id, "failures", n, "window", h.failures.window)
				}
			}
		} else if id != "" {
			h.failures.fail(clusterKey)
		}
		h.writeError(w, r, err)
		return
	}
	writeJSONResponse(w, http.StatusOK, map[string]any{
		"cluster":           res.Cluster.Name,
		"target":            res.Cluster.Observation.Target,
		"state":             res.Cluster.Observation.State,
		"renewAfterSeconds": int64(res.RenewAfter / time.Second),
	})
}

func bearerValue(r *http.Request) string {
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if len(auth) > len(prefix) && strings.EqualFold(auth[:len(prefix)], prefix) {
		return strings.TrimSpace(auth[len(prefix):])
	}
	return ""
}

// sourceKey is the failure limiter's key for failures that happened
// before a secret checked out: the address the request came from. A
// bundle id will not do — it is not a secret, it is in the bundle
// record and in the value a hub's add-on template carries — so a
// stranger who can read one could use this gate to shut out every
// cluster presenting under that bundle. Behind a reverse proxy that
// does not pass the client address along, every presenter shares the
// proxy's address, and so one limit; the alternative is trusting a
// header the client controls.
func sourceKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.Unmap().String()
	}
	return "src:" + host
}

// maxRequestBodyBytes bounds a request body. The largest legitimate
// body carries a CA bundle and a token, or a kubeconfig.
const maxRequestBodyBytes = 1 << 20

// decodeJSONBody reads a JSON request body into v. An empty body
// leaves v as it is. It writes the error response itself and reports
// whether the handler may continue.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body == nil || r.ContentLength == 0 {
		return true
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSONResponse(w, http.StatusBadRequest, map[string]any{
			"error": "request body is not valid JSON for this operation: " + err.Error(), "reason": "invalid-argument",
		})
		return false
	}
	return true
}

func writeJSONResponse(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// DecodeJSONBody and WriteJSONResponse let the handlers of an add-on
// that builds on enrollment read requests and answer in the same form
// as the enrollment routes.
func DecodeJSONBody(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeJSONBody(w, r, v)
}

// WriteJSONResponse writes body as a JSON response that is not cached.
func WriteJSONResponse(w http.ResponseWriter, status int, body any) {
	writeJSONResponse(w, status, body)
}

// WriteEnrollmentError writes err as the JSON error body every
// enrollment route uses, mapping its kind to an HTTP status. Failures
// that are not the caller's are logged and answered without detail.
func WriteEnrollmentError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	status, reason := http.StatusInternalServerError, "internal"
	switch {
	case errors.Is(err, ErrEnrollmentUnauthenticated):
		status, reason = http.StatusUnauthorized, "unauthenticated"
	case errors.Is(err, ErrEnrollmentRefused):
		status, reason = http.StatusForbidden, "refused"
	case errors.Is(err, ErrEnrollmentUnverified):
		status, reason = http.StatusUnprocessableEntity, "unverified"
	case errors.Is(err, context.DeadlineExceeded):
		// The handler's request budget ran out while a peer it called
		// did not answer. The deadline is the server's, so the caller
		// gets a timeout they can retry, not an internal error.
		status, reason = http.StatusGatewayTimeout, "timeout"
	case errors.Is(err, ErrEnrollmentConflict), errors.Is(err, domain.ErrAlreadyExists):
		status, reason = http.StatusConflict, "conflict"
	case errors.Is(err, domain.ErrNotFound):
		status, reason = http.StatusNotFound, "not-found"
	case errors.Is(err, domain.ErrInvalidArgument):
		status, reason = http.StatusBadRequest, "invalid-argument"
	}
	body := map[string]any{"error": err.Error(), "reason": reason}
	var ee *EnrollmentError
	if errors.As(err, &ee) {
		if ee.Reason != "" {
			body["reason"] = ee.Reason
		}
		if len(ee.Details) > 0 {
			body["details"] = ee.Details
		}
	}
	if status == http.StatusInternalServerError {
		logger.Error("enrollment request failed", "endpoint", r.Method+" "+r.URL.Path, "error", err)
		body["error"] = "internal error"
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	writeJSONResponse(w, status, body)
}

func (h *EnrollmentHTTP) writeError(w http.ResponseWriter, r *http.Request, err error) {
	WriteEnrollmentError(w, r, h.logger, err)
}

// failureLimiter refuses further attempts for a key that failed too
// often within a window. It guards the one route that is not behind
// user authentication against guessing bundle secrets. A key names
// what the failure is attributable to — the address a request came
// from, or one cluster name under a bundle — and a key may also be kept
// only to count a kind of failure for the log; only keys the handler
// asks about with blocked() gate anything.
type failureLimiter struct {
	mu     sync.Mutex
	max    int
	window time.Duration
	now    func() time.Time
	fails  map[string][]time.Time
}

func newFailureLimiter(max int, window time.Duration) *failureLimiter {
	return &failureLimiter{max: max, window: window, now: time.Now, fails: map[string][]time.Time{}}
}

func (l *failureLimiter) recent(key string) []time.Time {
	cutoff := l.now().Add(-l.window)
	kept := l.fails[key][:0]
	for _, at := range l.fails[key] {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	if len(kept) == 0 {
		delete(l.fails, key)
		return nil
	}
	l.fails[key] = kept
	return kept
}

func (l *failureLimiter) blocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.recent(key)) >= l.max
}

// failureLimiterMaxKeys bounds the failure map. Its keys are not free
// to choose: a source key needs an address the request actually came
// from, a cluster key needs a bundle secret that checked out, and a
// bundle key needs an id that names a real bundle. The bound still
// matters for a caller with many addresses.
const failureLimiterMaxKeys = 10000

// failureLimiterLowWater is how far a full map is taken down when it
// crosses its bound. Evicting a batch pays for the scan and sort once
// per batch of new keys instead of once per failure while the map is
// full, which an unauthenticated caller could otherwise force on every
// request.
const failureLimiterLowWater = failureLimiterMaxKeys * 9 / 10

// fail records one failure for key and returns the number of failures
// the key has in the window, saturating at max+1: the handler logs when
// the count reaches max, and that happens once per fill rather than on
// every failure past it. The stored slice keeps at most max entries,
// the most recent ones, so a key that is counted but never gated
// cannot grow it without bound and a full key still releases when its
// latest failures age out.
func (l *failureLimiter) fail(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	recent := l.recent(key)
	n := len(recent) + 1
	if len(recent) >= l.max {
		copy(recent, recent[1:])
		recent[len(recent)-1] = l.now()
		n = l.max + 1
	} else {
		recent = append(recent, l.now())
	}
	l.fails[key] = recent
	l.evict()
	return n
}

// evict keeps the failure map bounded without lifting an active
// lockout while any other key can go instead. Keys whose failures have
// all left the window are dropped first, then the oldest keys, and a
// key that is currently blocked is spared until only blocked keys
// remain. Clearing the whole map would let one burst of made-up keys
// reset every lockout. When the map crosses its bound it is taken down
// to failureLimiterLowWater in the same pass, so the scan and sort are
// paid once per batch of new keys rather than on every failure past
// the bound.
func (l *failureLimiter) evict() {
	if len(l.fails) <= failureLimiterMaxKeys {
		return
	}
	cutoff := l.now().Add(-l.window)
	type entry struct {
		key     string
		last    time.Time
		stale   bool
		blocked bool
	}
	entries := make([]entry, 0, len(l.fails))
	for key, times := range l.fails {
		e := entry{key: key, blocked: len(times) >= l.max}
		for _, at := range times {
			if at.After(e.last) {
				e.last = at
			}
		}
		e.stale = !e.last.After(cutoff)
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].stale != entries[j].stale {
			return entries[i].stale
		}
		if entries[i].blocked != entries[j].blocked {
			return !entries[i].blocked
		}
		return entries[i].last.Before(entries[j].last)
	})
	for _, e := range entries {
		if len(l.fails) <= failureLimiterLowWater {
			return
		}
		delete(l.fails, e.key)
	}
}
