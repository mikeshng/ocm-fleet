package kubernetes

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// An enrollment bundle is the authorization a cluster's side presents
// with its credential. It is created by an authenticated user, names
// what it covers (one cluster, or the clusters a registered verifier
// vouches for within one scope), and carries the choices that user
// made for the clusters enrolled with it. Its secret is shown once, in
// the generated manifest; only a hash of it is kept, in the vault.

// Bundle routes: how the access piece reaches the cluster. The route
// is information for people; verification does not depend on it.
const (
	// BundleRouteManifest: someone applies the generated manifest to
	// the cluster.
	BundleRouteManifest = "manifest"
	// BundleRouteAddon: an installer the cluster already trusts (for
	// example a hub's add-on mechanism) applies it.
	BundleRouteAddon = "addon"
)

// defaultClusterBundleTTL is how long a bundle that names one cluster
// stays valid when its creator sets no expiry.
const defaultClusterBundleTTL = 24 * time.Hour

// MaxBundleTTL is the longest a bundle may stay valid when its creator
// sets the expiry.
const MaxBundleTTL = 30 * 24 * time.Hour

// BundleScope says which clusters a bundle may enroll. Exactly one of
// the two forms is set: a verifier with its scope, or one cluster.
type BundleScope struct {
	// Verifier names a registered [PresentationVerifier] that must
	// vouch for every cluster presented under the bundle.
	Verifier string `json:"verifier,omitempty"`
	// VerifierScope is the verifier's own scope (for a registry of
	// clusters, the registry's id).
	VerifierScope string `json:"verifierScope,omitempty"`
	// ClusterID and APIServer name the single cluster the bundle
	// covers. The API address is the bundle creator's statement of
	// where that cluster is; the resource manager probes only there.
	ClusterID string `json:"clusterId,omitempty"`
	APIServer string `json:"apiServer,omitempty"`
}

// BundleObservation is the observation payload of a bundle record.
type BundleObservation struct {
	ID                    string            `json:"id"`
	Route                 string            `json:"route"`
	Scope                 BundleScope       `json:"scope"`
	Profile               string            `json:"profile"`
	ClusterRole           string            `json:"clusterRole,omitempty"`
	GrantPlatformDelivery bool              `json:"grantPlatformDelivery"`
	Labels                map[string]string `json:"labels,omitempty"`
	TokenLifetimeSeconds  int64             `json:"tokenLifetimeSeconds"`
	CreatedBy             string            `json:"createdBy"`
	CreatedAt             time.Time         `json:"createdAt"`
	ExpiresAt             *time.Time        `json:"expiresAt,omitempty"`
	Uses                  int64             `json:"uses"`
	LastUsedAt            *time.Time        `json:"lastUsedAt,omitempty"`
}

// TokenLifetime returns the credential lifetime the bundle asks for.
func (b BundleObservation) TokenLifetime() time.Duration {
	if b.TokenLifetimeSeconds <= 0 {
		return DefaultTokenLifetime
	}
	return time.Duration(b.TokenLifetimeSeconds) * time.Second
}

// CreateBundleRequest is the input of [EnrollmentService.CreateBundle].
type CreateBundleRequest struct {
	// ID is the id the creator chose for the bundle, empty when the
	// server generates one. A create that names an existing bundle
	// answers the conflict instead of a second secret, so a client that
	// is unsure whether a create landed can retry it safely.
	ID                    string
	Route                 string
	Scope                 BundleScope
	Profile               string
	ClusterRole           string
	GrantPlatformDelivery bool
	Labels                map[string]string
	// TokenLifetime of the credential the cluster's side requests;
	// zero means [DefaultTokenLifetime].
	TokenLifetime time.Duration
	// TTL of the bundle itself. Zero means the default: one day for a
	// bundle that names one cluster, no expiry for a verifier-scoped
	// bundle (it stays in use while its installer keeps running).
	TTL time.Duration
	// Actor is the authenticated subject creating the bundle.
	Actor string
}

// kubernetesNamePattern matches a name usable for the RBAC objects the
// access piece refers to.
var kubernetesNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

// bundleIDPattern is the shape of a bundle id, whether the server
// generated it or its creator chose it: the routes that name a bundle
// by id check the shape, so an id outside it could neither be
// presented nor revoked.
var bundleIDPattern = regexp.MustCompile(`^eb-[0-9a-f]{12}$`)

func bundleRecordName(id string) domain.ResourceName {
	return domain.ResourceName(string(EnrollmentBundleCollectionID) + "/" + id)
}

func bundleSecretRef(id string) domain.SecretRef {
	return domain.SecretRef("enrollment/bundles/" + id + "/secret")
}

func hashBundleSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return []byte(hex.EncodeToString(sum[:]))
}

// NormalizeAccessProfile validates an access profile and its custom
// role, applying the default profile when none is given.
func NormalizeAccessProfile(profile, clusterRole, defaultProfile string) (string, string, error) {
	if profile == "" {
		profile = defaultProfile
		if clusterRole != "" {
			profile = AccessProfileCustom
		}
	}
	switch profile {
	case AccessProfileClusterAdmin, AccessProfileWorkloads, AccessProfileReadOnly:
		if clusterRole != "" {
			return "", "", fmt.Errorf("%w: a cluster role can only be named with profile %q", domain.ErrInvalidArgument, AccessProfileCustom)
		}
	case AccessProfileCustom:
		if clusterRole == "" || len(clusterRole) > 253 || !kubernetesNamePattern.MatchString(clusterRole) {
			return "", "", fmt.Errorf("%w: profile %q needs the name of an existing ClusterRole", domain.ErrInvalidArgument, AccessProfileCustom)
		}
	default:
		return "", "", fmt.Errorf("%w: unknown access profile %q (use %s, %s, %s, or %s)", domain.ErrInvalidArgument,
			profile, AccessProfileClusterAdmin, AccessProfileWorkloads, AccessProfileReadOnly, AccessProfileCustom)
	}
	return profile, clusterRole, nil
}

// CreateBundle records a new bundle and returns it together with the
// bearer value ("<id>.<secret>") the cluster's side presents. The
// bearer value is returned only here.
func (s *EnrollmentService) CreateBundle(ctx context.Context, req CreateBundleRequest) (BundleObservation, string, error) {
	scope := req.Scope
	scope.APIServer = NormalizeAPIServerURL(scope.APIServer)
	route := req.Route
	switch {
	case scope.Verifier != "":
		if scope.ClusterID != "" || scope.APIServer != "" {
			return BundleObservation{}, "", fmt.Errorf("%w: a bundle covers either one cluster or a verifier's scope, not both", domain.ErrInvalidArgument)
		}
		if scope.VerifierScope == "" {
			return BundleObservation{}, "", fmt.Errorf("%w: verifier scope is required", domain.ErrInvalidArgument)
		}
		if s.verifier(scope.Verifier) == nil {
			return BundleObservation{}, "", fmt.Errorf("%w: no verifier named %q is registered", domain.ErrInvalidArgument, scope.Verifier)
		}
		if route == "" {
			route = BundleRouteAddon
		}
	default:
		if err := ValidateClusterID(scope.ClusterID); err != nil {
			return BundleObservation{}, "", err
		}
		if err := ValidateAPIServerURL(scope.APIServer); err != nil {
			return BundleObservation{}, "", err
		}
		if route == "" {
			route = BundleRouteManifest
		}
	}
	if route != BundleRouteManifest && route != BundleRouteAddon {
		return BundleObservation{}, "", fmt.Errorf("%w: unknown bundle route %q", domain.ErrInvalidArgument, route)
	}
	profile, clusterRole, err := NormalizeAccessProfile(req.Profile, req.ClusterRole, AccessProfileWorkloads)
	if err != nil {
		return BundleObservation{}, "", err
	}
	if err := validateLabels(req.Labels); err != nil {
		return BundleObservation{}, "", err
	}
	lifetime := req.TokenLifetime
	if lifetime == 0 {
		lifetime = DefaultTokenLifetime
	}
	if lifetime < MinTokenLifetime {
		return BundleObservation{}, "", fmt.Errorf("%w: token lifetime must be at least %s", domain.ErrInvalidArgument, MinTokenLifetime)
	}
	if lifetime > MaxTokenLifetime {
		return BundleObservation{}, "", fmt.Errorf("%w: token lifetime must be at most %s", domain.ErrInvalidArgument, MaxTokenLifetime)
	}
	if req.TTL < 0 {
		return BundleObservation{}, "", fmt.Errorf("%w: bundle lifetime must not be negative", domain.ErrInvalidArgument)
	}
	if req.TTL > MaxBundleTTL {
		return BundleObservation{}, "", fmt.Errorf("%w: bundle lifetime must be at most %s", domain.ErrInvalidArgument, MaxBundleTTL)
	}

	if scope.ClusterID != "" {
		// Fail now rather than when the cluster presents itself.
		if err := s.checkClusterIDFree(ctx, scope.ClusterID); err != nil {
			return BundleObservation{}, "", err
		}
	}

	id := req.ID
	if id != "" && !bundleIDPattern.MatchString(id) {
		return BundleObservation{}, "", fmt.Errorf("%w: bundle id %q must look like eb- followed by 12 lowercase hexadecimal digits", domain.ErrInvalidArgument, id)
	}
	secretBytes := make([]byte, 32)
	if _, err := io.ReadFull(s.random, secretBytes); err != nil {
		return BundleObservation{}, "", fmt.Errorf("generate bundle secret: %w", err)
	}
	if id == "" {
		idBytes := make([]byte, 6)
		if _, err := io.ReadFull(s.random, idBytes); err != nil {
			return BundleObservation{}, "", fmt.Errorf("generate bundle id: %w", err)
		}
		id = "eb-" + hex.EncodeToString(idBytes)
	}
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)

	now := s.now().UTC()
	obs := BundleObservation{
		ID:                    id,
		Route:                 route,
		Scope:                 scope,
		Profile:               profile,
		ClusterRole:           clusterRole,
		GrantPlatformDelivery: req.GrantPlatformDelivery,
		Labels:                req.Labels,
		TokenLifetimeSeconds:  int64(lifetime / time.Second),
		CreatedBy:             req.Actor,
		CreatedAt:             now,
	}
	ttl := req.TTL
	if ttl == 0 && scope.Verifier == "" {
		ttl = defaultClusterBundleTTL
	}
	if ttl > 0 {
		expires := now.Add(ttl)
		obs.ExpiresAt = &expires
	}

	m := s.lockBundle(id)
	defer s.unlockBundle(id, m)
	// The id must name no bundle yet. A create that names an existing
	// bundle answers the conflict and leaves that bundle alone, secret
	// included: a retried create must never leave the cluster that holds
	// the first answer with a secret the vault no longer accepts, and must
	// never mint a second working secret.
	switch _, err := s.GetBundle(ctx, id); {
	case err == nil:
		return BundleObservation{}, "", bundleIDInUse(id)
	case !errors.Is(err, domain.ErrNotFound):
		return BundleObservation{}, "", err
	}
	if err := s.vault.Put(ctx, bundleSecretRef(id), hashBundleSecret(secret)); err != nil {
		return BundleObservation{}, "", fmt.Errorf("store bundle secret: %w", err)
	}
	if err := s.writeBundle(ctx, obs); err != nil {
		_ = s.vault.Delete(ctx, bundleSecretRef(id))
		return BundleObservation{}, "", err
	}
	s.logger.Info("enrollment bundle created", "bundle", id, "route", route, "by", req.Actor)
	return obs, id + "." + secret, nil
}

// bundleIDInUse answers a create whose id names an existing bundle. The
// answer names the bundle so a client that is unsure whether its create
// landed can read the one that did, but it carries no secret: only the
// create that made a bundle ever does.
func bundleIDInUse(id string) *EnrollmentError {
	e := enrollmentErr(ErrEnrollmentConflict, "bundle-id-in-use",
		"enrollment bundle %q already exists: revoke it first, or create with another bundle id", id)
	e.Details = map[string]any{"name": string(bundleRecordName(id))}
	return e
}

// GetBundle returns a bundle record.
func (s *EnrollmentService) GetBundle(ctx context.Context, id string) (BundleObservation, error) {
	if !bundleIDPattern.MatchString(id) {
		return BundleObservation{}, fmt.Errorf("%w: enrollment bundle %q", domain.ErrNotFound, id)
	}
	res, err := s.readRecord(ctx, EnrollmentBundleResourceType, bundleRecordName(id))
	if err != nil {
		return BundleObservation{}, err
	}
	var obs BundleObservation
	if err := decodeObservation(res, &obs); err != nil {
		return BundleObservation{}, fmt.Errorf("enrollment bundle %q: %w", id, err)
	}
	return obs, nil
}

// ListBundles returns every bundle record.
func (s *EnrollmentService) ListBundles(ctx context.Context) ([]BundleObservation, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.RollbackOnExit()
	resources, err := tx.ExtensionResources().ListByResourceType(ctx, EnrollmentBundleResourceType)
	if err != nil {
		return nil, fmt.Errorf("list enrollment bundles: %w", err)
	}
	out := make([]BundleObservation, 0, len(resources))
	for _, res := range resources {
		var obs BundleObservation
		if err := decodeObservation(res, &obs); err != nil {
			s.logger.Warn("skipping unreadable enrollment bundle record", "name", string(res.Name()), "error", err)
			continue
		}
		out = append(out, obs)
	}
	return out, nil
}

// A bundleLock is one bundle record's mutex and the number of callers
// that hold or wait for it. The entry is removed from the service's map
// when the last caller releases it, so the map is bounded by the
// changes in flight, not by every bundle id ever named: an id that
// names no bundle costs at most the time its request spends holding
// it.
type bundleLock struct {
	mu   sync.Mutex
	refs int
}

// lockBundle serializes every change to one bundle record and counts
// the caller. Every call must be paired with [unlockBundle].
func (s *EnrollmentService) lockBundle(id string) *bundleLock {
	s.mu.Lock()
	l, ok := s.bundleLocks[id]
	if !ok {
		l = &bundleLock{}
		s.bundleLocks[id] = l
	}
	l.refs++
	s.mu.Unlock()
	l.mu.Lock()
	return l
}

// unlockBundle releases the record's mutex and drops the map entry when
// no caller holds or waits for it. The reference count is taken under
// s.mu before a caller waits, so an entry is never removed under a
// waiter: a new caller either joins this entry or finds none.
func (s *EnrollmentService) unlockBundle(id string, l *bundleLock) {
	l.mu.Unlock()
	s.mu.Lock()
	l.refs--
	if l.refs == 0 {
		delete(s.bundleLocks, id)
	}
	s.mu.Unlock()
}

// RevokeBundle removes a bundle and its secret. Clusters already
// enrolled with it stay enrolled; nothing new can be presented with it.
func (s *EnrollmentService) RevokeBundle(ctx context.Context, id, actor string) error {
	if !bundleIDPattern.MatchString(id) {
		return fmt.Errorf("%w: enrollment bundle %q", domain.ErrNotFound, id)
	}
	// Refuse an id that names no bundle before taking the bundle's
	// lock: the lock serializes revocation against use recording of a
	// stored bundle, and a caller sending random ids must not make the
	// lock map hold them. The check under the lock stays authoritative;
	// this one only keeps ids that name nothing out of the map.
	if _, err := s.GetBundle(ctx, id); err != nil {
		return err
	}
	m := s.lockBundle(id)
	defer s.unlockBundle(id, m)
	if _, err := s.GetBundle(ctx, id); err != nil {
		return err
	}
	if err := s.vault.Delete(ctx, bundleSecretRef(id)); err != nil && !errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("delete bundle secret: %w", err)
	}
	if err := s.records.ReplaceBatch(ctx, EnrollmentBundleResourceType, []InventoryObjectReport{{
		Name: bundleRecordName(id), IsDelete: true,
	}}); err != nil {
		return fmt.Errorf("delete bundle record: %w", err)
	}
	s.logger.Info("enrollment bundle revoked", "bundle", id, "by", actor)
	return nil
}

// reasonBundleSecretMismatch is the answer for a well-formed bearer
// value that names an existing bundle but carries the wrong secret. It
// is the one authentication failure that means someone is guessing at
// a real bundle rather than at an id.
const reasonBundleSecretMismatch = "bundle-secret-mismatch"

// authenticateBundle checks a presented bearer value ("<id>.<secret>")
// and returns its bundle. The message is the same for every failure, so
// the answer does not carry which bundle ids exist in its sentence; the
// reason distinguishes the bundle's state (unknown, wrong secret,
// expired), per the documented contract, and bundle ids are random
// enough that guessing one is not practical.
func (s *EnrollmentService) authenticateBundle(ctx context.Context, bearer string) (BundleObservation, error) {
	denied := func(reason string) error {
		return enrollmentErr(ErrEnrollmentUnauthenticated, reason, "the enrollment bundle secret is not valid")
	}
	id, secret, ok := strings.Cut(strings.TrimSpace(bearer), ".")
	if !ok || secret == "" || !bundleIDPattern.MatchString(id) {
		return BundleObservation{}, denied("bundle-malformed")
	}
	obs, err := s.GetBundle(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return BundleObservation{}, denied("bundle-unknown")
		}
		return BundleObservation{}, err
	}
	want, err := s.vault.Get(ctx, bundleSecretRef(id))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return BundleObservation{}, denied("bundle-unknown")
		}
		return BundleObservation{}, fmt.Errorf("read bundle secret: %w", err)
	}
	if subtle.ConstantTimeCompare(want, hashBundleSecret(secret)) != 1 {
		return BundleObservation{}, denied(reasonBundleSecretMismatch)
	}
	if obs.ExpiresAt != nil && !s.now().Before(*obs.ExpiresAt) {
		return BundleObservation{}, denied("bundle-expired")
	}
	return obs, nil
}

// noteBundleUse records that a bundle enrolled or refreshed a cluster.
// The record is read again under the bundle's lock: the snapshot taken
// when the secret was checked may be stale by now, and a bundle revoked
// while the cluster was being admitted must not be written back into
// existence. Failure to record the use does not fail the enrollment.
func (s *EnrollmentService) noteBundleUse(ctx context.Context, id string) {
	m := s.lockBundle(id)
	defer s.unlockBundle(id, m)
	obs, err := s.GetBundle(ctx, id)
	if err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			s.logger.Warn("could not read enrollment bundle to record its use", "bundle", id, "error", err)
		}
		return
	}
	now := s.now().UTC()
	obs.Uses++
	obs.LastUsedAt = &now
	if err := s.writeBundle(ctx, obs); err != nil {
		s.logger.Warn("could not record enrollment bundle use", "bundle", id, "error", err)
	}
}

func (s *EnrollmentService) writeBundle(ctx context.Context, obs BundleObservation) error {
	raw, err := json.Marshal(obs)
	if err != nil {
		return fmt.Errorf("encode bundle record: %w", err)
	}
	msg := json.RawMessage(raw)
	if err := s.records.ReplaceBatch(ctx, EnrollmentBundleResourceType, []InventoryObjectReport{{
		Name:        bundleRecordName(obs.ID),
		Labels:      map[string]string{},
		Observation: &msg,
		ObservedAt:  s.now().UTC(),
	}}); err != nil {
		return fmt.Errorf("write bundle record: %w", err)
	}
	return nil
}

// defaultRandom is the randomness source for bundle ids and secrets.
var defaultRandom io.Reader = rand.Reader
