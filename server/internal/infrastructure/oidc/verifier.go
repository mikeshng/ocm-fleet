package oidc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

const (
	// defaultKeyRefetchInterval is how long, per JWKS URI, must pass before
	// an unknown key ID may cause another on-demand fetch. It bounds the work
	// a caller with a made-up key ID can cause, and still recovers from a
	// signing-key rotation within a minute.
	defaultKeyRefetchInterval = time.Minute

	// keyRefetchTimeout bounds one on-demand fetch, so an identity provider
	// that does not answer cannot hold an authentication request open. It
	// also bounds the fetches a registration makes.
	keyRefetchTimeout = 10 * time.Second
)

// Verifier implements [domain.OIDCTokenVerifier] using lestrrat-go/jwx.
// It manages a [jwk.Cache] internally for JWKS auto-refresh. A token that
// names a key ID the verifier does not have — the identity provider rotated
// its signing key — causes one fresh fetch and a retry, at most once per
// [defaultKeyRefetchInterval] per URI.
type Verifier struct {
	cache      *jwk.Cache
	httpClient httprc.HTTPClient // optional; nil means http.DefaultClient for preflight

	mu      sync.RWMutex
	keySets map[string]jwk.Set // jwksURI -> cached set

	// refetchMu guards lastRefetch. refetchInterval is how long an entry in
	// it blocks a new on-demand fetch; now is the clock (tests advance it).
	refetchMu       sync.Mutex
	lastRefetch     map[string]time.Time // jwksURI -> last on-demand fetch attempt
	refetchInterval time.Duration
	now             func() time.Time

	// fetchTimeout bounds one JWKS fetch, both a registration's preflight
	// and an on-demand refetch, so a provider that does not answer cannot
	// hold the caller indefinitely.
	fetchTimeout time.Duration

	// registerMu guards registerLocks. Per-URI mutexes serialize registration
	// for one JWKS URL without holding v.mu across network I/O, and without
	// sharing one caller's context across waiters (unlike singleflight).
	// registerLocks is keyed by configured auth-method JWKS URIs only (not
	// request-supplied). Entries are never removed; cardinality is bounded by
	// the number of OIDC auth methods.
	//
	// TODO: During a prolonged IdP/JWKS outage, every Verify → getKeySet →
	// RegisterJWKS for an unpublished URI re-drives jwk.Fetch under the
	// per-URI mutex, so waiters queue and then each attempt another upstream
	// fetch. Consider a short negative-result cache (few seconds) so outage
	// load is bounded, while still allowing on-demand recovery when the IdP
	// returns (cool-down may delay first success by the TTL).
	registerMu    sync.Mutex
	registerLocks map[string]*sync.Mutex
}

// VerifierOption configures a [Verifier].
type VerifierOption func(*verifierConfig)

// verifierConfig holds optional settings collected by [VerifierOption]s.
type verifierConfig struct {
	httpClient      httprc.HTTPClient
	keyRefetchEvery time.Duration
	fetchTimeout    time.Duration
	now             func() time.Time
}

// WithHTTPClient sets the HTTP client used for JWKS fetching. This is
// useful for injecting custom CA trust (e.g., self-signed TLS in tests)
// or proxy configuration.
func WithHTTPClient(c httprc.HTTPClient) VerifierOption {
	return func(cfg *verifierConfig) { cfg.httpClient = c }
}

// WithKeyRefetchInterval sets how long, per JWKS URI, must pass before a
// token naming an unknown key ID may cause another on-demand JWKS fetch.
// The default is one minute.
func WithKeyRefetchInterval(d time.Duration) VerifierOption {
	return func(cfg *verifierConfig) { cfg.keyRefetchEvery = d }
}

// WithKeyFetchTimeout sets how long one JWKS fetch may take: a
// registration's preflight fetch and its first cache fetch, or an
// on-demand refetch. An earlier deadline on the caller's context still
// wins. The default is ten seconds.
func WithKeyFetchTimeout(d time.Duration) VerifierOption {
	return func(cfg *verifierConfig) { cfg.fetchTimeout = d }
}

// WithKeyRefetchClock overrides the clock the refetch interval is measured
// with, so tests can advance it instead of waiting.
func WithKeyRefetchClock(now func() time.Time) VerifierOption {
	return func(cfg *verifierConfig) { cfg.now = now }
}

// NewVerifier creates a verifier with a background JWKS cache.
func NewVerifier(ctx context.Context, opts ...VerifierOption) (*Verifier, error) {
	var cfg verifierConfig
	for _, o := range opts {
		o(&cfg)
	}

	var clientOpts []httprc.NewClientOption
	if cfg.httpClient != nil {
		clientOpts = append(clientOpts, httprc.WithHTTPClient(cfg.httpClient))
	}
	client := httprc.NewClient(clientOpts...)
	cache, err := jwk.NewCache(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("create JWK cache: %w", err)
	}
	refetchInterval := cfg.keyRefetchEvery
	if refetchInterval <= 0 {
		refetchInterval = defaultKeyRefetchInterval
	}
	fetchTimeout := cfg.fetchTimeout
	if fetchTimeout <= 0 {
		fetchTimeout = keyRefetchTimeout
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	return &Verifier{
		cache:           cache,
		httpClient:      cfg.httpClient,
		keySets:         make(map[string]jwk.Set),
		lastRefetch:     make(map[string]time.Time),
		refetchInterval: refetchInterval,
		fetchTimeout:    fetchTimeout,
		now:             now,
		registerLocks:   make(map[string]*sync.Mutex),
	}, nil
}

// RegisterJWKS registers a JWKS URI with the background cache so keys
// are refreshed automatically. Called at process start for auth methods
// and on demand from Verify when keys were not cached at boot.
//
// Registration is fail-fast and recoverable:
//  1. Preflight fetch under ctx, bounded by the fetch timeout (surfaces
//     503/timeout immediately).
//  2. On success, register with httprc for background auto-refresh.
//  3. Publish into keySets only after the cache has a ready set.
//
// A failed attempt leaves keySets untouched and clears any partial httprc
// registration so a later call (including on-demand from Verify) can recover.
// We intentionally avoid cache.Refresh on the failure path: httprc workers
// exit on synchronous refresh failure, which permanently breaks the cache
// after a handful of down-IdP attempts.
//
// Network I/O runs outside v.mu. Same-URI callers serialize on a per-URI
// mutex and each attempt uses that caller's ctx; other URIs are not blocked.
func (v *Verifier) RegisterJWKS(ctx context.Context, jwksURI domain.EndpointURL) error {
	uri := string(jwksURI)
	if v.hasKeySet(uri) {
		return nil
	}

	uriMu := v.lockForURI(uri)
	uriMu.Lock()
	defer uriMu.Unlock()

	if v.hasKeySet(uri) {
		return nil
	}
	return v.doRegisterJWKS(ctx, uri)
}

// lockForURI returns the per-URI mutex used to serialize registration.
func (v *Verifier) lockForURI(uri string) *sync.Mutex {
	v.registerMu.Lock()
	defer v.registerMu.Unlock()
	m, ok := v.registerLocks[uri]
	if !ok {
		m = &sync.Mutex{}
		v.registerLocks[uri] = m
	}
	return m
}

// doRegisterJWKS performs preflight fetch, cache registration, and publish.
// It must not hold v.mu across network or cache I/O.
func (v *Verifier) doRegisterJWKS(ctx context.Context, uri string) error {
	// Bound the registration: the preflight fetch and the cache's own
	// first fetch both talk to the identity provider, and a caller
	// without a deadline must not wait on a provider that never answers.
	// An earlier deadline on the caller's context still applies.
	ctx, cancel := context.WithTimeout(ctx, v.fetchTimeout)
	defer cancel()

	if v.cache.IsRegistered(ctx, uri) {
		// Prior success that wasn't published yet (shouldn't happen), or a
		// stale registration left after a failed WaitReady — reuse if ready,
		// otherwise clear and continue.
		if _, err := v.cache.Lookup(ctx, uri); err == nil {
			return v.publishCachedSet(ctx, uri)
		}
		if err := v.cache.Unregister(ctx, uri); err != nil {
			return fmt.Errorf("unregister stale JWKS URI %s: %w", uri, err)
		}
	}

	// Preflight: httprc Ready() only unblocks on first *success* or ctx
	// cancel — a 503 never fails Register quickly. Fetch ourselves first.
	if _, err := jwk.Fetch(ctx, uri, v.fetchOptions()...); err != nil {
		return fmt.Errorf("fetch JWKS for %s: %w", uri, err)
	}

	// JWKS is reachable; register for auto-refresh. Default WaitReady waits
	// for httprc's own first fetch (second GET). Bound by the same ctx.
	if err := v.cache.Register(ctx, uri); err != nil {
		if v.cache.IsRegistered(ctx, uri) {
			_ = v.cache.Unregister(ctx, uri)
		}
		return fmt.Errorf("register JWKS URI %s: %w", uri, err)
	}

	return v.publishCachedSet(ctx, uri)
}

// fetchOptions returns jwk.Fetch options using the verifier's HTTP client,
// or http.DefaultClient when none was configured.
func (v *Verifier) fetchOptions() []jwk.FetchOption {
	client := v.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	return []jwk.FetchOption{jwk.WithHTTPClient(client)}
}

// hasKeySet reports whether uri is already published in keySets.
func (v *Verifier) hasKeySet(uri string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, ok := v.keySets[uri]
	return ok
}

// publishCachedSet stores a ready CachedSet in keySets. On CachedSet failure
// it unregisters the URI so a later RegisterJWKS can recover.
func (v *Verifier) publishCachedSet(ctx context.Context, uri string) error {
	cached, err := v.cache.CachedSet(uri)
	if err != nil {
		_ = v.cache.Unregister(ctx, uri)
		return fmt.Errorf("create cached set for %s: %w", uri, err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.keySets[uri]; ok {
		return nil
	}
	v.keySets[uri] = cached
	return nil
}

// Verify validates a JWT against the OIDC configuration.
func (v *Verifier) Verify(ctx context.Context, config domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	keySet, err := v.getKeySet(ctx, config.JWKSURI)
	if err != nil {
		return domain.SubjectClaims{}, fmt.Errorf("get key set: %w", err)
	}

	tok, err := parseToken(rawToken, keySet, config)
	if err != nil && isUnknownKeyID(err) {
		// The identity provider may have replaced its signing key since this
		// key set was fetched. Fetch again and retry once; refetchKeySet
		// serializes and rate-limits the fetch, so a made-up key ID cannot
		// turn every request into one.
		refetchErr := v.refetchKeySet(ctx, config.JWKSURI)
		if refetchErr == nil {
			if fresh, ok := v.lookupKeySet(string(config.JWKSURI)); ok {
				tok, err = parseToken(rawToken, fresh, config)
			}
		} else {
			err = fmt.Errorf("%w (fetching the key set again: %v)", err, refetchErr)
		}
	}
	if err != nil {
		return domain.SubjectClaims{}, fmt.Errorf("parse/verify token: %w%s",
			err, tokenDiagnostics(rawToken, config))
	}

	sub, _ := tok.Subject()
	iss, _ := tok.Issuer()

	claims := domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: domain.SubjectID(sub),
			Issuer:  domain.IssuerURL(iss),
		},
		Extra: make(map[string][]string),
	}

	var email string
	if err := tok.Get("email", &email); err == nil {
		claims.Extra["email"] = []string{email}
	}

	if groups := getStringSliceClaim(tok, "groups"); len(groups) > 0 {
		claims.Extra["groups"] = groups
	}

	var azp string
	if err := tok.Get("azp", &azp); err == nil {
		claims.Extra["azp"] = []string{azp}
	}

	return claims, nil
}

// getStringSliceClaim extracts a string-array private claim from a JWT.
// The jwx library deserializes JSON arrays as []interface{}, so a direct
// Get into *[]string fails. This helper converts element-by-element.
func getStringSliceClaim(tok jwt.Token, key string) []string {
	var raw interface{}
	if err := tok.Get(key, &raw); err != nil {
		return nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// parseToken parses and validates rawToken against keySet and the configured
// issuer and audience.
func parseToken(rawToken string, keySet jwk.Set, config domain.OIDCConfig) (jwt.Token, error) {
	parseOpts := []jwt.ParseOption{
		jwt.WithKeySet(keySet),
		jwt.WithValidate(true),
		jwt.WithAcceptableSkew(30 * time.Second),
	}
	if config.IssuerURL != "" {
		parseOpts = append(parseOpts, jwt.WithIssuer(string(config.IssuerURL)))
	}
	if config.Audience != "" {
		parseOpts = append(parseOpts, jwt.WithAudience(string(config.Audience)))
	}
	return jwt.ParseString(rawToken, parseOpts...)
}

// isUnknownKeyID reports whether err is the signature check failing because
// the token names a key ID that is not in the key set. jws v3 returns a plain
// error for this, so its text is what can be matched; a library upgrade that
// changes the text is caught by the signing-key-rotation test, which then
// fails to recover.
func isUnknownKeyID(err error) bool {
	return err != nil && strings.Contains(err.Error(), "failed to find key with key ID")
}

// tokenDiagnostics does a best-effort insecure parse of the token and returns
// a suffix string with claim values useful for debugging verification failures.
// Returns empty string if the token cannot be decoded.
func tokenDiagnostics(rawToken string, config domain.OIDCConfig) string {
	tok, err := jwt.ParseInsecure([]byte(rawToken))
	if err != nil {
		return ""
	}
	iss, _ := tok.Issuer()
	aud, _ := tok.Audience()
	return fmt.Sprintf(" [expected: iss=%q aud=%q, got: iss=%q aud=%v]",
		config.IssuerURL, config.Audience, iss, aud)
}

// getKeySet returns the cached key set for jwksURI, registering it on demand
// when it was not published at boot (IdP recovery path).
func (v *Verifier) getKeySet(ctx context.Context, jwksURI domain.EndpointURL) (jwk.Set, error) {
	uri := string(jwksURI)
	if ks, ok := v.lookupKeySet(uri); ok {
		return ks, nil
	}

	if err := v.RegisterJWKS(ctx, jwksURI); err != nil {
		return nil, err
	}

	ks, ok := v.lookupKeySet(uri)
	if !ok {
		return nil, fmt.Errorf("key set for %s missing after registration", uri)
	}
	return ks, nil
}

// lookupKeySet returns the published key set for uri under v.mu.
func (v *Verifier) lookupKeySet(uri string) (jwk.Set, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ks, ok := v.keySets[uri]
	return ks, ok
}

// refetchKeySet fetches the JWKS afresh after a token named a key this
// verifier does not have, and publishes the result. The background cache's
// own copy is not touched: a key matters only once a token uses it, so the
// next rotation is served the same way, by one fetch the first time the new
// key is seen. Fetches are serialized per URI and allowed at most once per
// refetchInterval, so callers with made-up key IDs cannot turn every request
// into a fetch.
func (v *Verifier) refetchKeySet(ctx context.Context, jwksURI domain.EndpointURL) error {
	uri := string(jwksURI)
	uriMu := v.lockForURI(uri)
	uriMu.Lock()
	defer uriMu.Unlock()

	if !v.refetchDue(uri) {
		return nil
	}

	// Detached from the caller's cancellation: this one fetch serves every
	// request that will meet the same new key, so one caller going away must
	// not consume the fetch the interval allows.
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), v.fetchTimeout)
	defer cancel()
	fetched, err := jwk.Fetch(fetchCtx, uri, v.fetchOptions()...)
	if err != nil {
		return fmt.Errorf("fetch JWKS for %s: %w", uri, err)
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.keySets[uri] = fetched
	return nil
}

// refetchDue reports whether an on-demand fetch for uri is allowed now, and
// records the attempt when it is. A failed attempt also counts: recovery
// waits for the interval rather than hammering an identity provider that is
// down.
func (v *Verifier) refetchDue(uri string) bool {
	now := v.now()
	v.refetchMu.Lock()
	defer v.refetchMu.Unlock()
	if last, ok := v.lastRefetch[uri]; ok && now.Sub(last) < v.refetchInterval {
		return false
	}
	v.lastRefetch[uri] = now
	return true
}
