package kubernetes

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// Enrollment takes in a cluster that already exists: one the resource
// manager did not create. The cluster's side adds a small access piece
// (a service account and its bindings); the resource manager verifies
// what is presented, keeps the credential in the vault, registers an
// ordinary kubernetes target, indexes the cluster, renews the
// credential, and can withdraw. Nothing else on the cluster changes.

// ClusterResourceType is the inventory-only record of a cluster taken
// in by enrollment. Its name is "clusters/{id}", the same parent the
// cluster's indexed objects nest under.
const ClusterResourceType domain.ResourceType = domain.ResourceType(AddonID) + "/Cluster"

// EnrollmentBundleResourceType is the inventory-only record of an
// enrollment bundle: the authorization a cluster's side presents.
const EnrollmentBundleResourceType domain.ResourceType = domain.ResourceType(AddonID) + "/EnrollmentBundle"

// EnrollmentBundleCollectionID is the collection of bundle records.
const EnrollmentBundleCollectionID domain.CollectionID = "enrollmentBundles"

// Target properties an enrollment writes, besides the connection
// properties in cluster_connection.go. None of them holds a secret.
const (
	// PropOrigin says how the target came to exist. Absent on targets a
	// provisioner created.
	PropOrigin = "origin"
	// PropAccessForm is how the resource manager reaches the cluster.
	PropAccessForm = "access_form"
	// PropDeliveryCredential is the declared rule for when the stored
	// credential applies intent. See [DeliveryCredentialSigned].
	PropDeliveryCredential = "delivery_credential"
	// PropDeliveryGrantBy and PropDeliveryGrantAt record who chose
	// [DeliveryCredentialPlatform] and when.
	PropDeliveryGrantBy = "delivery_grant_by"
	PropDeliveryGrantAt = "delivery_grant_at"
	// PropServiceAccount is "<namespace>/<name>" of the access account
	// the stored credential belongs to; renewal needs it.
	PropServiceAccount = "service_account"
	// PropEnrolledBy and PropEnrolledAt record the enrollment.
	PropEnrolledBy = "enrolled_by"
	PropEnrolledAt = "enrolled_at"
	// PropEnrollmentBundle is the bundle that authorized the enrollment.
	PropEnrollmentBundle = "enrollment_bundle"
	// PropEnrollmentLabels and PropExternalLabels keep the two label
	// sources apart (JSON objects) so either can change without losing
	// the other. The target's labels are their union.
	PropEnrollmentLabels = "enrollment_labels"
	PropExternalLabels   = "external_labels"
	// PropEnrollmentSource is the JSON [ClusterSource] of a cluster
	// that a registered verifier vouched for.
	PropEnrollmentSource = "enrollment_source"
	// PropAccessProfile is the access profile declared at enrollment.
	PropAccessProfile = "access_profile"

	propTrustBundle = "trust_bundle"
)

const (
	// OriginEnrolled marks a target taken in by enrollment.
	OriginEnrolled = "enrolled"
	// AccessFormDirect: the resource manager calls the cluster's API.
	AccessFormDirect = "direct"

	// DeliveryCredentialSigned: the stored credential applies only
	// intent whose signature the target's trust settings verify.
	// Unsigned intent uses the caller's own token. This is the default.
	DeliveryCredentialSigned = "signed"
	// DeliveryCredentialPlatform: an explicit, recorded grant. Intent
	// of an authenticated caller is applied with the stored credential
	// even when it is not signed.
	DeliveryCredentialPlatform = "platform"
)

// Access profiles: how much the cluster's side lets the resource
// manager do. The cluster's own bindings decide; the profile is the
// declared intent the generated access piece implements.
const (
	AccessProfileClusterAdmin = "cluster-admin"
	AccessProfileWorkloads    = "workloads"
	AccessProfileReadOnly     = "read-only"
	AccessProfileCustom       = "custom"
)

// Names of the access piece on a cluster.
const (
	// AccessNamespace is where the generated manifest puts the access
	// piece. Another installer may place it elsewhere; the service
	// account name is what an enrollment is checked against.
	AccessNamespace = "ocm-fleet-system"
	// AccessServiceAccount is the account whose token is presented.
	AccessServiceAccount = "ocm-fleet-access"
)

// ClusterState is where an enrolled cluster is in its life.
type ClusterState string

const (
	// ClusterStateEnrolled: the target exists and its credential works.
	ClusterStateEnrolled ClusterState = "enrolled"
	// ClusterStateNeedsEnrollment: the credential lapsed; the target is
	// out of placement until the cluster's side presents a new one.
	ClusterStateNeedsEnrollment ClusterState = "needs-enrollment"
	// ClusterStateWithdrawn: the cluster left, by either side's choice.
	// Only the record remains.
	ClusterStateWithdrawn ClusterState = "withdrawn"
)

// Condition types reported on a cluster record.
const (
	ClusterConditionReady           domain.ConditionType = "Ready"
	ClusterConditionEnrolled        domain.ConditionType = "Enrolled"
	ClusterConditionReachable       domain.ConditionType = "Reachable"
	ClusterConditionCredentialValid domain.ConditionType = "CredentialValid"
	ClusterConditionIndexing        domain.ConditionType = "Indexing"
)

// Token lifetimes for the access account's credential.
const (
	// DefaultTokenLifetime is requested when a bundle sets none.
	DefaultTokenLifetime = 12 * time.Hour
	// MinTokenLifetime is the shortest lifetime Kubernetes issues.
	MinTokenLifetime = 10 * time.Minute
)

// ClusterSource names who vouched for an enrolled cluster other than
// the person enrolling it: a registered verifier (for example an
// integration that watches a registry of clusters), the scope within
// that verifier, and the cluster's name there.
type ClusterSource struct {
	Verifier string `json:"verifier"`
	Scope    string `json:"scope"`
	Name     string `json:"name"`
}

// ClusterObservation is the observation payload of a cluster record.
// It holds nothing secret.
type ClusterObservation struct {
	Name                string         `json:"name"`
	State               ClusterState   `json:"state"`
	Origin              string         `json:"origin"`
	AccessForm          string         `json:"accessForm"`
	APIServer           string         `json:"apiServer"`
	CAFingerprint       string         `json:"caFingerprint,omitempty"`
	Target              string         `json:"target,omitempty"`
	KubernetesVersion   string         `json:"kubernetesVersion,omitempty"`
	ServiceAccount      string         `json:"serviceAccount,omitempty"`
	Profile             string         `json:"profile,omitempty"`
	Rights              *ClusterRights `json:"rights,omitempty"`
	DeliveryCredential  string         `json:"deliveryCredential,omitempty"`
	CredentialExpiresAt *time.Time     `json:"credentialExpiresAt,omitempty"`
	CredentialRenewedAt *time.Time     `json:"credentialRenewedAt,omitempty"`
	EnrolledAt          *time.Time     `json:"enrolledAt,omitempty"`
	EnrolledBy          string         `json:"enrolledBy,omitempty"`
	EnrollmentBundle    string         `json:"enrollmentBundle,omitempty"`
	Source              *ClusterSource `json:"source,omitempty"`
	WithdrawnAt         *time.Time     `json:"withdrawnAt,omitempty"`
	WithdrawnBy         string         `json:"withdrawnBy,omitempty"`
	WithdrawnReason     string         `json:"withdrawnReason,omitempty"`
	// SearchTerms lists the exact words plain search matches on: the
	// query language has no substring or any-value match.
	SearchTerms []string `json:"searchTerms"`
}

// ClusterRecord is a cluster record as stored: id, labels, observation.
type ClusterRecord struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Labels      map[string]string  `json:"labels"`
	Observation ClusterObservation `json:"observation"`
}

// Errors an enrollment operation can end in. Each has one meaning for
// a caller; [EnrollmentError] adds the machine-readable reason.
var (
	// ErrEnrollmentUnauthenticated: no valid bundle secret was shown.
	ErrEnrollmentUnauthenticated = errors.New("enrollment is not authorized")
	// ErrEnrollmentRefused: the bundle is valid but does not cover this
	// cluster, or the cluster was withdrawn since.
	ErrEnrollmentRefused = errors.New("enrollment refused")
	// ErrEnrollmentUnverified: what was presented does not check out
	// against the cluster itself.
	ErrEnrollmentUnverified = errors.New("enrollment could not be verified")
	// ErrEnrollmentConflict: the request conflicts with what exists.
	ErrEnrollmentConflict = errors.New("enrollment conflicts with existing state")
)

// EnrollmentError is an enrollment failure with a stable reason word
// and optional details for the caller.
type EnrollmentError struct {
	// Kind is one of the Err* values above, [domain.ErrNotFound], or
	// [domain.ErrInvalidArgument].
	Kind    error
	Reason  string
	Message string
	Details map[string]any
}

func (e *EnrollmentError) Error() string { return e.Message }
func (e *EnrollmentError) Unwrap() error { return e.Kind }

func enrollmentErr(kind error, reason, format string, args ...any) *EnrollmentError {
	return &EnrollmentError{Kind: kind, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// clusterIDPattern: lowercase DNS subdomain. Dots separate the parts of
// an id formed from a scope and a name (for example "hub1.ext").
var clusterIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// maxClusterIDLength keeps "k8s-<id>" a valid name wherever it is used.
const maxClusterIDLength = 200

// ValidateClusterID checks that id is usable as a cluster id.
func ValidateClusterID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: cluster id is required", domain.ErrInvalidArgument)
	}
	if len(id) > maxClusterIDLength {
		return fmt.Errorf("%w: cluster id is longer than %d characters", domain.ErrInvalidArgument, maxClusterIDLength)
	}
	if !clusterIDPattern.MatchString(id) {
		return fmt.Errorf("%w: cluster id %q must consist of lowercase letters, digits, '-' and '.', and start and end with a letter or digit", domain.ErrInvalidArgument, id)
	}
	return nil
}

// TargetIDForCluster returns the target id of a cluster id.
func TargetIDForCluster(clusterID string) domain.TargetID {
	return domain.TargetID("k8s-" + clusterID)
}

// ClusterRecordName returns the record name of a cluster id.
func ClusterRecordName(clusterID string) domain.ResourceName {
	return domain.ResourceName(string(ClusterCollectionID) + "/" + clusterID)
}

// clusterObjectPrefix is the collection prefix every indexed object of
// a cluster shares.
func clusterObjectPrefix(clusterID string) string {
	return string(ClusterCollectionID) + "/" + url.PathEscape(clusterID) + "/"
}

// ClusterIDFromTarget returns the cluster id a kubernetes target
// indexes under, from its cluster_resource_name property.
func ClusterIDFromTarget(target domain.TargetInfo) (string, bool) {
	raw := target.Properties()[PropClusterResourceName]
	if raw == "" {
		return "", false
	}
	name, err := ParseClusterResourceName(raw)
	if err != nil {
		return "", false
	}
	return string(name.ID()), true
}

// IsEnrolledTarget reports whether target was taken in by enrollment.
func IsEnrolledTarget(target domain.TargetInfo) bool {
	return target.Type() == TargetType && target.Properties()[PropOrigin] == OriginEnrolled
}

// ValidateAPIServerURL checks that raw is an https URL with a host and
// nothing else (no path, query, or credentials).
func ValidateAPIServerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: cluster API address: %v", domain.ErrInvalidArgument, err)
	}
	if u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("%w: cluster API address must be an https URL with a host", domain.ErrInvalidArgument)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("%w: cluster API address must not carry a path, query, or credentials", domain.ErrInvalidArgument)
	}
	return nil
}

// NormalizeAPIServerURL returns raw without a trailing slash, so that
// two spellings of one address compare equal.
func NormalizeAPIServerURL(raw string) string {
	return strings.TrimRight(strings.TrimSpace(raw), "/")
}

// CAFingerprints returns the sorted SHA-256 fingerprints
// ("sha256:<hex>") of every certificate in a PEM bundle. It fails when
// the bundle holds no certificate.
func CAFingerprints(caPEM []byte) ([]string, error) {
	var out []string
	rest := caPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: CA certificate: %v", domain.ErrInvalidArgument, err)
		}
		sum := sha256.Sum256(cert.Raw)
		out = append(out, "sha256:"+hex.EncodeToString(sum[:]))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: CA certificate: no PEM CERTIFICATE block found", domain.ErrInvalidArgument)
	}
	sort.Strings(out)
	return slices.Compact(out), nil
}

// SameCA reports whether two PEM bundles share at least one
// certificate. Bundles that cannot be parsed never match.
func SameCA(a, b []byte) bool {
	fa, err := CAFingerprints(a)
	if err != nil {
		return false
	}
	fb, err := CAFingerprints(b)
	if err != nil {
		return false
	}
	for _, f := range fa {
		if slices.Contains(fb, f) {
			return true
		}
	}
	return false
}

// firstCAFingerprint returns one fingerprint to show for a CA bundle.
func firstCAFingerprint(caPEM []byte) string {
	fps, err := CAFingerprints(caPEM)
	if err != nil || len(fps) == 0 {
		return ""
	}
	return fps[0]
}

// SearchTerms builds the exact-match word list for a named thing with
// labels: the name, its dot-separated parts, every label value, every
// "key=value", and any extra words. The result is sorted and unique.
func SearchTerms(name string, labels map[string]string, extra ...string) []string {
	set := map[string]struct{}{}
	add := func(s string) {
		if s = strings.TrimSpace(s); s != "" {
			set[s] = struct{}{}
		}
	}
	add(name)
	if strings.Contains(name, ".") {
		for _, part := range strings.Split(name, ".") {
			add(part)
		}
	}
	for k, v := range labels {
		add(v)
		if v != "" {
			add(k + "=" + v)
		}
	}
	for _, e := range extra {
		add(e)
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// mergeLabels returns the union of label sets; later sets win.
func mergeLabels(sets ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, set := range sets {
		for k, v := range set {
			out[k] = v
		}
	}
	return out
}

// labelsFromProperty decodes a JSON label map stored in a target
// property. A missing or malformed property yields an empty map.
func labelsFromProperty(raw string) map[string]string {
	out := map[string]string{}
	if raw == "" {
		return out
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return map[string]string{}
	}
	return out
}

// labelsToProperty encodes a label map for a target property.
func labelsToProperty(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	data, err := json.Marshal(labels)
	if err != nil {
		return ""
	}
	return string(data)
}

// validateLabels rejects label keys and values that cannot be used in
// placement or shown safely.
func validateLabels(labels map[string]string) error {
	for k, v := range labels {
		if k == "" || len(k) > 317 || strings.ContainsAny(k, " \t\n\r") {
			return fmt.Errorf("%w: label key %q is not valid", domain.ErrInvalidArgument, k)
		}
		if len(v) > 253 || strings.ContainsAny(v, "\n\r") {
			return fmt.Errorf("%w: value of label %q is not valid", domain.ErrInvalidArgument, k)
		}
	}
	return nil
}
