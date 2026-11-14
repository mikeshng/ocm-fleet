package ocm

import (
	"fmt"
	"regexp"
	"time"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// VerifierName is the name the integration's presentation verifier is
// registered under with the enrollment capability. A bundle scoped to
// a hub names it, with the hub's id as the scope.
const VerifierName = "ocm-hub"

// Labels the integration contributes to the target of an enrolled
// cluster that a hub knows, besides the hub's own labels for it.
const (
	// LabelHub is the id of the hub connection that knows the cluster.
	LabelHub = "ocm.open-cluster-management.io/hub"
	// LabelManagedCluster is the cluster's name on that hub.
	LabelManagedCluster = "ocm.open-cluster-management.io/managed-cluster"
	// ClaimLabelPrefix prefixes a cluster claim's name.
	ClaimLabelPrefix = "claim."

	clusterSetLabel = "cluster.open-cluster-management.io/clusterset"
)

// Conditions of a hub record.
const (
	HubConditionReady           domain.ConditionType = "Ready"
	HubConditionConnected       domain.ConditionType = "Connected"
	HubConditionSynced          domain.ConditionType = "Synced"
	HubConditionCredentialValid domain.ConditionType = "CredentialValid"
)

// ConditionStale is added to an observed cluster's record while its
// hub cannot be reached: everything else in the record is then the
// last state seen, not the current one.
const ConditionStale domain.ConditionType = "Stale"

// Link states: what the resource manager itself has to do with a
// cluster a hub knows.
const (
	// LinkObserved: nothing. The cluster is only seen through the hub;
	// the resource manager cannot deliver to it and does not index it.
	LinkObserved = "observed"
	// LinkEnrolled: the cluster was taken in by enrollment.
	LinkEnrolled = "enrolled"
	// LinkCreated: the resource manager created the cluster itself.
	LinkCreated = "created"
	// LinkWithdrawn: the cluster was enrolled and has been withdrawn.
	LinkWithdrawn = "withdrawn"
)

// Bases of a link, strongest first.
const (
	// BasisEnrollment: the cluster was enrolled through this hub.
	BasisEnrollment = "enrollment"
	// BasisAddressAndCA: the hub records the same API address and CA
	// as a cluster the resource manager knows.
	BasisAddressAndCA = "address-and-ca"
	// BasisName: only the name agrees, the hub records no address for
	// the cluster, and exactly one candidate exists. A weak basis; it
	// is shown so that a person can judge it.
	BasisName = "name"
)

// HubObservation is the observation payload of a hub record. It holds
// nothing secret: the connection is kept in the vault.
type HubObservation struct {
	ID                  string     `json:"id"`
	DisplayName         string     `json:"displayName,omitempty"`
	APIServer           string     `json:"apiServer"`
	CAFingerprint       string     `json:"caFingerprint,omitempty"`
	ServiceAccount      string     `json:"serviceAccount,omitempty"`
	ConnectedBy         string     `json:"connectedBy,omitempty"`
	ConnectedAt         *time.Time `json:"connectedAt,omitempty"`
	LastSeenAt          *time.Time `json:"lastSeenAt,omitempty"`
	CredentialExpiresAt *time.Time `json:"credentialExpiresAt,omitempty"`
	// WriteAccessFound says the credential may do more than read
	// clusters. The integration never writes; this is shown so that
	// the hub's administrator can narrow the credential.
	WriteAccessFound bool `json:"writeAccessFound"`
	ClusterCount     int  `json:"clusterCount"`
	AcceptedCount    int  `json:"acceptedCount"`
	AvailableCount   int  `json:"availableCount"`
	EnrolledCount    int  `json:"enrolledCount"`
	// SearchTerms lists the exact words plain search matches on.
	SearchTerms []string `json:"searchTerms"`
}

// HubRecord is a hub record as returned to callers.
type HubRecord struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Observation HubObservation         `json:"observation"`
	Conditions  map[string]ConditionJS `json:"conditions"`
}

// ConditionJS is a condition in a JSON response.
type ConditionJS struct {
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ClientConfigObservation is one API address a hub records for a
// cluster, with the fingerprint of the CA recorded next to it.
type ClientConfigObservation struct {
	URL           string `json:"url"`
	CAFingerprint string `json:"caFingerprint,omitempty"`
}

// EnrollmentLink says what the resource manager has to do with an
// observed cluster and on what basis the two are taken to be the same.
type EnrollmentLink struct {
	State string `json:"state"`
	// Cluster is the resource manager's record of the cluster
	// ("clusters/{id}"), Target its target id.
	Cluster string `json:"cluster,omitempty"`
	Target  string `json:"target,omitempty"`
	Basis   string `json:"basis,omitempty"`
}

// ManagedClusterObservation is the observation payload of an observed
// cluster's record.
type ManagedClusterObservation struct {
	Hub               string                    `json:"hub"`
	Name              string                    `json:"name"`
	UID               string                    `json:"uid,omitempty"`
	Accepted          bool                      `json:"accepted"`
	Available         string                    `json:"available"`
	ClusterSet        string                    `json:"clusterSet,omitempty"`
	Claims            map[string]string         `json:"claims,omitempty"`
	KubernetesVersion string                    `json:"kubernetesVersion,omitempty"`
	ClientConfigs     []ClientConfigObservation `json:"clientConfigs,omitempty"`
	Taints            []string                  `json:"taints,omitempty"`
	CreatedAt         *time.Time                `json:"createdAt,omitempty"`
	Enrollment        EnrollmentLink            `json:"enrollment"`
	// Conflict is set instead of a link when several clusters the
	// resource manager knows could be this one.
	Conflict string `json:"conflict,omitempty"`
	// SearchTerms lists the exact words plain search matches on.
	SearchTerms []string `json:"searchTerms"`
}

// hubIDPattern: a DNS label of at most 40 characters. A hub id is the
// first part of the id of every cluster enrolled through the hub
// ("{hub}.{cluster}"), so it carries no dot.
var hubIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,38}[a-z0-9])?$`)

// ValidateHubID checks that id is usable as a hub connection's id.
func ValidateHubID(id string) error {
	if !hubIDPattern.MatchString(id) {
		return fmt.Errorf("%w: hub id %q must be 1 to 40 lowercase letters, digits, or '-', starting and ending with a letter or digit", domain.ErrInvalidArgument, id)
	}
	return nil
}

// HubRecordName returns the record name of a hub connection.
func HubRecordName(hubID string) domain.ResourceName {
	return domain.ResourceName(string(HubCollectionID) + "/" + hubID)
}

// ManagedClusterRecordName returns the record name of a cluster
// observed on a hub.
func ManagedClusterRecordName(hubID, name string) domain.ResourceName {
	return domain.ResourceName(string(HubCollectionID) + "/" + hubID + "/" + string(ManagedClusterCollectionID) + "/" + name)
}

// managedClusterPrefix is the collection prefix every observed
// cluster record of a hub shares.
func managedClusterPrefix(hubID string) string {
	return string(HubCollectionID) + "/" + hubID + "/"
}

// ClusterIDFor returns the id a cluster of a hub is enrolled under.
func ClusterIDFor(hubID, name string) string {
	return hubID + "." + name
}

// hubConnectionRef is where a hub's connection is kept in the vault.
func hubConnectionRef(hubID string) domain.SecretRef {
	return domain.SecretRef("ocm/hubs/" + hubID + "/connection")
}

// HubConnection is what is needed to read a hub: its API address, the
// CA that signs its serving certificate, and a bearer token.
type HubConnection struct {
	APIServer string `json:"apiServer"`
	CACert    string `json:"caCert"`
	Token     string `json:"token"`
}

func (c HubConnection) clusterConnection() kubernetesaddon.ClusterConnection {
	return kubernetesaddon.ClusterConnection{APIServer: c.APIServer, CACert: []byte(c.CACert), Token: c.Token}
}
