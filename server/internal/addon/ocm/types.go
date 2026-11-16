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
	// LabelHubClusterSet names the cluster's set on that hub as
	// "{hub}.{set}". The hub's own cluster set label carries the set's
	// name alone, and two hubs can each have a set of one name; this
	// label selects the set of one hub. It is left out when the two
	// names together are longer than a label value may be.
	LabelHubClusterSet = "ocm.open-cluster-management.io/hub-clusterset"
	// ClaimLabelPrefix prefixes a cluster claim's name.
	ClaimLabelPrefix = "claim."

	clusterSetLabel = "cluster.open-cluster-management.io/clusterset"
	// clusterIDClaim is the cluster claim that carries a cluster's own
	// identifier, where a hub's clusters publish one.
	clusterIDClaim = "id.k8s.io"
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

// ClusterConditionHubFacts is kept on the record of an enrolled cluster
// that carries a hub's facts (its labels, its cluster set, its claims).
// It is true while the hub that supplied them is connected, answers,
// and still reports the cluster. Otherwise the facts are the last ones
// seen: they still select the cluster, and nobody keeps them up to
// date. The reason says which of the three it is.
const ClusterConditionHubFacts domain.ConditionType = "HubFactsCurrent"

// Reasons of [ClusterConditionHubFacts].
const (
	HubFactsCurrent          = "Current"
	HubFactsHubUnreachable   = "HubUnreachable"
	HubFactsHubDisconnected  = "HubDisconnected"
	HubFactsNotReportedByHub = "NotReportedByHub"
)

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
	// BasisClusterID: the hub publishes the cluster's own identifier
	// (the cluster claim "id.k8s.io"), and it is the identifier a
	// cluster the resource manager knows reported when it was enrolled.
	// It holds at whatever address either side reaches the cluster.
	BasisClusterID = "cluster-id"
	// BasisAddressAndCA: the hub records the same API address and CA
	// as a cluster the resource manager knows.
	BasisAddressAndCA = "address-and-ca"
	// BasisName: only the name agrees, the hub records no address for
	// the cluster, exactly one candidate exists, and no other connected
	// hub has a cluster of that name without an address. A weak basis;
	// it is shown so that a person can judge it.
	BasisName = "name"
)

// Rollout modes: how a hub's enrollment add-on reaches the hub's
// clusters.
const (
	// RolloutManual: the hub's administrator enables the add-on for
	// one cluster at a time.
	RolloutManual = "manual"
	// RolloutEveryCluster: the hub enables the add-on for every cluster
	// it has, its own cluster included when that is registered with
	// the hub, and for every cluster that joins later.
	RolloutEveryCluster = "every-cluster"
	// RolloutPlacement: the hub enables the add-on for the clusters a
	// Placement of the hub administrator's selects.
	RolloutPlacement = "placement"
)

// EnrollmentRollout says how an enrollment add-on reaches a hub's
// clusters.
type EnrollmentRollout struct {
	Mode string `json:"mode"`
	// PlacementNamespace and PlacementName name the Placement the
	// add-on follows. They are set by the caller for the mode
	// "placement" and name the generated Placement for the mode
	// "every-cluster".
	PlacementNamespace string `json:"placementNamespace,omitempty"`
	PlacementName      string `json:"placementName,omitempty"`
}

// HubObservation is the observation payload of a hub record. It holds
// nothing secret: the connection is kept in the vault.
type HubObservation struct {
	ID            string `json:"id"`
	DisplayName   string `json:"displayName,omitempty"`
	APIServer     string `json:"apiServer"`
	CAFingerprint string `json:"caFingerprint,omitempty"`
	// ClusterUID identifies the cluster the hub runs on, whatever
	// address it is reached at: the UID of its kube-system namespace.
	// Empty for a hub whose credential may not read it.
	ClusterUID          string     `json:"clusterUID,omitempty"`
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
	// EnrolledThroughHubCount counts the hub's clusters that were
	// enrolled through the hub's own add-on. They are the ones that
	// depend on a valid enrollment secret of the hub: their agents
	// present it again from time to time, which repairs an enrollment.
	EnrolledThroughHubCount int `json:"enrolledThroughHubCount"`
	// EnrollmentRollout is how the enrollment add-on last generated
	// for the hub reaches its clusters; nil until one was generated
	// for this connection. The integration does not read the add-on on
	// the hub: this is what the generated manifest says, not what the
	// hub's administrator applied.
	EnrollmentRollout *EnrollmentRollout `json:"enrollmentRollout,omitempty"`
	// EnrollmentSecrets counts the valid enrollment secrets that cover
	// the hub's clusters. With none, an add-on on the hub carries a
	// secret that is not accepted any more: no cluster of the hub can
	// enroll through it, and the clusters counted by
	// EnrolledThroughHubCount cannot repair their enrollment, until an
	// add-on is generated and applied again.
	EnrollmentSecrets int `json:"enrollmentSecrets"`
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
