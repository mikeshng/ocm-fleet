package ocm

import (
	"encoding/base64"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// observedCluster is a hub's ManagedCluster as the integration reads
// it: only the fields it shows or acts on.
type observedCluster struct {
	name       string
	uid        string
	createdAt  time.Time
	labels     map[string]string
	accepted   bool
	clusterSet string
	claims     map[string]string
	version    string
	configs    []clientConfig
	taints     []string
	conditions []domain.Condition
	available  domain.ConditionStatus
}

// clientConfig is one API address a hub records for a cluster, with
// the PEM CA recorded next to it.
type clientConfig struct {
	url string
	ca  []byte
}

const conditionAvailable = "ManagedClusterConditionAvailable"

// parseManagedCluster reads a ManagedCluster object. It never fails:
// a field of an unexpected shape is read as absent.
func parseManagedCluster(obj *unstructured.Unstructured) observedCluster {
	c := observedCluster{
		name:      obj.GetName(),
		uid:       string(obj.GetUID()),
		createdAt: obj.GetCreationTimestamp().Time,
		labels:    map[string]string{},
		available: domain.ConditionUnknown,
	}
	for k, v := range obj.GetLabels() {
		c.labels[k] = v
	}
	c.clusterSet = c.labels[clusterSetLabel]
	c.accepted, _, _ = unstructured.NestedBool(obj.Object, "spec", "hubAcceptsClient")
	c.version, _, _ = unstructured.NestedString(obj.Object, "status", "version", "kubernetes")

	configs, _, _ := unstructured.NestedSlice(obj.Object, "spec", "managedClusterClientConfigs")
	for _, raw := range configs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		url, _ := m["url"].(string)
		if url == "" {
			continue
		}
		cfg := clientConfig{url: kubernetesaddon.NormalizeAPIServerURL(url)}
		// The CA is a byte field: base64 in the object's JSON form.
		if encoded, _ := m["caBundle"].(string); encoded != "" {
			if ca, err := base64.StdEncoding.DecodeString(encoded); err == nil {
				cfg.ca = ca
			}
		}
		c.configs = append(c.configs, cfg)
	}

	taints, _, _ := unstructured.NestedSlice(obj.Object, "spec", "taints")
	for _, raw := range taints {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["key"].(string)
		value, _ := m["value"].(string)
		effect, _ := m["effect"].(string)
		if key == "" {
			continue
		}
		taint := key
		if value != "" {
			taint += "=" + value
		}
		if effect != "" {
			taint += ":" + effect
		}
		c.taints = append(c.taints, taint)
	}

	claims, _, _ := unstructured.NestedSlice(obj.Object, "status", "clusterClaims")
	for _, raw := range claims {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		value, _ := m["value"].(string)
		if name == "" {
			continue
		}
		if c.claims == nil {
			c.claims = map[string]string{}
		}
		c.claims[name] = value
	}

	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conditions {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		ctype, _ := m["type"].(string)
		rawStatus, _ := m["status"].(string)
		status, err := domain.ParseConditionStatus(rawStatus)
		if ctype == "" || err != nil {
			continue
		}
		reason, _ := m["reason"].(string)
		message, _ := m["message"].(string)
		var at time.Time
		if ts, _ := m["lastTransitionTime"].(string); ts != "" {
			at, _ = time.Parse(time.RFC3339, ts)
		}
		cond, _ := domain.NewCondition(domain.ConditionType(ctype), status, reason, message, at)
		c.conditions = append(c.conditions, cond)
		if ctype == conditionAvailable {
			c.available = status
		}
	}
	sort.Slice(c.conditions, func(i, j int) bool { return c.conditions[i].Type() < c.conditions[j].Type() })
	return c
}

// observation builds the record's observation payload.
func (c observedCluster) observation(hubID string, link clusterLink) ManagedClusterObservation {
	obs := ManagedClusterObservation{
		Hub:               hubID,
		Name:              c.name,
		UID:               c.uid,
		Accepted:          c.accepted,
		Available:         string(c.available),
		ClusterSet:        c.clusterSet,
		Claims:            c.claims,
		KubernetesVersion: c.version,
		Taints:            c.taints,
		Enrollment:        link.link,
		Conflict:          link.conflict,
		SearchTerms:       kubernetesaddon.SearchTerms(c.name, c.labels, hubID, c.clusterSet),
	}
	if obs.Enrollment.State == "" {
		obs.Enrollment.State = LinkObserved
	}
	if !c.createdAt.IsZero() {
		at := c.createdAt.UTC()
		obs.CreatedAt = &at
	}
	for _, cfg := range c.configs {
		out := ClientConfigObservation{URL: cfg.url}
		if fps, err := kubernetesaddon.CAFingerprints(cfg.ca); err == nil && len(fps) > 0 {
			out.CAFingerprint = fps[0]
		}
		obs.ClientConfigs = append(obs.ClientConfigs, out)
	}
	return obs
}

// targetLabels are the hub's facts about the cluster in the form they
// are put on the target of the enrolled cluster: the hub's labels
// (which include the cluster set), each claim as "claim.<name>", and
// the two labels that name the hub and the cluster on it. Entries that
// cannot be a target label are left out.
func (c observedCluster) targetLabels(hubID string) map[string]string {
	out := map[string]string{}
	add := func(k, v string) {
		if k == "" || len(k) > 317 || strings.ContainsAny(k, " \t\n\r") {
			return
		}
		if len(v) > 253 || strings.ContainsAny(v, "\n\r") {
			return
		}
		out[k] = v
	}
	for k, v := range c.labels {
		add(k, v)
	}
	for name, value := range c.claims {
		add(ClaimLabelPrefix+name, value)
	}
	add(LabelHub, hubID)
	add(LabelManagedCluster, c.name)
	return out
}
