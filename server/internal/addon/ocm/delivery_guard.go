package ocm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

const (
	// ocmNamespace is the namespace of Open Cluster Management's
	// operators; its other namespaces start with the same name and "-".
	ocmNamespace = "open-cluster-management"
	// ocmAPIGroup is the API group of Open Cluster Management; its
	// other groups end in "." and the same name.
	ocmAPIGroup = "open-cluster-management.io"
)

// GuardTarget implements [kubernetesaddon.DeliveryGuard]: Open Cluster
// Management on a cluster is not a workload, and a deployment must be
// able neither to change it nor to take it away.
//
// On a cluster that a hub manages, the namespaces of Open Cluster
// Management are out of reach of deliveries. A cluster counts as
// managed by a hub when it was enrolled through a hub, when a hub's
// facts are on its target, when a connected hub's record is linked to
// it, or when a connected hub runs on it.
//
// On the cluster a connected hub runs on, more is the hub's: the
// namespace the hub keeps for each of its clusters, the namespace of
// the account that connects the hub, and every object of Open Cluster
// Management's own API. This resource manager does not write to a hub,
// and a deployment does not make it.
//
// It returns nil for a cluster no hub has to do with.
func (s *Service) GuardTarget(ctx context.Context, target domain.TargetInfo) kubernetesaddon.ObjectGuard {
	if target.Type() != kubernetesaddon.TargetType {
		return nil
	}
	managed := externalLabelsOf(target)[LabelHub] != ""
	if raw := target.Properties()[kubernetesaddon.PropEnrollmentSource]; raw != "" {
		var src kubernetesaddon.ClusterSource
		if err := json.Unmarshal([]byte(raw), &src); err == nil && src.Verifier == VerifierName {
			managed = true
		}
	}
	// hubsHere are the connected hubs that run on this cluster, each
	// with the names of its clusters.
	type hubHere struct {
		id       string
		clusters map[string]struct{}
	}
	var hubsHere []hubHere
	for _, hw := range s.hubList() {
		if !hw.runsOn(target) {
			managed = managed || hw.linksTo(target.ID())
			continue
		}
		hubsHere = append(hubsHere, hubHere{id: hw.id, clusters: s.clusterNames(ctx, hw)})
	}
	if !managed && len(hubsHere) == 0 {
		return nil
	}

	return func(obj kubernetesaddon.GuardedObject) string {
		if ns := obj.Namespace; ns == ocmNamespace || strings.HasPrefix(ns, ocmNamespace+"-") {
			return fmt.Sprintf("namespace %q belongs to Open Cluster Management on this cluster", ns)
		}
		for _, hub := range hubsHere {
			if obj.Group == ocmAPIGroup || strings.HasSuffix(obj.Group, "."+ocmAPIGroup) {
				return fmt.Sprintf("%s is an object of Open Cluster Management's own API and hub %q runs on this cluster: this resource manager does not write to a hub", obj.Kind, hub.id)
			}
			if obj.Namespace == HubReaderNamespace {
				return fmt.Sprintf("namespace %q holds the account that connects hub %q", obj.Namespace, hub.id)
			}
			if _, ok := hub.clusters[obj.Namespace]; ok {
				return fmt.Sprintf("namespace %q is where hub %q, which runs on this cluster, keeps what belongs to its cluster %q", obj.Namespace, hub.id, obj.Namespace)
			}
		}
		return ""
	}
}

var _ kubernetesaddon.DeliveryGuard = (*Service)(nil)

// clusterNames returns the names of a hub's clusters: as the hub says
// now, or, while the hub has not been read in this process, as its
// records say. A hub keeps a namespace of the same name for each.
func (s *Service) clusterNames(ctx context.Context, hw *hubWatch) map[string]struct{} {
	out := map[string]struct{}{}
	if hw.isSynced() {
		for _, name := range hw.informer.GetStore().ListKeys() {
			out[name] = struct{}{}
		}
		return out
	}
	recorded, err := s.recordedClusterNames(ctx, hw.id)
	if err != nil {
		s.logger.Warn("could not read the recorded clusters of a hub for the delivery guard", "hub", hw.id, "error", err)
	}
	for _, name := range recorded {
		out[name] = struct{}{}
	}
	return out
}

// runsOn reports whether the hub runs on the target's cluster: the
// cluster reported the identifier the hub reported, or, where one of
// them reported none, the target is reached at the hub connection's
// API address with its CA.
func (hw *hubWatch) runsOn(target domain.TargetInfo) bool {
	hw.mu.Lock()
	conn, uid := hw.conn, hw.obs.ClusterUID
	hw.mu.Unlock()
	props := target.Properties()
	if known := props[kubernetesaddon.PropClusterUID]; uid != "" && known != "" {
		return known == uid
	}
	return kubernetesaddon.NormalizeAPIServerURL(props[kubernetesaddon.PropAPIServer]) == conn.APIServer &&
		kubernetesaddon.SameCA([]byte(props[kubernetesaddon.PropCACert]), []byte(conn.CACert))
}

// linksTo reports whether a record of the hub is linked to the target.
func (hw *hubWatch) linksTo(id domain.TargetID) bool {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	_, ok := hw.linked[id]
	return ok
}
