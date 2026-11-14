package ocm

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// clusterLink is the declared match of an observed cluster: the link
// to a cluster the resource manager knows, or the reason none is made.
type clusterLink struct {
	link     EnrollmentLink
	conflict string
	// target is the linked target's id; empty without a link.
	target domain.TargetID
}

// targetIndex answers which cluster the resource manager knows is the
// same cluster as one a hub knows. The match is declared, never
// guessed: it rests on the enrollment's own record, or on the API
// address and CA both sides record, or (weakest, and only when the
// hub records no address) on a name carried by exactly one candidate.
type targetIndex struct {
	bySource  map[string]domain.TargetInfo
	byAddress map[string][]domain.TargetInfo
	byName    map[string][]domain.TargetInfo
}

func sourceKey(hubID, name string) string { return hubID + "/" + name }

// newTargetIndex indexes the kubernetes targets among targets.
func newTargetIndex(targets []domain.TargetInfo) *targetIndex {
	idx := &targetIndex{
		bySource:  map[string]domain.TargetInfo{},
		byAddress: map[string][]domain.TargetInfo{},
		byName:    map[string][]domain.TargetInfo{},
	}
	for _, t := range targets {
		if t.Type() != kubernetesaddon.TargetType {
			continue
		}
		props := t.Properties()
		if raw := props[kubernetesaddon.PropEnrollmentSource]; raw != "" {
			var src kubernetesaddon.ClusterSource
			if err := json.Unmarshal([]byte(raw), &src); err == nil && src.Verifier == VerifierName {
				idx.bySource[sourceKey(src.Scope, src.Name)] = t
			}
		}
		if addr := kubernetesaddon.NormalizeAPIServerURL(props[kubernetesaddon.PropAPIServer]); addr != "" {
			idx.byAddress[addr] = append(idx.byAddress[addr], t)
		}
		idx.byName[t.Name()] = append(idx.byName[t.Name()], t)
	}
	return idx
}

// match returns the link of one observed cluster.
func (idx *targetIndex) match(hubID string, c observedCluster) clusterLink {
	if t, ok := idx.bySource[sourceKey(hubID, c.name)]; ok {
		return linkTo(t, BasisEnrollment)
	}

	var candidates []domain.TargetInfo
	seen := map[domain.TargetID]struct{}{}
	for _, cfg := range c.configs {
		for _, t := range idx.byAddress[cfg.url] {
			if _, dup := seen[t.ID()]; dup {
				continue
			}
			if kubernetesaddon.SameCA([]byte(t.Properties()[kubernetesaddon.PropCACert]), cfg.ca) {
				seen[t.ID()] = struct{}{}
				candidates = append(candidates, t)
			}
		}
	}
	switch len(candidates) {
	case 1:
		return linkTo(candidates[0], BasisAddressAndCA)
	case 0:
	default:
		return clusterLink{
			link:     EnrollmentLink{State: LinkObserved},
			conflict: fmt.Sprintf("several clusters the resource manager knows have this API address and CA: %s", targetIDs(candidates)),
		}
	}

	// A name alone links only when the hub records no address to
	// compare: a recorded address that matches nothing is evidence
	// that a cluster of the same name is another cluster.
	if len(c.configs) == 0 {
		switch named := idx.byName[c.name]; len(named) {
		case 1:
			return linkTo(named[0], BasisName)
		case 0:
		default:
			return clusterLink{
				link:     EnrollmentLink{State: LinkObserved},
				conflict: fmt.Sprintf("several clusters the resource manager knows are named %q: %s", c.name, targetIDs(named)),
			}
		}
	}
	return clusterLink{link: EnrollmentLink{State: LinkObserved}}
}

func linkTo(t domain.TargetInfo, basis string) clusterLink {
	state := LinkCreated
	if kubernetesaddon.IsEnrolledTarget(t) {
		state = LinkEnrolled
	}
	return clusterLink{
		link: EnrollmentLink{
			State:   state,
			Cluster: t.Properties()[kubernetesaddon.PropClusterResourceName],
			Target:  string(t.ID()),
			Basis:   basis,
		},
		target: t.ID(),
	}
}

func targetIDs(targets []domain.TargetInfo) string {
	ids := make([]string, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, string(t.ID()))
	}
	sort.Strings(ids)
	return strings.Join(ids, ", ")
}

// externalLabelsOf decodes the labels an integration contributed to an
// enrolled target.
func externalLabelsOf(t domain.TargetInfo) map[string]string {
	out := map[string]string{}
	if raw := t.Properties()[kubernetesaddon.PropExternalLabels]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			return map[string]string{}
		}
	}
	return out
}

func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
