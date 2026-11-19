package ocm

import (
	"encoding/json"
	"fmt"
	"slices"
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
// guessed: it rests on the enrollment's own record, or on the
// cluster's own identifier where the hub publishes it, or on the API
// address and CA both sides record, or (weakest, and only when the
// hub records no address) on a name carried by exactly one candidate
// and by no other hub's cluster that is as hard to tell apart.
type targetIndex struct {
	bySource     map[string]domain.TargetInfo
	byClusterUID map[string][]domain.TargetInfo
	byAddress    map[string][]domain.TargetInfo
	byName       map[string][]domain.TargetInfo
	// nameElsewhere returns the other connected hubs that have a
	// cluster of the name with no address on record. Nil when there is
	// one hub to ask about.
	nameElsewhere func(name string) []string
}

func sourceKey(hubID, name string) string { return hubID + "/" + name }

// newTargetIndex indexes the kubernetes targets among targets.
func newTargetIndex(targets []domain.TargetInfo) *targetIndex {
	idx := &targetIndex{
		bySource:     map[string]domain.TargetInfo{},
		byClusterUID: map[string][]domain.TargetInfo{},
		byAddress:    map[string][]domain.TargetInfo{},
		byName:       map[string][]domain.TargetInfo{},
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
		if uid := props[kubernetesaddon.PropClusterUID]; uid != "" {
			idx.byClusterUID[uid] = append(idx.byClusterUID[uid], t)
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

	// The cluster's own identifier, where the hub publishes one, says
	// more than an address: it is the same at every address.
	if id := c.claims[clusterIDClaim]; id != "" {
		switch same := idx.byClusterUID[id]; len(same) {
		case 1:
			return linkTo(same[0], BasisClusterID)
		case 0:
		default:
			return clusterLink{
				link:     EnrollmentLink{State: LinkObserved},
				conflict: fmt.Sprintf("several clusters the resource manager knows report this cluster's identifier: %s", targetIDs(same)),
			}
		}
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
			// Two hubs that each have a cluster of this name, neither
			// with an address, cannot both mean the one candidate, and
			// the name does not say which of them does.
			if idx.nameElsewhere != nil {
				if others := idx.nameElsewhere(c.name); len(others) > 0 {
					return clusterLink{
						link: EnrollmentLink{State: LinkObserved},
						conflict: fmt.Sprintf("a cluster named %q with no API address on record is also on %s; the name alone cannot say which of them is %s",
							c.name, hubNames(others), named[0].ID()),
					}
				}
			}
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

// withdrawnIndex answers whether an observed cluster is one the
// enrollment records say was withdrawn. The match rests on the API
// address the hub records for the cluster and the CA recorded next to
// it: the same two facts the live match rests on, because a withdrawn
// record keeps both after its target is gone.
type withdrawnIndex struct {
	byAddress map[string][]withdrawnCluster
}

func newWithdrawnIndex(clusters []withdrawnCluster) *withdrawnIndex {
	idx := &withdrawnIndex{byAddress: map[string][]withdrawnCluster{}}
	for _, w := range clusters {
		if w.apiServer == "" {
			continue
		}
		idx.byAddress[w.apiServer] = append(idx.byAddress[w.apiServer], w)
	}
	return idx
}

// match returns the id of the withdrawn cluster an observed cluster
// is. found is true only when exactly one withdrawn record has one of
// the cluster's API addresses and the CA the hub records for it. Zero
// or several records find nothing: an address and CA several withdrawn
// records share say nothing about which of them the observed cluster
// is. A cluster the hub records no address for matches nothing: there
// is nothing to compare.
func (idx *withdrawnIndex) match(c observedCluster) (id string, found bool) {
	var matched []withdrawnCluster
	seen := map[string]struct{}{}
	for _, cfg := range c.configs {
		fingerprints, err := kubernetesaddon.CAFingerprints(cfg.ca)
		if err != nil {
			continue
		}
		for _, w := range idx.byAddress[cfg.url] {
			if _, dup := seen[w.id]; dup {
				continue
			}
			if !slices.Contains(fingerprints, w.caFingerprint) {
				continue
			}
			seen[w.id] = struct{}{}
			matched = append(matched, w)
		}
	}
	if len(matched) != 1 {
		return "", false
	}
	return matched[0].id, true
}

// withdrawnLink is the link of an observed cluster that is a withdrawn
// record of the resource manager's.
func withdrawnLink(clusterID string) clusterLink {
	return clusterLink{link: EnrollmentLink{
		State:   LinkWithdrawn,
		Cluster: string(kubernetesaddon.ClusterRecordName(clusterID)),
	}}
}

func targetIDs(targets []domain.TargetInfo) string {
	ids := make([]string, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, string(t.ID()))
	}
	sort.Strings(ids)
	return strings.Join(ids, ", ")
}

// hubNames words a list of hub ids for a sentence: `hub "a"`, or
// `hubs "a", "b"`.
func hubNames(ids []string) string {
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		quoted = append(quoted, fmt.Sprintf("%q", id))
	}
	if len(quoted) == 1 {
		return "hub " + quoted[0]
	}
	return "hubs " + strings.Join(quoted, ", ")
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
