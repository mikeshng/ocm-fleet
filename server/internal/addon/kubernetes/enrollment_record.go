package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// clusterState is a cluster record being changed: its observation and
// its conditions by type. Records are written whole, so a change loads
// the record, alters what it knows about, and writes everything back.
type clusterState struct {
	obs   ClusterObservation
	conds map[domain.ConditionType]domain.Condition
}

// loadClusterState reads the record of a cluster. A cluster without a
// record, or with one that cannot be read, starts from an empty state.
func (s *EnrollmentService) loadClusterState(ctx context.Context, clusterID string) *clusterState {
	state := &clusterState{conds: map[domain.ConditionType]domain.Condition{}}
	res, err := s.readRecord(ctx, ClusterResourceType, ClusterRecordName(clusterID))
	if err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			s.logger.Warn("could not read cluster record; starting from an empty one", "cluster", clusterID, "error", err)
		}
		return state
	}
	if err := decodeObservation(res, &state.obs); err != nil {
		s.logger.Warn("could not decode cluster record; starting from an empty one", "cluster", clusterID, "error", err)
		state.obs = ClusterObservation{}
	}
	if inv := res.Inventory(); inv != nil {
		for _, c := range inv.Conditions() {
			state.conds[c.Type()] = c
		}
	}
	return state
}

// set records a condition. The transition time moves only when the
// status changes.
func (c *clusterState) set(t domain.ConditionType, status domain.ConditionStatus, reason, message string, now time.Time) {
	at := now
	if prev, ok := c.conds[t]; ok && prev.Status() == status {
		at = prev.LastTransitionTime()
	}
	cond, _ := domain.NewCondition(t, status, reason, message, at)
	c.conds[t] = cond
}

func (c *clusterState) status(t domain.ConditionType) domain.ConditionStatus {
	if cond, ok := c.conds[t]; ok {
		return cond.Status()
	}
	return domain.ConditionUnknown
}

// setIndexing records whether the cluster's objects are being indexed.
func (c *clusterState) setIndexing(configured bool, err error, now time.Time) {
	switch {
	case !configured:
		c.set(ClusterConditionIndexing, domain.ConditionFalse, "NotConfigured", "indexing is not enabled on this resource manager", now)
	case err != nil:
		c.set(ClusterConditionIndexing, domain.ConditionFalse, "StartFailed", truncate(err.Error(), 300), now)
	default:
		c.set(ClusterConditionIndexing, domain.ConditionTrue, "Running", "the cluster's objects are indexed", now)
	}
}

// setReady derives the summary condition: enrolled, reachable, and
// holding a credential the cluster recognizes.
func (c *clusterState) setReady(now time.Time) {
	switch {
	case c.status(ClusterConditionEnrolled) != domain.ConditionTrue:
		reason, message := "NotEnrolled", "the cluster is not enrolled"
		if cond, ok := c.conds[ClusterConditionEnrolled]; ok {
			reason, message = cond.Reason(), cond.Message()
		}
		c.set(ClusterConditionReady, domain.ConditionFalse, reason, message, now)
	case c.status(ClusterConditionCredentialValid) == domain.ConditionFalse:
		cond := c.conds[ClusterConditionCredentialValid]
		c.set(ClusterConditionReady, domain.ConditionFalse, cond.Reason(), cond.Message(), now)
	case c.status(ClusterConditionReachable) == domain.ConditionFalse:
		cond := c.conds[ClusterConditionReachable]
		c.set(ClusterConditionReady, domain.ConditionFalse, cond.Reason(), cond.Message(), now)
	default:
		c.set(ClusterConditionReady, domain.ConditionTrue, "Enrolled", "the cluster is enrolled and its API answers", now)
	}
}

// writeClusterState writes the whole record.
func (s *EnrollmentService) writeClusterState(ctx context.Context, clusterID string, labels map[string]string, state *clusterState, now time.Time) error {
	raw, err := json.Marshal(state.obs)
	if err != nil {
		return fmt.Errorf("encode record of cluster %q: %w", clusterID, err)
	}
	msg := json.RawMessage(raw)
	types := make([]string, 0, len(state.conds))
	for t := range state.conds {
		types = append(types, string(t))
	}
	sort.Strings(types)
	conds := make([]domain.Condition, 0, len(types))
	for _, t := range types {
		conds = append(conds, state.conds[domain.ConditionType(t)])
	}
	if labels == nil {
		labels = map[string]string{}
	}
	if err := s.records.ReplaceBatch(ctx, ClusterResourceType, []InventoryObjectReport{{
		Name:        ClusterRecordName(clusterID),
		Labels:      labels,
		Observation: &msg,
		Conditions:  conds,
		ObservedAt:  now,
	}}); err != nil {
		return fmt.Errorf("write record of cluster %q: %w", clusterID, err)
	}
	return nil
}

// observationFromTarget fills the fields of a cluster observation that
// the target states, keeping from prev what only probes and events
// know (state, version, rights, credential times, withdrawal).
func observationFromTarget(target domain.TargetInfo, prev ClusterObservation) ClusterObservation {
	props := target.Properties()
	obs := prev
	clusterID, _ := ClusterIDFromTarget(target)
	obs.Name = clusterID
	obs.Origin = props[PropOrigin]
	obs.AccessForm = props[PropAccessForm]
	obs.APIServer = props[PropAPIServer]
	obs.CAFingerprint = firstCAFingerprint([]byte(props[PropCACert]))
	obs.Target = string(target.ID())
	obs.ServiceAccount = props[PropServiceAccount]
	obs.Profile = props[PropAccessProfile]
	obs.DeliveryCredential = props[PropDeliveryCredential]
	obs.EnrolledBy = props[PropEnrolledBy]
	obs.EnrollmentBundle = props[PropEnrollmentBundle]
	obs.EnrolledAt = nil
	if at, err := time.Parse(time.RFC3339, props[PropEnrolledAt]); err == nil {
		obs.EnrolledAt = &at
	}
	obs.Source = nil
	var extra []string
	if raw := props[PropEnrollmentSource]; raw != "" {
		var src ClusterSource
		if err := json.Unmarshal([]byte(raw), &src); err == nil {
			obs.Source = &src
			extra = append(extra, src.Scope, src.Name)
		}
	}
	obs.SearchTerms = SearchTerms(clusterID, target.Labels(), extra...)
	return obs
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
