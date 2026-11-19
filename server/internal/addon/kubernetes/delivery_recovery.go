package kubernetes

import (
	"context"
	"errors"
	"fmt"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// RecoverActiveDeliveries re-dispatches the deliveries that were in
// flight for this add-on's targets when the resource manager stopped.
// A delivery acknowledged before a restart would otherwise stay
// non-terminal with nothing working on it: the platform waits for a
// completion signal that no process is left to send.
//
// [domain.DeliveryReporter.ListActiveDeliveries] returns only
// non-terminal deliveries, enriched with the target, the caller's
// auth, and a signed delivery's re-assembled attestation, so a
// redispatch is the same call the platform makes for a new delivery.
// Both operations are safe to repeat: an apply is idempotent and a
// removal skips objects that are already gone. Deliveries for target
// types this add-on does not serve are excluded before the listing.
func (a *DeliveryAgent) RecoverActiveDeliveries(ctx context.Context, lister TargetLister) error {
	targets, err := lister.ListTargets(ctx)
	if err != nil {
		return fmt.Errorf("recover active deliveries: list targets: %w", err)
	}
	ids := make([]domain.TargetID, 0, len(targets))
	for _, t := range targets {
		if t.Type() == TargetType {
			ids = append(ids, t.ID())
		}
	}
	if len(ids) == 0 {
		return nil
	}

	active, err := a.reporter.ListActiveDeliveries(ctx, ids)
	if err != nil {
		return fmt.Errorf("recover active deliveries: list: %w", err)
	}

	var errs []error
	for _, d := range active {
		var err error
		if d.Delivery.Operation() == domain.DeliveryOperationRemove {
			err = a.Remove(ctx, d.Target, d.Delivery.ID(), d.Delivery.Manifests(), d.Auth, d.Attestation, d.Delivery.Generation())
			if err != nil {
				err = fmt.Errorf("recover removal %s to %s: %w", d.Delivery.ID(), d.Target.ID(), err)
			}
		} else {
			err = a.Deliver(ctx, d.Target, d.Delivery.ID(), d.Delivery.Manifests(), d.Auth, d.Attestation, d.Delivery.Generation())
			if err != nil {
				err = fmt.Errorf("recover delivery %s to %s: %w", d.Delivery.ID(), d.Target.ID(), err)
			}
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
