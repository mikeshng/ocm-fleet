package bootstrap

import (
	"context"

	kubernetesaddon "github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	ocmaddon "github.com/stolostron/ocm-fleet/server/internal/addon/ocm"
	"github.com/stolostron/ocm-fleet/server/internal/application"
)

// newOCMIntegration assembles the integration with Open Cluster
// Management hubs. enrollment is the kubernetes add-on's enrollment
// capability, or nil when that add-on is not enabled: hubs are then
// observed and clusters cannot be enrolled through them. It reports
// false when the composition supplies no record store (test
// assemblies).
func newOCMIntegration(deps AddonDeps, enrollment *kubernetesEnrollment) (AddonSpec, bool) {
	if deps.InventoryRecords == nil || deps.Store == nil || deps.Vault == nil {
		return AddonSpec{}, false
	}
	var enrollmentSvc *kubernetesaddon.EnrollmentService
	var endpoint kubernetesaddon.EnrollmentEndpoint
	if enrollment != nil {
		enrollmentSvc = enrollment.service
		endpoint = enrollment.endpoint
	}
	svc := ocmaddon.NewService(ocmaddon.Deps{
		Store:      deps.Store,
		Vault:      deps.Vault,
		Records:    deps.InventoryRecords,
		Enrollment: enrollmentSvc,
		Logger:     deps.Logger,
	})
	handlers := ocmaddon.NewHTTP(svc, endpoint, deps.Caller, deps.Logger)
	appCtx := deps.AppCtx
	return AddonSpec{
		Descriptor: ocmaddon.Descriptor(),
		Connect: application.ConnectInput{
			Schemas: ocmaddon.Schemas(),
		},
		// Hubs connected earlier are watched again. A hub that cannot be
		// reached does not fail the start: its record says so.
		AfterConnect:  func(context.Context) error { return svc.Start(appCtx) },
		Close:         svc.Close,
		HTTPRoutes:    handlers.Register,
		TargetAdvisor: svc,
	}, true
}
