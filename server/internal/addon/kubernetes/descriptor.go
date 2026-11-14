package kubernetes

import "github.com/stolostron/ocm-fleet/server/internal/domain"

// AddonID is the [domain.AddonID] for the Kubernetes addon. It also
// doubles as the service-name prefix every resource type the addon
// owns must start with, per the platform's addon-ownership rule.
const AddonID domain.AddonID = "kubernetes.open-cluster-management.io"

// Descriptor returns the addon descriptor for the generic Kubernetes
// agent. It declares a delivery capability for Kubernetes
// targets using token-passthrough delivery (no fleetlet), an inventory
// capability for the generic watched-object resource type, and
// inventory capabilities for the records enrollment keeps: clusters
// taken in and the bundles that authorize it.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   AddonID,
		Name: "Kubernetes Agent",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.InventoryResourceCapability{ResourceType: ObjectResourceType},
			domain.InventoryResourceCapability{ResourceType: ClusterResourceType},
			domain.InventoryResourceCapability{ResourceType: EnrollmentBundleResourceType},
		},
	}
}

// Schemas returns every extension resource schema the addon registers.
func Schemas() []domain.ExtensionResourceSchema {
	return []domain.ExtensionResourceSchema{InventorySchema(), ClusterSchema(), EnrollmentBundleSchema()}
}

// ClusterSchema returns the inventory-only schema of a cluster taken in
// by enrollment ("clusters/{id}").
func ClusterSchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: ClusterResourceType,
		ProtoPackage: "ocm.kubernetes.v1alpha",
		Version:      "v1alpha",
		CollectionID: string(ClusterCollectionID),
		Singular:     "Cluster",
		Plural:       "Clusters",
		Inventory:    &domain.InventorySchema{},
	}
}

// EnrollmentBundleSchema returns the inventory-only schema of an
// enrollment bundle ("enrollmentBundles/{id}").
func EnrollmentBundleSchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: EnrollmentBundleResourceType,
		ProtoPackage: "ocm.kubernetes.v1alpha",
		Version:      "v1alpha",
		CollectionID: string(EnrollmentBundleCollectionID),
		Singular:     "EnrollmentBundle",
		Plural:       "EnrollmentBundles",
		Inventory:    &domain.InventorySchema{},
	}
}

// InventorySchema returns the extension resource schema for the generic
// Kubernetes object inventory type. Every watched Kubernetes object,
// regardless of kind, is reported under this one schema. It has no
// management section (no per-object spec to fulfill, only observed
// state) and no ProtoFiles to compile. Connect still registers a type
// definition with Inventory set and Management nil, and activates the
// schema so QueryResources can include the type in its activation scope.
func InventorySchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: ObjectResourceType,
		ProtoPackage: "ocm.kubernetes.v1alpha",
		Version:      "v1alpha",
		CollectionID: string(ObjectCollectionID),
		Singular:     "Object",
		Plural:       "Objects",
		Inventory:    &domain.InventorySchema{},
	}
}
