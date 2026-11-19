// Package ocm is the integration with Open Cluster Management hubs.
//
// It observes: for every hub a person connects, it lists and watches
// the hub's ManagedCluster objects with a read-only credential and
// keeps one inventory record per cluster, so that the clusters a hub
// manages can be seen and searched next to the clusters the resource
// manager manages itself. It writes nothing to a hub.
//
// It helps enrollment: clusters of a connected hub can be taken in by
// the resource manager's own enrollment capability (the kubernetes
// add-on). The integration vouches for a presented cluster against the
// hub's record of it, renders the pieces a hub's administrator applies
// to roll enrollment out through the hub's add-on mechanism, and keeps
// the hub's facts about an enrolled cluster on its target as labels.
package ocm

import (
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// AddonID is the [domain.AddonID] of the integration and the service
// name its resource types live under.
const AddonID domain.AddonID = "ocm.open-cluster-management.io"

const (
	// HubResourceType is the inventory-only record of a connected hub,
	// named "hubs/{hub}".
	HubResourceType domain.ResourceType = domain.ResourceType(AddonID) + "/Hub"
	// ManagedClusterResourceType is the inventory-only record of a
	// cluster a hub manages, named "hubs/{hub}/managedClusters/{name}".
	ManagedClusterResourceType domain.ResourceType = domain.ResourceType(AddonID) + "/ManagedCluster"

	// HubCollectionID and ManagedClusterCollectionID are the leaf
	// collections of the two record types.
	HubCollectionID            domain.CollectionID = "hubs"
	ManagedClusterCollectionID domain.CollectionID = "managedClusters"
)

// Descriptor returns the add-on descriptor. The integration delivers
// nothing itself: it has inventory capabilities only.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   AddonID,
		Name: "Open Cluster Management Integration",
		Capabilities: []domain.Capability{
			domain.InventoryResourceCapability{ResourceType: HubResourceType},
			domain.InventoryResourceCapability{ResourceType: ManagedClusterResourceType},
		},
	}
}

// Schemas returns the extension resource schemas the add-on registers.
func Schemas() []domain.ExtensionResourceSchema {
	return []domain.ExtensionResourceSchema{HubSchema(), ManagedClusterSchema()}
}

// HubSchema returns the inventory-only schema of a connected hub.
func HubSchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: HubResourceType,
		ProtoPackage: "ocm.ocmio.v1alpha",
		Version:      "v1alpha",
		CollectionID: string(HubCollectionID),
		Singular:     "Hub",
		Plural:       "Hubs",
		Inventory:    &domain.InventorySchema{},
	}
}

// ManagedClusterSchema returns the inventory-only schema of a cluster
// observed on a hub.
func ManagedClusterSchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: ManagedClusterResourceType,
		ProtoPackage: "ocm.ocmio.v1alpha",
		Version:      "v1alpha",
		CollectionID: string(ManagedClusterCollectionID),
		Singular:     "ManagedCluster",
		Plural:       "ManagedClusters",
		Inventory:    &domain.InventorySchema{},
	}
}
