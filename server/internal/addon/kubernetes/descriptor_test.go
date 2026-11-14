package kubernetes_test

import (
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/addon/kubernetes"
	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func TestDescriptor_DeclaresDeliveryAndInventoryCapabilities(t *testing.T) {
	desc := kubernetes.Descriptor()

	if desc.ID != kubernetes.AddonID {
		t.Errorf("ID = %q, want %q", desc.ID, kubernetes.AddonID)
	}
	if len(desc.Capabilities) != 4 {
		t.Fatalf("len(Capabilities) = %d, want 4", len(desc.Capabilities))
	}

	var hasDelivery bool
	inventory := map[domain.ResourceType]bool{}
	for _, cap := range desc.Capabilities {
		switch c := cap.(type) {
		case domain.DeliveryCapability:
			hasDelivery = c.TargetType == kubernetes.TargetType
		case domain.InventoryResourceCapability:
			inventory[c.ResourceType] = true
		}
	}
	if !hasDelivery {
		t.Error("expected a DeliveryCapability for kubernetes.TargetType")
	}
	for _, rt := range []domain.ResourceType{
		kubernetes.ObjectResourceType, kubernetes.ClusterResourceType, kubernetes.EnrollmentBundleResourceType,
	} {
		if !inventory[rt] {
			t.Errorf("expected an InventoryResourceCapability for %s", rt)
		}
	}
}

// TestSchemas_MatchInventoryCapabilities pins that every inventory
// capability the descriptor claims has a schema, each inventory-only
// and owned by the addon's service name.
func TestSchemas_MatchInventoryCapabilities(t *testing.T) {
	schemas := map[domain.ResourceType]domain.ExtensionResourceSchema{}
	for _, s := range kubernetes.Schemas() {
		schemas[s.ResourceType] = s
	}
	wantCollections := map[domain.ResourceType]string{
		kubernetes.ObjectResourceType:           "objects",
		kubernetes.ClusterResourceType:          "clusters",
		kubernetes.EnrollmentBundleResourceType: "enrollmentBundles",
	}
	for _, cap := range kubernetes.Descriptor().Capabilities {
		inv, ok := cap.(domain.InventoryResourceCapability)
		if !ok {
			continue
		}
		s, ok := schemas[inv.ResourceType]
		if !ok {
			t.Errorf("no schema for inventory capability %s", inv.ResourceType)
			continue
		}
		if s.Inventory == nil || s.Management != nil {
			t.Errorf("schema %s must be inventory-only", inv.ResourceType)
		}
		if s.CollectionID != wantCollections[inv.ResourceType] {
			t.Errorf("schema %s CollectionID = %q, want %q", inv.ResourceType, s.CollectionID, wantCollections[inv.ResourceType])
		}
		if inv.ResourceType.ServiceName() != domain.ServiceName(kubernetes.AddonID) {
			t.Errorf("schema %s is not under the addon's service name", inv.ResourceType)
		}
	}
	if len(schemas) != len(wantCollections) {
		t.Errorf("Schemas() returned %d schemas, want %d", len(schemas), len(wantCollections))
	}
}

// TestInventorySchema_ObjectInventoryShape pins every field of the generic
// object inventory schema. Most of these fields (ProtoPackage,
// Singular, Plural) are never read back through
// [domain.ExtensionResourceType] for an inventory-only schema -- the
// platform persists ResourceType/APIVersion/CollectionID and activates
// inventory-only schemas for QueryResources scoping without a management
// proto surface -- so an AddonManager-level registration test cannot
// catch a typo in the unused-looking identity fields. This is the only
// test that can.
func TestInventorySchema_ObjectInventoryShape(t *testing.T) {
	s := kubernetes.InventorySchema()

	if s.ResourceType != kubernetes.ObjectResourceType {
		t.Errorf("ResourceType = %q, want %q", s.ResourceType, kubernetes.ObjectResourceType)
	}
	if s.ProtoPackage != "ocm.kubernetes.v1alpha" {
		t.Errorf("ProtoPackage = %q, want %q", s.ProtoPackage, "ocm.kubernetes.v1alpha")
	}
	if s.Version != "v1alpha" {
		t.Errorf("Version = %q, want %q", s.Version, "v1alpha")
	}
	if s.CollectionID != string(kubernetes.ObjectCollectionID) {
		t.Errorf("CollectionID = %q, want %q", s.CollectionID, kubernetes.ObjectCollectionID)
	}
	if s.Singular != "Object" {
		t.Errorf("Singular = %q, want %q", s.Singular, "Object")
	}
	if s.Plural != "Objects" {
		t.Errorf("Plural = %q, want %q", s.Plural, "Objects")
	}
	if s.Inventory == nil {
		t.Error("Inventory is nil, want non-nil")
	}
	if s.Management != nil {
		t.Error("Management is non-nil, want nil (inventory-only schema)")
	}
	if len(s.ProtoFiles) != 0 {
		t.Errorf("ProtoFiles = %v, want empty (no proto to compile for an inventory-only schema)", s.ProtoFiles)
	}
	if s.EntryFile != "" {
		t.Errorf("EntryFile = %q, want empty", s.EntryFile)
	}
}
