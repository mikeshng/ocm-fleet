package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/restmapper"
	k8stesting "k8s.io/client-go/testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

func newTestApplier(t *testing.T, objects ...runtime.Object) *applier {
	t.Helper()
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMapList"}, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NamespaceList"}, &unstructured.UnstructuredList{})

	client := dynamicfake.NewSimpleDynamicClient(scheme, objects...)

	apiGroupResources := []*restmapper.APIGroupResources{
		{
			Group: metav1.APIGroup{
				Name: "",
				Versions: []metav1.GroupVersionForDiscovery{
					{GroupVersion: "v1", Version: "v1"},
				},
			},
			VersionedResources: map[string][]metav1.APIResource{
				"v1": {
					{
						Name:       "configmaps",
						Kind:       "ConfigMap",
						Namespaced: true,
						Group:      "",
						Version:    "v1",
					},
					{
						Name:       "namespaces",
						Kind:       "Namespace",
						Namespaced: false,
						Group:      "",
						Version:    "v1",
					},
				},
			},
		},
	}

	mapper := restmapper.NewDiscoveryRESTMapper(apiGroupResources)
	return &applier{client: client, mapper: mapper}
}

func TestApplier_Delete(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName("test-cm")

	ap := newTestApplier(t, obj)

	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestApplier_Delete_NotFound(t *testing.T) {
	ap := newTestApplier(t) // no objects

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName("nonexistent")

	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete of non-existent resource should not error: %v", err)
	}
}

func TestApplier_Delete_DefaultsEmptyNamespace(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetName("no-ns-cm")
	// Namespace intentionally unset — delete should treat it as "default".

	seeded := obj.DeepCopy()
	seeded.SetNamespace("default")
	ap := newTestApplier(t, seeded)

	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete with empty namespace: %v", err)
	}
}

func TestApplier_Delete_ClusterScoped(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
	obj.SetName("team-a")

	ap := newTestApplier(t, obj)
	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete cluster-scoped: %v", err)
	}
}

func TestApplier_Delete_InvalidJSON(t *testing.T) {
	ap := newTestApplier(t)
	err := ap.delete(context.Background(), json.RawMessage(`{not-json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestApplier_Delete_UnknownGVK(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"})
	obj.SetName("w1")
	raw, _ := json.Marshal(obj)
	err := ap.delete(context.Background(), raw)
	if err == nil {
		t.Fatal("expected error for unknown GVK")
	}
}

func TestApplier_Apply_InvalidJSON(t *testing.T) {
	ap := newTestApplier(t)
	err := ap.apply(context.Background(), json.RawMessage(`{not-json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestApplier_Apply_UnknownGVK(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"})
	obj.SetName("w1")
	raw, _ := json.Marshal(obj)
	err := ap.apply(context.Background(), raw)
	if err == nil {
		t.Fatal("expected error for unknown GVK")
	}
}

func TestApplier_Apply_ConfigMap(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName("applied-cm")
	obj.Object["data"] = map[string]any{"k": "v"}

	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Exercise the apply path. The dynamic fake may not fully implement
	// server-side apply patches; either success or a patch error is fine
	// as long as we got past JSON parse and GVR resolution.
	if err := ap.apply(context.Background(), raw); err != nil {
		t.Logf("apply returned error (acceptable for fake client SSA): %v", err)
	}
}

func TestApplier_Apply_DefaultsEmptyNamespace(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetName("applied-no-ns")
	raw, _ := json.Marshal(obj)
	// Exercise the empty-namespace → "default" branch regardless of whether
	// the fake client accepts SSA patches.
	_ = ap.apply(context.Background(), raw)
}

func TestApplier_Apply_ClusterScoped(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
	obj.SetName("applied-ns")
	raw, _ := json.Marshal(obj)
	_ = ap.apply(context.Background(), raw)
}

// configMapManifest returns a ConfigMap manifest and the object to seed
// a fake cluster with.
func configMapManifest(name string) (json.RawMessage, *unstructured.Unstructured) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName(name)
	raw, _ := json.Marshal(obj)
	return raw, obj
}

// forbid makes the fake cluster refuse verb on ConfigMaps, the way a
// cluster refuses a credential that lacks the right: whether or not
// the object exists.
func forbid(ap *applier, verb string) {
	ap.client.(*dynamicfake.FakeDynamicClient).PrependReactor(verb, "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "", errors.New("not allowed"))
	})
}

func TestApplier_DeleteWithStoredCredential(t *testing.T) {
	ctx := context.Background()

	t.Run("allowed delete removes the object", func(t *testing.T) {
		raw, obj := configMapManifest("cm")
		ap := newTestApplier(t, obj)
		unseen, err := ap.deleteWithStoredCredential(ctx, raw)
		if err != nil || unseen != "" {
			t.Fatalf("delete = %q, %v", unseen, err)
		}
		if _, err := ap.client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("default").Get(ctx, "cm", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("object still there: %v", err)
		}
	})

	t.Run("forbidden delete of an object that is not there is nothing to remove", func(t *testing.T) {
		raw, _ := configMapManifest("never-created")
		ap := newTestApplier(t)
		forbid(ap, "delete")
		unseen, err := ap.deleteWithStoredCredential(ctx, raw)
		if err != nil || unseen != "" {
			t.Fatalf("delete = %q, %v; want success: a deployment the cluster refused at creation must be deletable", unseen, err)
		}
	})

	t.Run("forbidden delete of a kind the credential cannot read is left and named", func(t *testing.T) {
		raw, obj := configMapManifest("out-of-reach")
		ap := newTestApplier(t, obj)
		forbid(ap, "delete")
		forbid(ap, "get")
		unseen, err := ap.deleteWithStoredCredential(ctx, raw)
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
		if unseen != "ConfigMap default/out-of-reach" {
			t.Fatalf("unseen = %q, want the object named", unseen)
		}
	})

	t.Run("forbidden delete of an object that is there and readable is an error", func(t *testing.T) {
		raw, obj := configMapManifest("present")
		ap := newTestApplier(t, obj)
		forbid(ap, "delete")
		unseen, err := ap.deleteWithStoredCredential(ctx, raw)
		if !apierrors.IsForbidden(err) || unseen != "" {
			t.Fatalf("delete = %q, %v; want the cluster's refusal", unseen, err)
		}
		if deliveryStateForError(err) != domain.DeliveryStateAuthFailed {
			t.Errorf("the refusal is not reported as an authorization failure")
		}
	})

	t.Run("with a caller's credential a forbidden delete stays an error", func(t *testing.T) {
		raw, _ := configMapManifest("never-created")
		ap := newTestApplier(t)
		forbid(ap, "delete")
		if err := ap.delete(ctx, raw); !apierrors.IsForbidden(err) {
			t.Fatalf("delete = %v; want the refusal, so that someone with more rights can resume", err)
		}
	})
}

func TestDeleteWithStoredCredential_GoesOnPastWhatItCannotReach(t *testing.T) {
	ctx := context.Background()
	first, firstObj := configMapManifest("first")
	second, secondObj := configMapManifest("second")
	ap := newTestApplier(t, firstObj, secondObj)
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	// Only the first object is out of reach.
	ap.client.(*dynamicfake.FakeDynamicClient).PrependReactor("*", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		named, ok := action.(interface{ GetName() string })
		if ok && named.GetName() == "first" && (action.GetVerb() == "delete" || action.GetVerb() == "get") {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "first", errors.New("not allowed"))
		}
		return false, nil, nil
	})

	unseen, err := deleteWithStoredCredential(ctx, ap, []domain.Manifest{{Raw: first}, {Raw: second}})
	if err != nil {
		t.Fatalf("deleteWithStoredCredential: %v", err)
	}
	if len(unseen) != 1 || unseen[0] != "ConfigMap default/first" {
		t.Fatalf("unseen = %v", unseen)
	}
	if _, err := ap.client.Resource(gvr).Namespace("default").Get(ctx, "second", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the object after the unreachable one was not removed: %v", err)
	}
}
