package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

const fieldManager = "resource-manager"

// applier wraps a dynamic client and REST mapper for SSA.
type applier struct {
	client dynamic.Interface
	mapper meta.RESTMapper
	// guard, when set, names the objects a removal leaves as they are
	// (see [DeliveryGuard]).
	guard ObjectGuard
}

// newApplierFromConfig builds an applier from a REST config by creating
// a discovery client, dynamic client, and deferred REST mapper.
func newApplierFromConfig(cfg *rest.Config) (*applier, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

	return &applier{client: dyn, mapper: mapper}, nil
}

// resolveResource maps obj to a dynamic ResourceInterface, defaulting
// namespaced resources with an empty namespace to "default" and returning
// the namespace it resolved to ("" for cluster-scoped resources).
func (a *applier) resolveResource(obj *unstructured.Unstructured) (dynamic.ResourceInterface, string, error) {
	gvk := obj.GroupVersionKind()
	mapping, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, "", fmt.Errorf("resolve GVR for %s: %w", gvk, err)
	}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		return a.client.Resource(mapping.Resource).Namespace(ns), ns, nil
	}
	return a.client.Resource(mapping.Resource), "", nil
}

// apply performs a server-side apply of the given raw JSON manifest.
// Namespaced resources default to the "default" namespace when unset.
func (a *applier) apply(ctx context.Context, raw json.RawMessage) error {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	dr, _, err := a.resolveResource(obj)
	if err != nil {
		return err
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}

	gvk := obj.GroupVersionKind()
	// TODO: consider Force:true so the Kubernetes addon reclaims fields owned by other
	// managers (kubectl, HPA, etc.). Without Force, SSA 409 conflicts fail delivery.
	_, err = dr.Patch(ctx, obj.GetName(), "application/apply-patch+yaml", data, metav1.PatchOptions{
		FieldManager: fieldManager,
	})
	if err != nil {
		return fmt.Errorf("apply %s %s/%s: %w", gvk.Kind, obj.GetNamespace(), obj.GetName(), err)
	}

	return nil
}

// delete removes the Kubernetes resource described by the raw JSON
// manifest, when this resource manager wrote it. It returns the object
// it left in place, with the reason, or an error.
func (a *applier) delete(ctx context.Context, raw json.RawMessage) (left string, err error) {
	return a.deleteObject(ctx, raw, false)
}

// deleteWithStoredCredential is delete for a removal that runs with the
// platform's stored credential, whose rights the cluster's side has
// limited. A cluster answers "forbidden" whether or not the object
// exists, and with this credential nobody can be asked to try again
// with more rights. So a forbidden read is not an error: the credential
// has put the kind outside what the resource manager may touch, the
// object is left as it is, and named in the result.
//
// Without this a deployment that the cluster refused at creation could
// never be deleted.
func (a *applier) deleteWithStoredCredential(ctx context.Context, raw json.RawMessage) (left string, err error) {
	return a.deleteObject(ctx, raw, true)
}

// deleteObject deletes the resource described by the manifest only when
// the cluster's managed-fields record names this addon's field manager.
// A removal can be dispatched for a delivery that failed before it
// applied anything: its manifest then names an object another manager
// wrote, and deleting that object by name would take it away from its
// owner. The managed-fields record is the cluster's own answer to
// whether this resource manager wrote the object; server-side apply
// always records the entry, and the apply path never forces fields
// another manager owns.
//
// A resource that is already gone (404) is nothing to remove. An object
// this resource manager did not write is left in place and named. With
// the caller's credential the refusal to read stands, so a caller with
// more rights can resume.
//
// An object the guard keeps out of reach is left in place and named
// before the cluster is asked anything about it.
func (a *applier) deleteObject(ctx context.Context, raw json.RawMessage, storedCredential bool) (left string, err error) {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return "", fmt.Errorf("parse manifest: %w", err)
	}
	if a.guard != nil {
		if guarded, ok := guardedObject(raw); ok {
			if why := a.guard(guarded); why != "" {
				return fmt.Sprintf("%s (out of reach of deliveries on this cluster: %s)", guarded, why), nil
			}
		}
	}

	dr, ns, err := a.resolveResource(obj)
	if err != nil {
		return "", err
	}

	gvk := obj.GroupVersionKind()
	name := fmt.Sprintf("%s %s", gvk.Kind, obj.GetName())
	if ns != "" {
		name = fmt.Sprintf("%s %s/%s", gvk.Kind, ns, obj.GetName())
	}
	current, err := dr.Get(ctx, obj.GetName(), metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return "", nil
	case apierrors.IsForbidden(err) && storedCredential:
		return name + " (the stored credential may neither read nor delete it)", nil
	case err != nil:
		return "", fmt.Errorf("read %s: %w", name, err)
	}
	if !writtenByResourceManager(current) {
		return name + " (not written by this resource manager)", nil
	}

	deleteErr := dr.Delete(ctx, obj.GetName(), metav1.DeleteOptions{})
	if deleteErr == nil || apierrors.IsNotFound(deleteErr) {
		return "", nil
	}
	return "", fmt.Errorf("delete %s: %w", name, deleteErr)
}

// writtenByResourceManager reports whether the cluster attributes at
// least one field of obj to this addon's field manager.
func writtenByResourceManager(obj *unstructured.Unstructured) bool {
	for _, mf := range obj.GetManagedFields() {
		if mf.Manager == fieldManager {
			return true
		}
	}
	return false
}
