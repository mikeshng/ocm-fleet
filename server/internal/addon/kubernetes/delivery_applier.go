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
// namespaced resources with an empty namespace to "default".
func (a *applier) resolveResource(obj *unstructured.Unstructured) (dynamic.ResourceInterface, error) {
	gvk := obj.GroupVersionKind()
	mapping, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("resolve GVR for %s: %w", gvk, err)
	}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		return a.client.Resource(mapping.Resource).Namespace(ns), nil
	}
	return a.client.Resource(mapping.Resource), nil
}

// apply performs a server-side apply of the given raw JSON manifest.
// Namespaced resources default to the "default" namespace when unset.
func (a *applier) apply(ctx context.Context, raw json.RawMessage) error {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	dr, err := a.resolveResource(obj)
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

// delete removes the Kubernetes resource described by the raw JSON manifest.
// Returns nil if the resource is already gone (404).
func (a *applier) delete(ctx context.Context, raw json.RawMessage) error {
	_, err := a.deleteObject(ctx, raw, false)
	return err
}

// deleteWithStoredCredential is delete for a removal that runs with the
// platform's stored credential, whose rights the cluster's side has
// limited. A cluster answers "forbidden" to a delete it does not allow
// whether or not the object exists, and with this credential nobody
// can be asked to try again with more rights. So a forbidden delete is
// looked at more closely:
//
//   - the object is not there: nothing to remove;
//   - the credential may not read objects of this kind either: the
//     cluster's side has put the kind outside what the resource manager
//     may touch. The object is left as it is, and named in unseen so
//     that the caller can say so;
//   - the object is there and readable: the error stands.
//
// Without this a deployment that the cluster refused at creation could
// never be deleted.
func (a *applier) deleteWithStoredCredential(ctx context.Context, raw json.RawMessage) (unseen string, err error) {
	return a.deleteObject(ctx, raw, true)
}

func (a *applier) deleteObject(ctx context.Context, raw json.RawMessage, storedCredential bool) (unseen string, err error) {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return "", fmt.Errorf("parse manifest: %w", err)
	}

	dr, err := a.resolveResource(obj)
	if err != nil {
		return "", err
	}

	gvk := obj.GroupVersionKind()
	deleteErr := dr.Delete(ctx, obj.GetName(), metav1.DeleteOptions{})
	if deleteErr == nil || apierrors.IsNotFound(deleteErr) {
		return "", nil
	}
	if storedCredential && apierrors.IsForbidden(deleteErr) {
		_, getErr := dr.Get(ctx, obj.GetName(), metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			return "", nil
		case apierrors.IsForbidden(getErr):
			return fmt.Sprintf("%s %s/%s", gvk.Kind, obj.GetNamespace(), obj.GetName()), nil
		}
	}
	return "", fmt.Errorf("delete %s %s/%s: %w", gvk.Kind, obj.GetNamespace(), obj.GetName(), deleteErr)
}
