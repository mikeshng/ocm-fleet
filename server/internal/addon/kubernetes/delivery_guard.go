package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// GuardedObject is what a delivery guard is told about one object of a
// delivery or of a removal. All of it is read from the manifest, so
// every object of a delivery can be judged before the first one is
// applied.
type GuardedObject struct {
	// Group and Kind are the object's API group and kind.
	Group string
	Kind  string
	// Namespace is the namespace the manifest names for the object. For
	// a Namespace object it is the object's own name: applying or
	// deleting the Namespace object acts on the namespace.
	Namespace string
	Name      string
}

// String names the object the way delivery messages do.
func (o GuardedObject) String() string {
	if o.Namespace != "" && !o.isNamespace() {
		return fmt.Sprintf("%s %s/%s", o.Kind, o.Namespace, o.Name)
	}
	return fmt.Sprintf("%s %s", o.Kind, o.Name)
}

func (o GuardedObject) isNamespace() bool { return o.Group == "" && o.Kind == "Namespace" }

// ObjectGuard says why an object is out of reach of deliveries on one
// cluster. It returns "" for an object that is not.
type ObjectGuard func(GuardedObject) string

// DeliveryGuard keeps deliveries and removals out of what is not a
// workload on a cluster: the components that manage the cluster, which
// a deployment must be able neither to change nor to take away.
//
// A delivery that names such an object fails with the guard's reason
// and applies nothing. A removal leaves such an object as it is and
// names it, like an object another manager wrote.
type DeliveryGuard interface {
	// GuardTarget returns the guard for the target's cluster, or nil
	// when it keeps nothing out of reach there. It is asked once for
	// each delivery and each removal.
	GuardTarget(ctx context.Context, target domain.TargetInfo) ObjectGuard
}

// AddGuard registers a guard that is asked before every delivery and
// every removal. Guards are registered while the server is assembled.
func (a *DeliveryAgent) AddGuard(g DeliveryGuard) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.guards = append(a.guards, g)
}

// guardFor returns the guard for one delivery to, or removal from, the
// target: the agent's own rule and every registered guard. The first
// reason found answers.
func (a *DeliveryAgent) guardFor(ctx context.Context, target domain.TargetInfo) ObjectGuard {
	a.mu.RLock()
	guards := append([]DeliveryGuard{accessGuard{}}, a.guards...)
	a.mu.RUnlock()
	var rules []ObjectGuard
	for _, g := range guards {
		if rule := g.GuardTarget(ctx, target); rule != nil {
			rules = append(rules, rule)
		}
	}
	return func(obj GuardedObject) string {
		for _, rule := range rules {
			if why := rule(obj); why != "" {
				return why
			}
		}
		return ""
	}
}

// accessGuard is the agent's own rule. On an enrolled cluster whose
// access account lives in the access namespace, that namespace is not a
// place for deliveries: a deployment that could write there could
// change what this resource manager may do on the cluster, and one that
// could delete there could end it.
type accessGuard struct{}

func (accessGuard) GuardTarget(_ context.Context, target domain.TargetInfo) ObjectGuard {
	if !IsEnrolledTarget(target) {
		return nil
	}
	sa, ok := ParseServiceAccountRef(target.Properties()[PropServiceAccount])
	if !ok || sa.Namespace != AccessNamespace {
		return nil
	}
	return func(obj GuardedObject) string {
		if obj.Namespace != AccessNamespace {
			return ""
		}
		return fmt.Sprintf("namespace %q holds this resource manager's own access to the cluster", AccessNamespace)
	}
}

// guardedObject reads from a manifest what a guard is told about it.
func guardedObject(raw json.RawMessage) (GuardedObject, bool) {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return GuardedObject{}, false
	}
	gvk := obj.GroupVersionKind()
	out := GuardedObject{Group: gvk.Group, Kind: gvk.Kind, Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if out.isNamespace() {
		out.Namespace = obj.GetName()
	}
	return out, true
}

// refusedByGuard returns the first manifest a guard keeps out of reach,
// with the guard's reason. found is false when there is none. A
// manifest that cannot be read is not judged here: applying or deleting
// it reports what is wrong with it.
func refusedByGuard(guard ObjectGuard, manifests []domain.Manifest) (index int, obj GuardedObject, why string, found bool) {
	if guard == nil {
		return 0, GuardedObject{}, "", false
	}
	for i, m := range manifests {
		o, ok := guardedObject(m.Raw)
		if !ok {
			continue
		}
		if why := guard(o); why != "" {
			return i, o, why, true
		}
	}
	return 0, GuardedObject{}, "", false
}
