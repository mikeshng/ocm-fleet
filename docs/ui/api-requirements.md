# Console API Requirements and Current Surface

Platform APIs let the console query fleet-wide inventory without browser-side fan-out to every target, while keeping live-target operations on a separate access path. The console consumes those APIs without owning target connectivity, credential validation, or authorization; this reference separates the implemented browser-facing surface from established architectural direction.

## Fleet-wide resource queries

The shared resource client queries platform-held resources through:

```text
GET /apis/open-cluster-management.io/v1alpha/{scope}:queryResources
```

Current callers use `-` for whole-platform scope. The query accepts a CEL filter, page size, page token, and ordering, and returns resource results plus a continuation token. A result contains resource identity and the addon-defined resource body; it is not a universal normalized Kubernetes object.

The core pod, namespace, and node views query Kubernetes observations through this surface. Their hook follows result pages and filters by resource type, kind, and optionally cluster identity. It does not fan out to each cluster from the browser.

The cluster list, the cluster page, and the overview read cluster records of three types through the same surface: clusters a provider created, clusters that were enrolled, and the clusters of connected hubs. A shared helper folds them into one row per cluster, so a cluster that a hub also reports appears once. Only created and enrolled clusters have indexed Kubernetes observations; a cluster that is only observed through a hub shows what the hub holds. The hubs page reads the records of connected hubs and of their clusters the same way.

Sources:

- [Resource query client and response/error types](../../sdk/common/src/resourceApi.ts)
- [Core resource fetching](../../extensions/core/client/src/plugins/core-plugin/useClusterResources.ts)
- [Cluster rows of every origin](../../sdk/common/src/objects/fleetCluster.ts)
- [Inventory query and live-object boundary](../design/architecture/resource_indexing.md#search-api-shape)

Fleet-wide pages need bounded queries, pagination, and enough resource identity to link results to their owning views. Addons own their resource schemas and extracted observations. The current API does not establish automatic normalization of every Kubernetes or OpenShift version into one resource shape.

## Resource operations

Plugins can call typed resource APIs as well as the cross-resource query API. For example, the Kind wizard creates a managed cluster through:

```text
POST /apis/kind.open-cluster-management.io/v1alpha/clusters?cluster_id={id}
```

Its request contains structured cluster specification fields, including nodes and networking. It is not the former Express-proxied inline deployment flow.

Sources: [Kind API client](../../extensions/kind/client/src/api.ts), [wizard submission](../../extensions/kind/client/src/CreateClusterWizard.tsx).

The Targets page lists the registered targets, with their state, origin, access form, and labels, through:

```text
GET /apis/open-cluster-management.io/v1alpha/targets
```

The OCM hubs plugin calls the operations that the resource manager's OCM add-on serves under `/apis/ocm.open-cluster-management.io/v1alpha`. It reads the manifest that a hub's administrator applies before connecting, connects and disconnects a hub, and asks for the manifests that enroll a hub's clusters. The console shows those manifests for an administrator to apply; it does not write to a hub or to a cluster. Connected hubs and their clusters are read through the resource query API, not through these operations.

The core plugin calls the Kubernetes add-on's enrollment operations under `/apis/kubernetes.open-cluster-management.io/v1alpha`: renewing an enrolled cluster's credential (`POST clusters/{id}:renewCredential`), withdrawing one (`POST clusters/{id}:withdraw`, retried with `leaveDeployments` when deployments still resolve to it), forgetting a withdrawn record (`DELETE clusters/{id}`), and generating a manifest bundle for a cluster with no hub (`POST enrollmentBundles`, with `DELETE enrollmentBundles/{id}` to revoke it). These handlers answer a failure with the add-on's own envelope `{error, reason, details}`, not an RPC status. The shared client keeps that body, so a caller reads the server's sentence, the stable reason word, and the details — the deployment names and target of a withdrawal refusal, for example.

Sources: [targets client](../../extensions/core/client/src/plugins/management-plugin/api.ts), [hub operations client](../../extensions/ocm/client/src/api.ts), [enrollment client](../../extensions/core/client/src/plugins/core-plugin/enrollmentApi.ts), [withdrawal dialog](../../extensions/core/client/src/plugins/core-plugin/WithdrawClusterModal.tsx), [manifest enrollment dialog](../../extensions/core/client/src/plugins/core-plugin/EnrollManifestModal.tsx).

## Live target access: established design direction

Platform inventory contains what reporters sent to the platform. Reading a full live target object or using watch, exec, attach, and log streaming is a separate access path, not a richer variant of the inventory query.

The [fleetlet transport design](../design/architecture/fleetlet_and_transport.md#kubernetes-api-proxy) describes transparent Kubernetes API tunneling for these operations. This is architectural direction, not a claim that the console currently provides the full proxy surface. No production browser proxy URL convention is established here.

Plugins need an explicit target context and an authorized route to the relevant API. UI visibility and knowledge of a cluster ID do not themselves grant target access. Credential presentation and authorization follow the platform's [authentication design](../design/authentication.md).

## Refresh and streaming

Current resource views fetch snapshots. Some views poll: the cluster list and detail views refresh transient state, and the overview refreshes fleet data periodically. The console does not currently provide a functioning shell-wide resource-topic subscription bus.

The Go server does expose `/api/ui/events/ws` for delivery events. It is a broadcaster, not the removed prototype's informer-topic protocol or authenticated ticket handshake. Its authentication remains explicitly unfinished in bootstrap code; it is not a production-ready subscription contract.

Sources:

- [Cluster list polling](../../extensions/core/client/src/plugins/core-plugin/ClustersPage.tsx)
- [Overview refresh](../../extensions/core/client/src/plugins/overview-plugin/useFleetData.ts)
- [Delivery event stream](../../server/internal/transport/http/event_ws.go)
- [WebSocket route and authentication TODO](../../server/internal/bootstrap/ui.go)
- [Shell subscription stub](../../client/web/src/components/Root/ScalprumShell.tsx)

Live updates remain useful for status and logs. This document does not select a new browser streaming protocol, prescribe server informers, or promise a watch RPC. Any production subscription contract must define authorization, reconnect behavior, and loss/recovery semantics before consumers rely on it.
