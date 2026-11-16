# Existing clusters and OCM hubs: HTTP API

This is the OCM Fleet-specific reference for the HTTP requests that take in
existing Kubernetes clusters and connect Open Cluster Management (OCM) hubs:
the path, the request body, the answer, and every error with its `reason`
word.

## What this doc covers

- the three API prefixes these requests are served under
- how a request is authenticated, and the three shapes an authentication
  failure takes
- every request, with its body fields, its answer, and its errors
- the records the reads return
- a sequence of examples that runs as written, in one shell
- what an operator must know about security
- troubleshooting by `reason` word

## When to read this

Read this when you call these requests from a script or a terminal, when you
are writing a client for them, or when a refusal needs to be understood. For
the concepts behind them read [the design
document](design/addons/existing-clusters-and-ocm-hubs.md); for a guided run
from a fresh clone read [the
walkthrough](../deploy/aio/existing-clusters-walkthrough.md).

## What is intentionally elsewhere

- Design of enrollment and of the OCM hub integration: [design/addons/existing-clusters-and-ocm-hubs.md](design/addons/existing-clusters-and-ocm-hubs.md)
- Delivery to and indexing of the cluster once it is a target: [design/addons/kubernetes-inventory-and-delivery.md](design/addons/kubernetes-inventory-and-delivery.md)
- The query language the examples use for the inventory: [query-resources-cel-filters.md](query-resources-cel-filters.md)
- The console surface and its API use: [ui/api-requirements.md](ui/api-requirements.md)
- API design conventions: [api-design.md](api-design.md)
- Credential presentation and `PausedAuth`: [design/authentication.md](design/authentication.md)
- Running the all-in-one sandbox these examples work against: [../deploy/aio/README.md](../deploy/aio/README.md#existing-clusters-and-ocm-hubs)

## Common form

### Paths

Every request is served under one of three prefixes.

| Prefix | Value                                                 | Serves                                                  |
| ------ | ----------------------------------------------------- | ------------------------------------------------------- |
| `K8S`  | `/apis/kubernetes.open-cluster-management.io/v1alpha` | the access manifest, enrollment, and enrollment bundles |
| `OCM`  | `/apis/ocm.open-cluster-management.io/v1alpha`        | hub connections and the hub access manifest             |
| `CORE` | `/apis/open-cluster-management.io/v1alpha`            | targets and the inventory query                         |

The reads of cluster, bundle, and hub records and the inventory query are
served by the platform's inventory API; the custom methods (`:enroll`,
`:withdraw`, `:renewCredential`, `:present`, `:connect`, `:disconnect`,
`:generateEnrollmentAddon`, `:adoptCluster`) are served by the addons. The
examples below write the prefixes as the shell variables `$K8S`, `$OCM`, and
`$CORE`.

### Authentication

Every request but `enrollments:present` needs a bearer token from the
resource manager's identity provider: `Authorization: Bearer <token>`. The
answer to a missing or unacceptable token depends on where the request lands.

| Where the request lands | Requests                                                                                                                    | No credential                                              | Not a valid token                                                               |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Request handlers        | `accessManifest`, `hubAccessManifest`, `hubs/{id}/observation`, every `POST` and `DELETE`, `targets`                        | 401 `{"error":"missing bearer token"}`                     | 401 `{"error":"token verification failed"}`                                     |
| Record reads            | `GET` of `clusters`, `clusters/{id}`, `enrollmentBundles`, `enrollmentBundles/{id}`, `hubs`, `hubs/{id}`, `managedClusters` | 401 `text/plain`, body `unauthenticated`                   | 401 `text/plain`, body `token verification failed: parse/verify token: <cause>` |
| The query surface       | `-:queryResources`                                                                                                          | 401 `{"code":16,"message":"unauthenticated","details":[]}` | 401 with the same object and the `message` above                                |

The record-read body names the verifier's cause after the fixed prefix. When
the value parses as a JWT but fails verification — expired, wrong issuer or
audience, bad signature — the cause ends with
`[expected: iss=..., aud=..., got: ...]`; the query surface carries the same
text as its `message`.

`POST .../enrollments:present` is the one public request: a cluster's side
shows its enrollment secret as the bearer value. An absent secret answers 401
`{"error":"an enrollment bundle secret is required","reason":"bundle-missing"}`;
a value that is not shaped like a bundle secret (`<bundle id>.<secret>`) answers
401 `{"error":"the enrollment bundle secret is not valid","reason":"bundle-malformed"}`;
a well-formed value naming an unknown bundle, carrying the wrong secret, or
whose bundle has expired answers `bundle-unknown`, `bundle-secret-mismatch`, or
`bundle-expired`. Failed presentations are limited per client address and, once
the secret has checked out, per cluster name under a bundle: after 20 failures
in a minute the answer is 429
`{"error":"too many failed enrollment attempts; try again later","reason":"rate-limited"}`
with `Retry-After: 60`. Wrong secrets aimed at a bundle that exists are counted
and logged as a sign that someone is guessing, but that count never refuses a
presentation: a bundle id is not a secret, so it cannot be used to shut out the
clusters presenting under one. Behind a reverse proxy that does not pass the
client address along, every presenter shares the proxy's address and so one
limit.

### Bodies and answers

- Request bodies are JSON, sent with `Content-Type: application/json`. A field
  the request does not define is refused; so is a body over 1 MiB. An empty
  body is an empty request. The routes that take no input — `renewCredential`,
  `forget`, `revoke`, and `disconnect` — do not read the body at all; anything
  sent with them is ignored.
- Answers are JSON. Answers the addons and `targets` write carry `Cache-Control:
no-store`; the record reads and the query surface set no `Cache-Control`. A
  time is an RFC 3339 string, a number is a number, a condition's status is
  `"True"` or `"False"`, and a field that does not apply is left out — so an
  empty record list answers `{}`, while the query surface and `targets` write
  their empty lists: `{"resources":[],"nextPageToken":""}` and
  `{"targets":[]}`.
- The addon routes answer an error in the same envelope:

```text
{"error": "<message>", "reason": "<word>", "details": {...}}
```

`details` is present only when the failure has some. The three authentication
shapes above have no `reason` (the two query-surface shapes use `code`/`message`
instead), and neither does the `targets` failure `{"error":"list targets"}`.
The record reads are not addon routes: every failure there is plain text
(`Content-Type: text/plain`). Every other error has one of these status and
`reason` pairs:

| Status | `reason`           | Means                                                                                                      |
| ------ | ------------------ | ---------------------------------------------------------------------------------------------------------- |
| 400    | `invalid-argument` | the request is malformed, or asks for something that does not exist                                        |
| 401    | `unauthenticated`  | no acceptable credential                                                                                   |
| 403    | `refused`          | the credential is fine, but the request is not allowed                                                     |
| 404    | `not-found`        | the named record or connection does not exist                                                              |
| 409    | `conflict`         | the request conflicts with what exists (the request-specific word says how)                                |
| 422    | `unverified`       | what was presented could not be verified against the cluster or hub                                        |
| 429    | `rate-limited`     | too many failed presentations from one address, or under one cluster name, in a minute; see Authentication |
| 500    | `internal`         | the resource manager failed; the detail is in its log                                                      |

A request-specific word replaces the generic one; the requests below list
them. The envelope, the 422 status, and the lowercase reason words are a
deliberate exception to AIP-193, which the gRPC API and the inventory query
follow (see [API design](api-design.md#deliberate-exceptions)): these routes
are the add-ons' own surface, and their callers — the enrollment agent, a hub's
add-on template, and an administrator reading a reason in a terminal — are
served by this vocabulary. 422 `unverified` says the request is well-formed and
authenticated but what the cluster or hub presented could not be verified: 400
would misdescribe the request, and nothing stored conflicts, so 409 would too.

### Records

The reads answer inventory records. Every record has this envelope:

| Field                                | Meaning                                                                                                                             |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `name`                               | the record's name, `clusters/<id>`, `enrollmentBundles/<id>`, or `hubs/<id>`                                                        |
| `uid`                                | the record's identity, stable across updates                                                                                        |
| `labels`                             | labels on the record, when it has any                                                                                               |
| `localLabels`                        | labels last reported by the cluster or hub for the observed object, when the report has any; the record's own `labels` are separate |
| `createTime`, `updateTime`           | when the record was made and last changed                                                                                           |
| `etag`                               | changes when the record changes                                                                                                     |
| `conditions`                         | a map from condition type to `{status, reason, message, lastTransitionTime}`                                                        |
| `observation`                        | what the record says about the thing it stands for                                                                                  |
| `localUpdateTime`, `indexUpdateTime` | when the platform last observed it and last indexed it                                                                              |

A cluster record's observation has `name`, `state` (`enrolled`,
`needs-enrollment`, or `withdrawn`), `origin`, `accessForm`, `apiServer`,
`caFingerprint`, `clusterUID`, `target`, `kubernetesVersion`, `serviceAccount`,
`profile`, `rights` (`canRenew`, `clusterAdmin`, `namespaces`, `readCluster`,
`workloads`), `deliveryCredential`, `credentialExpiresAt`,
`credentialRenewedAt`, `enrolledAt`, `enrolledBy`, `enrollmentBundle`, `source`,
`withdrawnAt`, `withdrawnBy`, `withdrawnReason`, and `searchTerms`. The optional
fields are absent when they do not apply. `clusterUID` is the UID of the
cluster's `kube-system` namespace, read when the cluster presented itself: it
identifies the cluster at whatever address it is reached, and is absent where
the presented credential may not read that namespace. The record of a cluster
whose target carries a hub's labels also has the condition `HubFactsCurrent`:
`True` with reason `Current` while the hub supplies them, and `False` with
`HubUnreachable`, `HubDisconnected`, or `NotReportedByHub` when it does not;
the labels then stay as last seen, and deployments still select the cluster by
them.

A hub record's observation has `id`, `displayName`, `apiServer`,
`caFingerprint`, `clusterUID`, `serviceAccount`, `writeAccessFound`,
`clusterCount`, `acceptedCount`, `availableCount`, `enrolledCount`,
`enrolledThroughHubCount`, `enrollmentRollout`, `enrollmentSecrets`,
`connectedAt`, `connectedBy`, `credentialExpiresAt`, `lastSeenAt`, and
`searchTerms`. Its conditions are `Connected`, `CredentialValid`, `Ready`, and
`Synced`. `clusterUID` identifies the hub the same way, and is absent for a
connection whose account was made before the access manifest let it read that
namespace. `enrollmentRollout` (`mode`, and for a Placement
`placementNamespace` and `placementName`) says which clusters the enrollment
add-on last generated for this connection reaches; it is what the generated
manifest says, not a reading of the hub. `enrollmentSecrets` counts the valid
enrollment bundles scoped to the hub, and `enrolledThroughHubCount` the
clusters that enrolled through its add-on: with clusters and no valid bundle,
the add-on on the hub carries a secret that is no longer accepted and has to be
generated and applied again.

An enrollment bundle record's observation has `id`, `route` (`manifest` or
`addon`), `scope` (`clusterId` and `apiServer`, or `verifier` and
`verifierScope`), `profile`, `clusterRole`, `grantPlatformDelivery`, `labels`,
`tokenLifetimeSeconds` (the credential lifetime in seconds: 12 hours when the
request left it unset, at least 10 minutes, at most 24 hours), `createdBy`,
`createdAt`, `expiresAt` (absent when the bundle does not expire), `uses`, and
`lastUsedAt`.

#### Listing records

The record lists (`clusters`, `enrollmentBundles`, `hubs`, and
`managedClusters`) are paged. The request takes `pageSize` and `pageToken`,
the spellings the rest of the API uses (AIP-127). The proto spellings
`page_size` and `page_token` answer 400 rather than being ignored: a client
that meant to page would otherwise get a silently unpaged answer.

- `pageSize` — how many records one answer carries: 50 when absent or zero,
  at most 500; a larger value reads as 500. A negative value answers 400
  `text/plain` `invalid page_size: must not be negative`.
- `pageToken` — the previous answer's `nextPageToken`; absent for the first
  page.

The list is in record-name order. While records remain, the answer carries
`nextPageToken` beside the collection; pass it back as `pageToken` until it is
absent. The token is the last record's name in base64url, so a record deleted
between two calls does not move the records after it: the next page resumes
after the name the token carries. A token that is not base64url text, or
decodes to nothing, answers 400 `text/plain` `invalid page_token: ...`; any
other value is read as a record name to resume after, so pass back only a
token an answer gave you. A `pageSize` that is not a number answers 400
`text/plain` `invalid pageSize: ...`, and the proto spellings answer 400
`text/plain` `unknown query parameter ...`.

## Examples, in order

The examples below are one shell session in this order: each request is shown
where it is documented, and a later block uses what an earlier one left. They
assume:

- a resource manager at `https://ocm-fleet.localhost:8085`, with its
  certificate at `$PT/ca.crt` (the all-in-one container's own; see [the
  all-in-one README](../deploy/aio/README.md#existing-clusters-and-ocm-hubs));
- a hub whose read-only account was created from the `hubAccessManifest`
  request (the block below does that), and the hub administrator's kubeconfig;
- a cluster that is a managed cluster of that hub, so the hub knows it, and the
  cluster administrator's kubeconfig;
- the enrollment agent image available to that cluster, as [the walkthrough's
  step
  0.4](../deploy/aio/existing-clusters-walkthrough.md#04-build-the-images)
  shows.

Set the names and paths first. The first block only defines names and
functions and signs in.

```bash
HUB_ID=${HUB_ID:-my-hub}
CLUSTER_ID=${CLUSTER_ID:-my-cluster}
HUB_APISERVER=${HUB_APISERVER:-https://my-hub-control-plane:6443}
CLUSTER_APISERVER=${CLUSTER_APISERVER:-https://my-cluster-control-plane:6443}
KUBECONFIG_HUB=${KUBECONFIG_HUB:-$HOME/my-hub.kubeconfig}
KUBECONFIG_CLUSTER=${KUBECONFIG_CLUSTER:-$HOME/my-cluster.kubeconfig}
PT=${PT:-$(mktemp -d)}; mkdir -p "$PT"

FLEET=https://ocm-fleet.localhost:8085
K8S=/apis/kubernetes.open-cluster-management.io/v1alpha
OCM=/apis/ocm.open-cluster-management.io/v1alpha
CORE=/apis/open-cluster-management.io/v1alpha
CURL=(curl -sS --cacert "$PT/ca.crt" --resolve ocm-fleet.localhost:8085:127.0.0.1)

# fleet_login: sign in; the token stays in this shell.
fleet_login() {
  TOKEN=$("${CURL[@]}" -X POST $FLEET/idp/token -d grant_type=password \
    -d username=ops@ocm-fleet.local -d password=ocm-fleet-ops -d client_id=fleetctl \
    -d 'scope=openid profile email groups audience:server:client_id:resource-manager' | jq -r .id_token)
  if [ -n "$TOKEN" ] && [ "$TOKEN" != null ]; then echo "signed in"; else echo "sign-in failed"; fi
}
# fleet METHOD PATH [JSON]: one call with the token.
fleet() {
  local method=$1 path=$2 body=${3:-}
  if [ -n "$body" ]; then
    "${CURL[@]}" -X "$method" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data-binary "$body" "$FLEET$path"
  else
    "${CURL[@]}" -X "$method" -H "Authorization: Bearer $TOKEN" "$FLEET$path"
  fi
  echo
}
# query FILTER [PAGE_SIZE]: the inventory query.
query() {
  "${CURL[@]}" -G -H "Authorization: Bearer $TOKEN" --data-urlencode "filter=$1" \
    --data-urlencode "pageSize=${2:-50}" "$FLEET$CORE/-:queryResources"
  echo
}
kc_hub() { kubectl --kubeconfig "$KUBECONFIG_HUB" "$@"; }
kc_cluster() { kubectl --kubeconfig "$KUBECONFIG_CLUSTER" "$@"; }
fleet_login
```

```text
signed in
```

The rest of this section shows the three authentication answers once each, so
that a failure is recognizable. Nothing here needs the token. The query
surface's answer goes through `jq -c`: the spacing inside that object is not
fixed and can differ from one build of the server to the next.

```bash
echo "a request handler without a token:"
curl -sS --cacert "$PT/ca.crt" --resolve ocm-fleet.localhost:8085:127.0.0.1 "$FLEET$K8S/accessManifest"
echo; echo "a record read without a token:"
curl -sS --cacert "$PT/ca.crt" --resolve ocm-fleet.localhost:8085:127.0.0.1 "$FLEET$K8S/clusters"
echo; echo "the query surface without a token:"
curl -sS --cacert "$PT/ca.crt" --resolve ocm-fleet.localhost:8085:127.0.0.1 "$FLEET$CORE/-:queryResources" | jq -c .
```

```text
a request handler without a token:
{"error":"missing bearer token"}
a record read without a token:
unauthenticated
the query surface without a token:
{"code":16,"message":"unauthenticated","details":[]}
```

## Hubs

### GET hubAccessManifest

```text
GET /apis/ocm.open-cluster-management.io/v1alpha/hubAccessManifest
```

The manifest of the read-only account a hub's administrator applies: a
namespace of its own, `ocm-fleet-hub-connection`, a service account in it, a
cluster role that gets, lists, and watches `managedclusters` and
`managedclustersets` and gets the one namespace `kube-system` (whose UID tells
one hub from another), and the bindings and role that let the account request
its own tokens. Every object carries the label
`app.kubernetes.io/part-of=ocm-fleet-hub-connection`, which differs from the
label of a cluster's access piece: a hub that is registered with itself can be
enrolled like any cluster, and removing that cluster's access piece must not
remove the hub's connection. The answer is
`{"manifest": "<YAML>", "instructions": ["...", "...", "...", "..."]}`. The
instructions are the steps a person follows; the second one requests the
account's token, and the fourth ends the connection from the hub's side. This
request has no parameters and no request-specific errors.

The block applies the manifest to the hub and keeps the file for the
connection. On a hub where the account already exists, `kubectl apply` reports
the objects as configured or unchanged.

```bash
fleet GET $OCM/hubAccessManifest > "$PT/hub-access.json"
jq -r '.instructions[]' "$PT/hub-access.json"
jq -r .manifest "$PT/hub-access.json" > "$PT/hub-access.yaml"
kc_hub apply -f "$PT/hub-access.yaml"
```

```text
Apply the manifest to the hub as its administrator: kubectl apply -f <file>
Request a credential for the account: kubectl -n ocm-fleet-hub-connection create token ocm-fleet-hub-reader --duration=720h0m0s
Connect the hub with its API address, its CA certificate, and that credential (POST /apis/ocm.open-cluster-management.io/v1alpha/hubs:connect). The resource manager renews the credential by itself and writes nothing to the hub.
To end the connection from the hub's side, delete namespace ocm-fleet-hub-connection and the cluster role and binding named ocm-fleet-hub-reader.
```

The instructions are the same on every resource manager; the third line is
what the connect request below does.

### POST hubs:connect

```text
POST /apis/ocm.open-cluster-management.io/v1alpha/hubs:connect
```

| Field                          | Meaning                                                                                                                                   |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `hubId`                        | the id this resource manager gives the hub: 1 to 40 lowercase letters, digits, or `-`, starting and ending with a letter or digit, no dot |
| `displayName`                  | a single-line name for the console, at most 120 bytes (UTF-8)                                                                             |
| `kubeconfig`                   | the administrator's kubeconfig for the hub, as text; a way to give the address, CA, and credential in one field                           |
| `apiServer`, `caCert`, `token` | the alternative to `kubeconfig`: the hub's API address, its PEM CA certificate, and a token of the read-only account                      |

Give either `kubeconfig` or the three fields. A `kubeconfig` is accepted only
with a service account's bearer token and an inline CA
(`certificate-authority-data`); a client certificate, a token file, an exec
plugin, `insecure-skip-tls-verify`, or a CA given as a file is refused. A
`token` given with `apiServer` and `caCert` must be a service account's as
well: a credential that belongs to a user is refused with 422
`not-a-service-account`, because the resource manager renews the hub credential
itself and only a service account's token allows that. The
connection is tried at once: the resource manager asks the hub who the
credential is and what it may do. The credential must be allowed to list and
watch `ManagedCluster` objects. A credential that may also update or delete
them, or create `ManifestWork` objects, is accepted and reported in the
observation's `writeAccessFound`; it is not refused. The
answer is `{"hub": <hub record>}`, with the first observation and `Connected`,
`CredentialValid`, and `Ready` conditions.

A hub is one connection. The resource manager reads the UID of the hub's
`kube-system` namespace and keeps it as the hub's identifier. A connection with
the same `hubId` to the same hub replaces the stored credential and keeps what
was observed, also when the hub is now given at another address or with another
CA; different cluster lists are read again. The same hub under another `hubId`
is refused, at whatever address it is given. Where an identifier is missing on
either side (an account made from an older access manifest may not read the
namespace), the address and CA decide instead: the same `hubId` must then name
the same address and CA.

Several hubs can be connected at once, each under its own `hubId`. Their
records, credentials, enrollment bundles, and add-ons are separate, and a
cluster of a hub is named `<hubId>.<name>`, so two hubs that each have a cluster
of one name have two clusters here.

Connecting is how the resource manager learns a hub's clusters. Nothing is
written to the hub; the credential is kept in the resource manager's secret
store and renewed by the resource manager itself.

| Status | `reason`                | When                                                                                                                                               |
| ------ | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| 409    | `hub-id-in-use`         | the id belongs to a connection to another hub: disconnect it first or choose another id                                                            |
| 409    | `hub-already-connected` | this hub is connected under another id, at this address or at another one; `details.hub` names the record                                          |
| 422    | `credential-rejected`   | the hub does not recognize the credential                                                                                                          |
| 422    | `not-a-hub`             | the address serves no ManagedCluster API                                                                                                           |
| 422    | `hub-unreachable`       | the address or the CA is wrong, or the hub does not answer                                                                                         |
| 422    | `access-denied`         | the credential may not list and watch ManagedCluster objects                                                                                       |
| 422    | `not-a-service-account` | the credential belongs to another kind of identity, so it cannot be renewed                                                                        |
| 400    | `invalid-argument`      | a field is missing, both a kubeconfig and the three fields are given, the kubeconfig is not in the accepted form, or a name breaks the rules above |

```bash
HUB_CA=$(kc_hub config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d)
HUB_TOKEN=$(kc_hub -n ocm-fleet-hub-connection create token ocm-fleet-hub-reader --duration=720h)
fleet POST $OCM/hubs:connect "$(jq -cn \
  --arg id "$HUB_ID" --arg name "My hub" --arg api "$HUB_APISERVER" \
  --arg ca "$HUB_CA" --arg token "$HUB_TOKEN" \
  '{hubId:$id, displayName:$name, apiServer:$api, caCert:$ca, token:$token}')" \
  | jq -c '{hub: .hub.name, reason, error}'

n=0; until [ "$(fleet GET $OCM/hubs/$HUB_ID | jq -r .conditions.Ready.status 2> /dev/null)" = True ]; do
  (( ++n > 60 )) && { echo "gave up after two minutes: the hub is not ready"; break; }
  sleep 2
done; (( n <= 60 )) && echo "hub is connected"
```

```text
{"hub":"hubs/my-hub","reason":null,"error":null}
hub is connected
```

The address in `HUB_APISERVER` is the hub's API as the resource manager should
dial it; set it before the first block if your hub answers elsewhere.

### GET hubs and hubs/{id}

```text
GET /apis/ocm.open-cluster-management.io/v1alpha/hubs
GET /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}
```

The list answers `{"hubs": [<record>, ...]}`; an empty list answers `{}`. One
hub answers its record; a missing one answers 404 `text/plain` `not found`. The
list is paged: `pageSize` and `pageToken` in the request, `nextPageToken` in
the answer while more hubs remain (see [Listing records](#listing-records)).

```bash
fleet GET $OCM/hubs | jq -c '[.hubs[] | {name, connected: .conditions.Connected.status, account: .observation.serviceAccount}]'
fleet GET $OCM/hubs | jq -c '[.hubs[] | {name}]'
fleet GET $OCM/hubs/$HUB_ID | jq -c '.observation | {id, displayName, serviceAccount, writeAccessFound}'
fleet GET $OCM/hubs/no-such-hub
```

```text
[{"name":"hubs/my-hub","connected":"True","account":"ocm-fleet-hub-connection/ocm-fleet-hub-reader"}]
[{"name":"hubs/my-hub"}]
{"id":"my-hub","displayName":"My hub","serviceAccount":"ocm-fleet-hub-connection/ocm-fleet-hub-reader","writeAccessFound":false}
not found
```

The second line is the same list narrowed to its names; with no hub it prints
`[]`, while the request itself answers `{}`.

`writeAccessFound` is the resource manager's own probe: the hub is asked
whether the credential may update or delete `ManagedCluster` objects or create
`ManifestWork` objects. It is `false` with the manifest above. The integration
uses none of those permissions; it lists and watches `ManagedCluster` objects
and writes nothing to the hub.

### GET hubs/{id}/observation

```text
GET /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}/observation
```

A connected hub answers its observation as the watch has it now; a hub that is
not connected answers 404 `text/plain` `not found`. The record that
`GET hubs/{id}` answers carries the same observation, but the record is
written at most every few minutes when only the last-heard time changes: a
view that shows that time reads the observation instead.

```bash
fleet GET $OCM/hubs/$HUB_ID/observation | jq -c '{id, hasLastSeenAt: (.lastSeenAt != null)}'
```

```text
{"id":"hub2","hasLastSeenAt":true}
```

### GET managedClusters

```text
GET /apis/ocm.open-cluster-management.io/v1alpha/managedClusters
GET /apis/ocm.open-cluster-management.io/v1alpha/hubs/{hub}/managedClusters/{name}
```

Every cluster a connected hub reports is a `ManagedCluster` record, named
`hubs/{hub}/managedClusters/{name}`. The list answers
`{"managedClusters": [<record>, ...]}`; an empty list answers `{}`, and the
list is paged like the other record lists (`pageSize`, `pageToken`,
`nextPageToken`; see [Listing records](#listing-records)). One record is read
at its resource name: the name List returns is the URL path below the service
root, not a segment appended to the collection. A missing record answers 404
`text/plain` `not found`. The
record's `observation.enrollment.state` is `observed` for a cluster the
resource manager only sees through the hub; once the cluster is enrolled it
names the target it is linked to.

### POST hubs/{id}:generateEnrollmentAddon

```text
POST /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}:generateEnrollmentAddon
```

The add-on route: one bundle that covers the clusters of a connected hub, and
the hub objects that install the enrollment agent on the clusters the hub
accepts. The body is any of `profile` (default `workloads`),
`clusterRole` (with `profile: "custom"`), `grantPlatformDelivery`, `labels`,
`tokenLifetimeSeconds` (default 12 hours; at least 10 minutes, at most 24
hours), `ttlSeconds` (default: the bundle does not expire; at most 30 days),
`removeAccessWithAddon`, and `rollout` with `placementNamespace` and
`placementName`. `removeAccessWithAddon` (default false) decides
what disabling the add-on does: without it the access piece stays and the
cluster remains enrolled; with it, disabling removes the access piece and
withdraws the cluster.

`rollout` says which clusters of the hub the add-on reaches:

| `rollout`       | The hub enables the add-on for                                                                                                   | The manifest holds                                                                                                                                                                             |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `manual`        | the clusters its administrator enables it for, one `ManagedClusterAddOn` at a time                                               | an `AddOnTemplate` and a `ClusterManagementAddOn` with install strategy `Manual`                                                                                                               |
| `every-cluster` | every cluster the hub has, its own cluster included when that is registered with the hub, and every cluster that joins later     | before those two: the namespace `ocm-fleet-enrollment`, in it a `ManagedClusterSetBinding` of the cluster set `global` and the `Placement` `every-cluster`, which the install strategy follows |
| `placement`     | the clusters that the `Placement` named by `placementNamespace` and `placementName` selects, which the hub's administrator keeps | the two add-on objects, with an install strategy that follows that `Placement`                                                                                                                 |

The choice is kept on the hub's record (`enrollmentRollout`). A request that
names no `rollout` uses the one of the add-on generated before for this
connection, and `manual` when there was none, so an add-on that is generated
again to replace its secret reaches the same clusters. The generated
`Placement` tolerates the taints a hub puts on a cluster that stops answering:
a cluster that is away is still a cluster of the hub. An add-on that follows a
`Placement` is disabled by the hub for every cluster that `Placement` does not
select, also where it was enabled by hand.

The answer is `{"bundle": <bundle record>, "manifest": "<YAML>", "enableExample": "<YAML>", "rollout": {...}, "instructions": ["..."]}`.
The manifest is for the hub's administrator; `rollout` is the rollout the
add-on was made for; the example is the `ManagedClusterAddOn` that enables the
add-on for one cluster, and is empty unless the rollout is `manual`. The
bundle's secret appears in the manifest, in the
`AddOnTemplate` the manifest creates on the hub, and in the `Secret` the agent
reads on each cluster. Keep the bundle while those add-on objects are on the
hub: each cluster that enables the add-on presents with it, and the agent
re-presents to repair a lapsed credential. Revoke the bundle with
`DELETE .../enrollmentBundles/{id}` when the add-on objects are removed and no
cluster will enroll through them. Disconnecting the hub revokes its bundles:
after the hub is connected again, generate the add-on again and apply it. Until
then the hub's record counts no valid bundle (`enrollmentSecrets`) beside the
clusters that enrolled through the add-on (`enrolledThroughHubCount`).

| Status | `reason`           | When                                                                                                                                                                                                              |
| ------ | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 404    | `not-found`        | the hub is not connected                                                                                                                                                                                          |
| 400    | `invalid-argument` | a profile or a lifetime breaks the rules above, the rollout is unknown, `placement` lacks the two names or another rollout carries them, or this resource manager has no agent image or server address configured |

The block below makes the bundle, shows the instructions, and revokes it
again: this doc does not install the add-on. The walkthrough's steps 2.2 and
2.3 install it on a hub and enable it for one cluster.

```bash
fleet POST $OCM/hubs/$HUB_ID:generateEnrollmentAddon '{"profile":"workloads"}' > "$PT/addon.json"
jq -r '.instructions[]' "$PT/addon.json"
jq -c '.bundle | {route, scope, profile}' "$PT/addon.json"
jq -c .rollout "$PT/addon.json"
fleet DELETE $K8S/enrollmentBundles/$(jq -r .bundle.id "$PT/addon.json")
```

```text
Apply the manifest to the hub as its administrator: kubectl apply -f <file>. It adds one AddOnTemplate and one ClusterManagementAddOn; nothing changes on any cluster yet.
Enable the add-on for a cluster by creating the ManagedClusterAddOn "ocm-fleet-enrollment" in that cluster's namespace on the hub (see enableExample). To enroll every cluster of the hub at once, ask for the add-on with rollout "every-cluster".
Each cluster is then enrolled as clusters/my-hub.<managed-cluster-name> with target k8s-my-hub.<managed-cluster-name>, and carries the hub's labels for it.
Disabling the add-on for a cluster removes the agent and leaves the access account: the cluster stays enrolled and the resource manager renews its credential by itself.
To withdraw a cluster from its own side, delete namespace ocm-fleet-system and the cluster role bindings labeled app.kubernetes.io/part-of=ocm-fleet on it.
The manifest carries an enrollment secret. It lets only clusters that this hub has accepted, and whose CA matches the hub's record, present themselves; it is not a cluster credential. Keep it as you keep the hub's other secrets.
A manifest that is made again replaces that secret on every cluster the add-on is enabled for, and keeps this rollout unless another is asked for. One is needed after the hub was disconnected and connected again: the disconnect revokes the secret.
{"route":"addon","scope":{"verifier":"ocm-hub","verifierScope":"my-hub"},"profile":"workloads"}
{"mode":"manual"}
{}
```

The scope says what the bundle covers: the verifier `ocm-hub` of the hub
`my-hub` — every cluster that hub vouches for. A bundle of one hub is refused
for a cluster of another: the hub named in the scope must know the cluster and
have accepted it.

### POST hubs/{id}:adoptCluster

```text
POST /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}:adoptCluster
```

Makes a connected hub the one whose labels an enrolled cluster carries. The
body is `{"cluster": "<cluster id>"}`; the answer is
`{"hub": "<id>", "cluster": "<cluster id>", "managedCluster": "<the cluster's name on the hub>"}`.

The labels a hub puts on an enrolled cluster's target stay with the connection
that supplied them. They are not moved by themselves: not when that hub stops
answering, not when its connection is removed, and not when another connected
hub reports the same cluster. This request is the explicit move, for a cluster
that joined another hub or whose hub was connected again under another id. The
hub must know the cluster on more than its name: it enrolled the cluster, it
publishes the cluster's identifier (the cluster claim `id.k8s.io`), or it
records the cluster's API address and CA.

| Status | `reason`             | When                                                                                                    |
| ------ | -------------------- | ------------------------------------------------------------------------------------------------------- |
| 404    | `not-found`          | the hub is not connected, or the cluster is not enrolled                                                |
| 409    | `cluster-not-on-hub` | the hub has no cluster that is this cluster                                                             |
| 409    | `weak-link`          | the hub's cluster matches this one by its name alone, which is too little to put the hub's labels on it |
| 422    | `hub-not-synced`     | the hub's clusters have not been read yet; try again shortly                                            |
| 400    | `invalid-argument`   | the hub id or the cluster id breaks the rules of `hubs:connect` and `clusters:enroll`                   |

This document's examples have one hub, so they do not call it.

## Clusters

### GET accessManifest

```text
GET /apis/kubernetes.open-cluster-management.io/v1alpha/accessManifest?profile=<profile>&clusterRole=<name>
```

The lasting part of the access piece for an enrollment by hand: the
`ocm-fleet-system` namespace, the `ocm-fleet-access` service account, and the
role bindings of the named profile. The cluster's administrator applies it,
requests the account's token, and calls `clusters:enroll` with it. The answer
is `{"manifest": "<YAML>", "instructions": ["...", "...", "..."]}`.

| Query parameter | Meaning                                                                                                                                                |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `profile`       | `cluster-admin`, `workloads` (the default), `read-only`, or `custom`                                                                                   |
| `clusterRole`   | a ClusterRole to bind, only with `profile=custom`: 1 to 253 lowercase letters, digits, `-`, or `.`, starting and ending with an alphanumeric character |

| Status | `reason`           | When                                                                                                                   |
| ------ | ------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| 400    | `invalid-argument` | the profile is unknown, a cluster role is named with a profile other than `custom`, or `custom` has no valid role name |

The block applies the read-only piece and makes the account's credential. The
default lifetime is 12 hours; any duration of at least 10 minutes is accepted.

```bash
fleet GET "$K8S/accessManifest?profile=read-only" > "$PT/access.json"
jq -r '.instructions[]' "$PT/access.json"
jq -r .manifest "$PT/access.json" > "$PT/access.yaml"
kc_cluster apply -f "$PT/access.yaml"
SA_TOKEN=$(kc_cluster -n ocm-fleet-system create token ocm-fleet-access --duration=10m)
```

```text
Apply the manifest to the cluster as its administrator: kubectl apply -f <file>
Request a credential for the access account: kubectl -n ocm-fleet-system create token ocm-fleet-access --duration=12h0m0s
Enroll the cluster with its API address, its CA certificate, and that credential (POST /apis/kubernetes.open-cluster-management.io/v1alpha/clusters:enroll).
```

`kc_cluster apply` prints one line per object; on a cluster where the piece is
already applied it prints them as unchanged.

### POST clusters:enroll

```text
POST /apis/kubernetes.open-cluster-management.io/v1alpha/clusters:enroll
```

| Field                   | Meaning                                                                                                                                                                                                                          |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `clusterId`             | the id the cluster gets in this resource manager: a DNS subdomain of at most 200 characters — lowercase letters, digits, `-` and `.`, each part a DNS label of at most 63 characters, starting and ending with a letter or digit |
| `apiServer`             | the cluster's API address, as an `https` URL; from inside the resource manager the name must resolve                                                                                                                             |
| `caCert`                | the PEM CA certificate that signs that address's certificate                                                                                                                                                                     |
| `token`                 | a token of a service account on the cluster                                                                                                                                                                                      |
| `serviceAccount`        | optional `{"namespace": ..., "name": ...}`; when set, the token must belong to that account                                                                                                                                      |
| `labels`                | optional labels for the cluster's target, in Kubernetes label syntax: a key's name part and a value are at most 63 characters                                                                                                    |
| `grantPlatformDelivery` | whether the stored credential may apply deployments with the caller's intent verified only by the platform (see Security)                                                                                                        |
| `profile`               | what the token may do; the profile the access piece granted: `cluster-admin`, `workloads`, `read-only`, or `custom`; the default is `workloads`                                                                                  |

The resource manager verifies the credential against the cluster at once: it
probes the cluster with the presented token and CA and reads the token's
identity. The answer is `{"cluster": <cluster record>}` — the record as it is
stored, not the inventory envelope: `{"id", "name", "labels", "observation"}`.
Enrolling a cluster that is already enrolled with this id replaces its
credential and labels; enrolling the same cluster under another id is
refused. Whether it is the same cluster is decided by the cluster's identifier
(the UID of its `kube-system` namespace, which the probe reads) and, where the
credential may not read it, by the API address and CA: a cluster that is
reached at two addresses is still one cluster and gets one target.

| Status | `reason`                | When                                                                                                                                                                             |
| ------ | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 400    | `invalid-argument`      | the id, address, CA, labels, or profile break the rules above                                                                                                                    |
| 409    | `cluster-id-in-use`     | the id belongs to a target the resource manager did not take in by enrollment; `details.target` names it                                                                         |
| 409    | `cluster-already-known` | the cluster already belongs to another `kubernetes` target — one enrolled under another id, or one a provider made, at this address or at another one; `details.target` names it |
| 422    | `credential-rejected`   | the cluster does not recognize the presented credential                                                                                                                          |
| 422    | `not-a-service-account` | the credential belongs to another kind of identity, so it cannot be renewed                                                                                                      |
| 422    | `unexpected-identity`   | the token belongs to another account than `serviceAccount` names                                                                                                                 |
| 422    | `cluster-unreachable`   | the address or the CA is wrong, or the cluster does not answer                                                                                                                   |

The block enrolls the cluster with the credential from the block above. The
target appears as the enrollment is recorded; the indexing starts with it.

```bash
CLUSTER_CA=$(kc_cluster config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d)
fleet POST $K8S/clusters:enroll "$(jq -cn \
  --arg id "$CLUSTER_ID" --arg api "$CLUSTER_APISERVER" --arg ca "$CLUSTER_CA" --arg token "$SA_TOKEN" \
  '{clusterId:$id, apiServer:$api, caCert:$ca, token:$token, profile:"read-only", grantPlatformDelivery:true}')" \
  | jq -c '.cluster | {name, state: .observation.state, target: .observation.target, profile: .observation.profile}'

n=0; until fleet GET $CORE/targets | jq -e --arg id "k8s-$CLUSTER_ID" '.targets[] | select(.id==$id and .state=="ready")' > /dev/null 2>&1; do
  (( ++n > 60 )) && { echo "gave up after a minute: the target is not ready"; break; }
  sleep 1
done; (( n <= 60 )) && echo "target k8s-$CLUSTER_ID is ready"
```

```text
{"name":"clusters/my-cluster","state":"enrolled","target":"k8s-my-cluster","profile":"read-only"}
target k8s-my-cluster is ready
```

A second enrollment of the same id with a token of another account shows the
identity check; an unknown profile shows argument validation. Neither changes
the record.

```bash
fleet POST $K8S/clusters:enroll "$(jq -cn --arg id "$CLUSTER_ID" --arg api "$CLUSTER_APISERVER" --arg ca "$CLUSTER_CA" \
  '{clusterId:$id, apiServer:$api, caCert:$ca, token:"not-a-token", profile:"workloads"}')"
fleet POST $K8S/clusters:enroll "$(jq -cn --arg id "$CLUSTER_ID" --arg api "$CLUSTER_APISERVER" --arg ca "$CLUSTER_CA" \
  '{clusterId:$id, apiServer:$api, caCert:$ca, token:"not-a-token", profile:"root"}')"
```

```text
{"error":"invalid argument: unknown access profile \"root\"","reason":"invalid-argument"}
```

The order of the two calls is the order of the checks: the profile is
validated before anything is sent to the cluster, so the first call — whose
profile is valid — reaches the cluster and is refused there with a
`credential-rejected` answer naming the address, while the second is refused
before any probe. The first answer is also the one a bad credential gets:

```text
{"error":"the cluster at https://my-cluster-control-plane:6443 does not recognize the presented credential","reason":"credential-rejected"}
```

### POST clusters/{id}:renewCredential

```text
POST /apis/kubernetes.open-cluster-management.io/v1alpha/clusters/{id}:renewCredential
```

Renewal on demand: the resource manager asks the cluster for a new token of the
stored account and keeps it. It does the same by itself when a credential has
used a third of its lifetime. The body is empty (or `{}`); the answer is
`{"cluster": <cluster record>}`.

| Status | `reason`              | When                                                                                    |
| ------ | --------------------- | --------------------------------------------------------------------------------------- |
| 404    | `not-found`           | the cluster is not enrolled under this id                                               |
| 422    | `credential-rejected` | the cluster no longer recognizes the stored credential; its side must present a new one |
| 422    | `renewal-denied`      | the cluster does not let the account request its own token                              |
| 504    | `timeout`             | the cluster did not answer within the resource manager's request budget; retry          |

```bash
fleet POST $K8S/clusters/$CLUSTER_ID:renewCredential '{}' \
  | jq -c '.cluster.observation | {state, credentialExpiresAt: (.credentialExpiresAt != null)}'
```

```text
{"state":"enrolled","credentialExpiresAt":true}
```

### GET clusters and clusters/{id}

```text
GET /apis/kubernetes.open-cluster-management.io/v1alpha/clusters
GET /apis/kubernetes.open-cluster-management.io/v1alpha/clusters/{id}
```

The list answers `{"clusters": [<record>, ...]}`; an empty list answers `{}`.
One cluster answers its record; a missing one answers 404 `text/plain`
`not found`. `id` in the path is the cluster id, and the record's `name` is
`clusters/<id>`. The list is paged: `pageSize` and `pageToken` in the
request, `nextPageToken` in the answer while more clusters remain (see
[Listing records](#listing-records)).

```bash
fleet GET $K8S/clusters | jq -c '[.clusters[] | {name, state: .observation.state}]'
fleet GET $K8S/clusters/$CLUSTER_ID | jq -c '.observation | {state, apiServer, profile, rights}'
fleet GET $K8S/clusters/no-such-cluster
```

```text
[{"name":"clusters/my-cluster","state":"enrolled"}]
{"state":"enrolled","apiServer":"https://my-cluster-control-plane:6443","profile":"read-only","rights":{"canRenew":true,"clusterAdmin":false,"namespaces":false,"readCluster":true,"workloads":false}}
not found
```

### POST clusters/{id}:withdraw

```text
POST /apis/kubernetes.open-cluster-management.io/v1alpha/clusters/{id}:withdraw
```

The management side ends the enrollment. The target, the stored credential,
and the indexed objects of the cluster go; the cluster itself and its hub
registration are not touched. The body may name a `reason` (kept in the
record; at most 500 bytes, a longer one answers 400 `invalid-argument`) and set
`leaveDeployments`: without it, deployments that still resolve
to the target refuse the withdrawal; with it, what they delivered stays on the
cluster. Nothing is dispatched to the cluster once the withdrawal starts: a
deployment placed by label drops the cluster from its placement without a
removal, and one that names the target keeps naming it and can be deleted
afterwards. The answer is `{"cluster": <cluster record>}` with state `withdrawn`.

| Status | `reason`                        | When                                                                                               |
| ------ | ------------------------------- | -------------------------------------------------------------------------------------------------- |
| 404    | `not-found`                     | the cluster is not enrolled under this id                                                          |
| 409    | `not-enrolled`                  | a target of that id exists, but it was not created by enrollment (a provider made it)              |
| 409    | `deployments-resolve-to-target` | deployments still use the target; `details.deployments` names them and `details.target` the target |

The block withdraws the cluster enrolled above. The second call shows the
answer once the target is gone.

```bash
fleet POST $K8S/clusters/$CLUSTER_ID:withdraw '{"reason":"done with the examples"}' \
  | jq -c '{state: .cluster.observation.state, withdrawnBy: .cluster.observation.withdrawnBy}'
fleet POST $K8S/clusters/$CLUSTER_ID:withdraw '{}'
```

```text
{"state":"withdrawn","withdrawnBy":"ops@ocm-fleet.local"}
{"error":"not found: cluster \"my-cluster\" is not enrolled","reason":"not-found"}
```

## Enrollment bundles

### POST enrollmentBundles

```text
POST /apis/kubernetes.open-cluster-management.io/v1alpha/enrollmentBundles
POST /apis/kubernetes.open-cluster-management.io/v1alpha/enrollmentBundles?bundleId=<id>
```

A bundle for one cluster: its administrator applies the returned manifest, a
job runs once on the cluster, and the cluster presents itself. No hub object
is involved and no hub is contacted: the bundle fixes the cluster's id and
address, and the CA the cluster presents becomes the CA recorded for it. (A
bundle made by `hubs/{id}:generateEnrollmentAddon` is different: it is scoped to that
hub, which vouches for each cluster that presents.)

| Field                             | Meaning                                                                                                          |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `clusterId`, `apiServer`          | the cluster the bundle covers; the same rules as at `clusters:enroll`                                            |
| `profile`, `clusterRole`          | as at `accessManifest`: the default is `workloads`                                                               |
| `grantPlatformDelivery`, `labels` | as at `clusters:enroll`                                                                                          |
| `tokenLifetimeSeconds`            | the lifetime of the credential the cluster asks for: 0 means 12 hours; at least 10 minutes, at most 24 hours     |
| `ttlSeconds`                      | the bundle's own lifetime: 0 means one day for a bundle of one cluster; negative is refused, and at most 30 days |

The request may name the bundle's id with the `bundleId` query parameter:
`eb-` followed by 12 lowercase hexadecimal digits. A create without it gets a
generated id. Naming an id that already exists answers 409 `bundle-id-in-use`
with `details.name` holding the existing bundle's name; it mints no second
secret and leaves the existing bundle's secret working. That makes a retried
create safe: a client that is unsure whether its create landed sends the same
request again and learns from the conflict that it did.

The answer is
`{"name": "enrollmentBundles/<id>", "bundle": <bundle record>, "manifest": "<YAML>", "instructions": ["...", "...", "...", "..."], "expiresAt": <time>}`;
`expiresAt` is absent when the bundle does not expire. The bundle's secret
appears only inside the manifest — in the `Secret` the job reads — and is never
part of the record. The instructions are the four steps for the cluster's
administrator: apply the manifest; the job enrolls the cluster; the agent's
objects can then be removed; and how to end the enrollment from the cluster's
side.

| Status | `reason`            | When                                                                                                     |
| ------ | ------------------- | -------------------------------------------------------------------------------------------------------- |
| 400    | `invalid-argument`  | a field breaks the rules above, or this resource manager has no agent image or server address configured |
| 409    | `cluster-id-in-use` | the id belongs to a target the resource manager did not take in by enrollment                            |
| 409    | `bundle-id-in-use`  | the `bundleId` parameter names an existing bundle; `details.name` is the existing bundle's name          |

```bash
fleet POST $K8S/enrollmentBundles "$(jq -cn --arg id "$CLUSTER_ID" --arg api "$CLUSTER_APISERVER" \
  '{clusterId:$id, apiServer:$api, profile:"read-only", grantPlatformDelivery:true, tokenLifetimeSeconds:600}')" \
  > "$PT/bundle.json"
jq -c '{name: (.name | sub("^enrollmentBundles/eb-[0-9a-f]+$"; "enrollmentBundles/<bundle>")), profile: .bundle.profile, tokenLifetimeSeconds: .bundle.tokenLifetimeSeconds, expiresAt: (.expiresAt != null), instructions: (.instructions | length)}' "$PT/bundle.json"
jq -r .manifest "$PT/bundle.json" > "$PT/bundle.yaml"
kc_cluster apply -f "$PT/bundle.yaml"
n=0; until fleet GET $CORE/targets | jq -e --arg id "k8s-$CLUSTER_ID" '.targets[] | select(.id==$id and .state=="ready")' > /dev/null 2>&1; do
  (( ++n > 90 )) && { echo "gave up after three minutes: the job did not enroll the cluster"; break; }
  sleep 2
done; (( n <= 90 )) && echo "$CLUSTER_ID is enrolled"
```

```text
{"name":"enrollmentBundles/<bundle>","profile":"read-only","tokenLifetimeSeconds":600,"expiresAt":true,"instructions":4}
my-cluster is enrolled
```

The job's `Secret` on the cluster is where the enrollment secret lives. The
block below reads it to make the same call the job made, with the same body
the agent sends.

### POST enrollments:present

```text
POST /apis/kubernetes.open-cluster-management.io/v1alpha/enrollments:present
```

A cluster's side presents itself. This is the one request without a user
token: the bearer value is the bundle secret, `<bundle id>.<secret>`. The body
names the cluster and carries what the cluster shows:

| Field            | Meaning                                                                                                                                                                                |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `clusterName`    | the cluster's name as the hub knows it; a bundle made through a hub needs it and the hub checks it, while a bundle made for one cluster takes that cluster's id from the bundle itself |
| `caCert`         | the PEM CA certificate of the cluster's API                                                                                                                                            |
| `token`          | a token of the access account on the cluster; it is required                                                                                                                           |
| `serviceAccount` | optional `{"namespace": ..., "name": ...}`; when set, the token must belong to it                                                                                                      |

The resource manager checks the secret and probes the cluster with the
presented CA and token. A bundle made through a hub also asks the hub whether
the cluster is known, what address it records for it, and what CA it records
for that address; where the hub records no CA for the address, the presented CA
is the probe's only trust root. A bundle made for one cluster uses the address
in the bundle. The answer is
`{"cluster": "clusters/<id>", "target": "k8s-<id>", "state": "enrolled", "renewAfterSeconds": <N>}`;
`N` is a third of the presented token's lifetime, when the cluster should ask
for a renewal.

| Status | `reason`                                                                                           | When                                                                                                                                                                                                  |
| ------ | -------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 400    | `invalid-argument`                                                                                 | the body is not valid JSON, `token` is empty, `caCert` is not a PEM certificate, or a verifier bundle lacks `clusterName` or yields an invalid cluster id or API address                              |
| 401    | `bundle-missing`, `bundle-malformed`, `bundle-unknown`, `bundle-secret-mismatch`, `bundle-expired` | no secret, not a secret, no such bundle, wrong secret, or the bundle's time is over; all answer "the enrollment bundle secret is not valid" but for `bundle-missing`                                  |
| 409    | `cluster-id-in-use`                                                                                | the cluster's id belongs to a target the resource manager did not take in by enrollment; `details.target` names it                                                                                    |
| 409    | `cluster-already-known`                                                                            | the presenting cluster belongs to another `kubernetes` target — one enrolled under another id, or one a provider made, at the presented address or at another one; `details.target` names that target |
| 429    | `rate-limited`                                                                                     | too many failures from this address, or under this cluster name; `Retry-After` says when to try again                                                                                                 |
| 403    | `verifier-unavailable`                                                                             | the bundle's verifier is not registered any more                                                                                                                                                      |
| 403    | `cluster-withdrawn`                                                                                | the cluster was withdrawn after the bundle was made; make a new bundle or forget the record first                                                                                                     |
| 403    | `hub-not-connected`                                                                                | the hub that must vouch is not connected                                                                                                                                                              |
| 403    | `cluster-not-on-hub`                                                                               | the hub knows no cluster of that name                                                                                                                                                                 |
| 403    | `cluster-not-accepted`                                                                             | the hub has not accepted the cluster                                                                                                                                                                  |
| 422    | `hub-not-synced`                                                                                   | the hub's clusters have not been read yet; try again shortly                                                                                                                                          |
| 422    | `no-recorded-address`                                                                              | the hub records no API address for the cluster, so the presented CA cannot be checked; give the cluster's registration agent the external address, or enroll with a bundle of its own                 |
| 422    | `ca-mismatch`                                                                                      | the CA presented matches no CA set the hub records for the cluster's addresses; an address recorded without a CA accepts the presented CA                                                             |
| 422    | `credential-rejected`                                                                              | the cluster does not recognize the presented credential                                                                                                                                               |
| 422    | `cluster-unreachable`                                                                              | the cluster does not answer at the address                                                                                                                                                            |
| 422    | `unexpected-identity`                                                                              | the token belongs to another account than the access account or than `serviceAccount` names                                                                                                           |

The block makes the call the agent makes; the cluster is already enrolled, so
the call refreshes it. It then makes two calls that must be refused: the same
presentation with a wrong secret, and a presentation with no credential.

```bash
SECRET=$(kc_cluster -n ocm-fleet-system get secret ocm-fleet-enrollment -o jsonpath='{.data.token}' | base64 -d)
present() { "${CURL[@]}" -X POST -H "Authorization: Bearer $1" -H 'Content-Type: application/json' --data-binary "$2" "$FLEET$K8S/enrollments:present"; echo; }
BODY=$(jq -cn --arg n "$CLUSTER_ID" --arg ca "$CLUSTER_CA" --arg t "$SA_TOKEN" \
  '{clusterName:$n, caCert:$ca, token:$t, serviceAccount:{namespace:"ocm-fleet-system", name:"ocm-fleet-access"}}')
echo "a wrong secret:"
present "${SECRET%%.*}.wrong" "$BODY"
echo "the right secret:"
present "$SECRET" "$BODY"
echo "no credential:"
present "$SECRET" "$(jq -c '.token=""' <<<"$BODY")"
```

```text
a wrong secret:
{"error":"the enrollment bundle secret is not valid","reason":"bundle-secret-mismatch"}
the right secret:
{"cluster":"clusters/my-cluster","renewAfterSeconds":200,"state":"enrolled","target":"k8s-my-cluster"}
no credential:
{"error":"invalid argument: a credential issued by the cluster is required","reason":"invalid-argument"}
```

### GET enrollmentBundles and enrollmentBundles/{id}

```text
GET /apis/kubernetes.open-cluster-management.io/v1alpha/enrollmentBundles
GET /apis/kubernetes.open-cluster-management.io/v1alpha/enrollmentBundles/{id}
```

The list answers `{"enrollmentBundles": [<record>, ...]}`; an empty list
answers `{}`. One bundle answers its record; a missing one answers 404
`text/plain` `not found`; `id` is the bundle id and the record's `name` is
`enrollmentBundles/<id>`. The list is paged: `pageSize` and `pageToken` in
the request, `nextPageToken` in the answer while more bundles remain (see
[Listing records](#listing-records)). A bundle's `uses` counts the presentations
it has served, and `lastUsedAt` says when it last served one. A bundle serves every
presentation that passes its scope until it expires or is revoked. The secret
appears in the manifest the request that made the bundle answered, and in the
`Secret` the agent reads on the cluster; for a hub-scoped bundle it also
appears in the `AddOnTemplate` on the hub. No record carries it.

```bash
fleet GET $K8S/enrollmentBundles | jq -c '[.enrollmentBundles[] | {name: (.name | sub("^enrollmentBundles/eb-[0-9a-f]+$"; "enrollmentBundles/<bundle>")), profile: .observation.profile, uses: .observation.uses}]'
fleet GET $K8S/enrollmentBundles/$(jq -r .bundle.id "$PT/bundle.json") \
  | jq -c '.observation | {profile, uses, expires: (.expiresAt != null), lastUsed: (.lastUsedAt != null)}'
fleet GET $K8S/enrollmentBundles/eb-000000000000
```

```text
[{"name":"enrollmentBundles/<bundle>","profile":"read-only","uses":2}]
{"profile":"read-only","uses":2,"expires":true,"lastUsed":true}
not found
```

### DELETE enrollmentBundles/{id}

```text
DELETE /apis/kubernetes.open-cluster-management.io/v1alpha/enrollmentBundles/{id}
```

Revoking a bundle: its secret stops working at once; clusters it enrolled stay
enrolled. The answer is `{}`; a missing bundle answers 404 with `not-found`.
The path takes the bundle id (the part of `name` after `enrollmentBundles/`),
not the secret.

```bash
fleet DELETE $K8S/enrollmentBundles/$(jq -r .bundle.id "$PT/bundle.json")
echo "presenting with the revoked secret:"
present "$SECRET" "$BODY"
```

```text
{}
presenting with the revoked secret:
{"error":"the enrollment bundle secret is not valid","reason":"bundle-unknown"}
```

## Targets and queries

### GET targets

```text
GET /apis/open-cluster-management.io/v1alpha/targets
```

Every target the platform can deliver to: the ones providers created and the
ones enrollment took in. The answer is `{"targets": [<target>, ...]}`. Every
target has `id`, `name`, `type`, `state`, `labels`, `acceptedManifestTypes`,
`origin` (`provisioned` or `enrolled`), and `properties` (an object, empty when
the target has none). A target that stands for a cluster also has `cluster`,
the cluster resource's name, whether enrollment or a provider (kind) put it
there; `accessForm` and `deliveryCredential` are present only on a target
enrollment added.

```bash
fleet GET $CORE/targets | jq -c '[.targets[] | select(.origin=="enrolled") | {id, state, origin, accessForm, deliveryCredential}]'
```

```text
[{"id":"k8s-my-cluster","state":"ready","origin":"enrolled","accessForm":"direct","deliveryCredential":"platform"}]
```

### GET -:queryResources

```text
GET /apis/open-cluster-management.io/v1alpha/-:queryResources?filter=<CEL>&pageSize=<n>
```

The inventory query: the records of every activated type, filtered with the
language of [query-resources-cel-filters.md](query-resources-cel-filters.md).
The answer is `{"resources": [<result>, ...], "nextPageToken": "<token>"}`;
a result carries `name`, `resourceType`, and the record as `resource`. Pass
`nextPageToken` back as `pageToken` for the next page. The scope `-` means the
whole platform.

The hub's observation of the cluster is a `ManagedCluster` record — what the
hub says, with `enrollment` naming the cluster record and target it is linked
to. The cluster's own record is a `Cluster` record. Two connected hubs can each
have a cluster of one name, so the first query names the hub as well.

```bash
query 'resourceType == "ocm.open-cluster-management.io/ManagedCluster"' 500 \
  | jq -c --arg h "$HUB_ID" --arg n "$CLUSTER_ID" '[.resources[].resource.observation | select(.hub==$h and .name==$n) | {name, enrollment: .enrollment.state, basis: .enrollment.basis}]'
query 'resourceType == "kubernetes.open-cluster-management.io/Cluster"' 500 \
  | jq -c --arg n "$CLUSTER_ID" '[.resources[] | select(.resource.observation.name==$n) | {name: .resource.observation.name, state: .resource.observation.state, profile: .resource.observation.profile}]'
```

```text
[{"name":"my-cluster","enrollment":"enrolled","basis":"address-and-ca"}]
[{"name":"my-cluster","state":"enrolled","profile":"read-only"}]
```

## DELETE clusters/{id}

```text
DELETE /apis/kubernetes.open-cluster-management.io/v1alpha/clusters/{id}
```

Forgetting a cluster's record. Only a cluster that is not enrolled can be
forgotten: while the target exists the answer is 409 `still-enrolled`. After
the withdrawal the answer is `{}`, and the record is gone from the reads.

| Status | `reason`         | When                                       |
| ------ | ---------------- | ------------------------------------------ |
| 404    | `not-found`      | there is no record of that id              |
| 409    | `still-enrolled` | the cluster is enrolled; withdraw it first |

The bundle above enrolled the cluster again. The block first shows the
refusal, then withdraws (the request documented under Clusters) and forgets.

```bash
fleet DELETE $K8S/clusters/$CLUSTER_ID
fleet POST $K8S/clusters/$CLUSTER_ID:withdraw '{"reason":"done with the examples"}' > /dev/null
fleet DELETE $K8S/clusters/$CLUSTER_ID
echo "clusters: $(fleet GET $K8S/clusters | jq -c .)"
fleet GET $K8S/clusters/$CLUSTER_ID
```

```text
{"error":"cluster \"my-cluster\" is enrolled; withdraw it before forgetting it","reason":"still-enrolled"}
{}
clusters: {}
not found
```

## POST hubs/{id}:disconnect

```text
POST /apis/ocm.open-cluster-management.io/v1alpha/hubs/{id}:disconnect
```

Ends the connection: the hub record, the cluster records observed through it,
the stored credential, and the hub-scoped enrollment bundles go. Clusters
enrolled through the hub stay enrolled and stay deliverable — their delivery
never goes through the hub — and the labels the hub put on their targets stay
as they were and stop being updated; each such cluster's record says so, with
the condition `HubFactsCurrent` at `False` and the reason `HubDisconnected`.
Other connected hubs are not touched. The body is empty (or `{}`); the
answer is `{}`. The read-only account on the hub stays until the hub's
administrator removes it, and so does an enrollment add-on on the hub, whose
secret no longer works: after connecting the hub again, generate the add-on
again and apply it. Connecting the hub again under the same id makes those
labels current again; a connection under another id does not, until
`hubs/{id}:adoptCluster` is called for the cluster.

A missing connection answers 404 with `not-found`, also when the call is
repeated.

```bash
fleet POST $OCM/hubs/$HUB_ID:disconnect '{}'
echo "hubs: $(fleet GET $OCM/hubs | jq -c .)"
fleet GET $OCM/hubs/$HUB_ID
fleet POST $OCM/hubs/$HUB_ID:disconnect '{}'
```

```text
{}
hubs: {}
not found
{"error":"not found: hub \"my-hub\" is not connected","reason":"not-found"}
```

## Put back what the examples changed

The cluster's administrator removes the access piece: the namespace holds the
account and its job, and the cluster role bindings and the generated cluster
role carry the platform's label. Nothing else on the cluster was touched.

```bash
kc_cluster delete namespace ocm-fleet-system --ignore-not-found
kc_cluster delete clusterrolebinding,clusterrole -l app.kubernetes.io/part-of=ocm-fleet --ignore-not-found
echo "access piece on the cluster: $(kc_cluster get namespace ocm-fleet-system --ignore-not-found -o name 2> /dev/null | wc -l) namespace(s), $(kc_cluster get clusterrolebinding -l app.kubernetes.io/part-of=ocm-fleet --no-headers 2> /dev/null | wc -l) binding(s)"
```

```text
namespace "ocm-fleet-system" deleted
clusterrolebinding.rbac.authorization.k8s.io "ocm-fleet-access-cluster" deleted
clusterrolebinding.rbac.authorization.k8s.io "ocm-fleet-access-view" deleted
clusterrole.rbac.authorization.k8s.io "ocm-fleet-access-cluster" deleted
access piece on the cluster: 0 namespace(s), 0 binding(s)
```

The read-only account on the hub is separate: the hub's administrator made it
from the manifest, and removing it ends every connection that uses it. On a hub
that other resource managers use, keep it. The walkthrough's step 10 shows the
removal of everything it created, the account included.

## Security for operators

**What the hub account can do.** The account from `hubAccessManifest` gets,
lists, and watches `managedclusters` and `managedclustersets` in
`cluster.open-cluster-management.io`, gets the one namespace `kube-system`
(for its UID, the hub's identifier), and may request tokens for itself and
nothing else. It cannot write to the hub: `writeAccessFound` in the hub record
reports the resource manager's own probe of that, and a credential that could
change the hub (for example `cluster-admin`) is reported the same way. The
resource manager writes nothing to a hub on its own.

**What a deployment cannot reach.** A hub that is registered with itself can
be enrolled like any of its clusters, and the access piece's rights there are
the profile's, as on any cluster. The resource manager therefore refuses, by
itself and before anything is sent, a delivery that would write where Open
Cluster Management or a hub lives:

- on a cluster a hub manages: the namespaces `open-cluster-management` and
  `open-cluster-management-*`;
- on the cluster a connected hub runs on, also: the namespace the hub keeps for
  each of its clusters, the namespace `ocm-fleet-hub-connection`, and every
  object of an API group that is or ends in `open-cluster-management.io`;
- on every enrolled cluster whose access account is in `ocm-fleet-system`:
  that namespace.

Such a delivery fails with a message that names the manifest and says why it
`is out of reach of deliveries on this cluster`; nothing of that delivery is
applied. A removal leaves such an object in place and names it. This is a
rule of the resource manager, not of the cluster: the stored credential itself
still has the profile's rights, so choose the profile for a hub's own cluster
with that in mind.

**What a profile grants.** The access piece on the cluster is the profile,
applied by the cluster's administrator:

| Profile         | What the account gets                                                                                                                                                      |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster-admin` | the built-in `cluster-admin` role                                                                                                                                          |
| `workloads`     | the built-in `edit` role, plus read access to the cluster's namespaces, nodes, storage, and the objects below, and `create`, `update`, `patch`, and `delete` on namespaces |
| `read-only`     | the built-in `view` role, plus the same read access                                                                                                                        |
| `custom`        | the ClusterRole the request names                                                                                                                                          |

Read access, for `workloads` and `read-only`, covers namespaces, nodes,
persistent volumes, storage classes and CSI objects, custom resource
definitions, API services, priority classes, runtime classes, ingress classes,
RBAC objects, admission and flow-control configuration, and certificate
signing requests. The read rules the platform adds deliberately exclude
Secrets. The built-in roles carry what Kubernetes gives them: `view` also
excludes Secrets, while `edit` — bound by `workloads` — can read and write
Secrets in every namespace, and can request tokens of the service accounts
there, so it reaches what those accounts may do. `workloads` can also delete a
namespace, and with it everything in it: a deployment may bring its namespace,
and removing the deployment removes what it applied. Choose `read-only` where
the account must not reach Secrets, and `custom` where it must not act as other
accounts or must not delete namespaces. Every profile can request its own
token: that is how a credential is renewed.

**Where credentials are kept.** The token a cluster presented is stored in the
resource manager's secret store and never appears in a record, an answer, or
the log (at `LOG_LEVEL=debug` included). The hub's token is kept the same way.
The enrollment secret lives in the `manifest` of the answer that made it, in
the `Secret` the agent reads on the cluster, and, for a hub-scoped bundle, in
the `AddOnTemplate` on the hub; it is not in any record.

**What the enrollment secret allows.** It lets a cluster present itself, and
nothing else: only a cluster the bundle's verifier vouches for (a hub's
accepted cluster, at an address the hub records for it, with the CA it records
for that address where it records one) and only with a credential of that
cluster's access account. It is not a cluster credential
and cannot be used to call any other request. For a hub-scoped bundle a copy
of it is readable by anyone who can read the add-on template on the hub — the
design document's limits say so; treat it as you treat the hub's other
secrets. Revoke the bundle (`DELETE .../enrollmentBundles/{id}`) only once the
add-on objects are removed and no cluster will enroll through them: while they
are on the hub, the agent re-presents with the bundle to repair a lapsed
credential.

**The stored credential and intent.** By default a deployment reaches an
enrolled cluster only when the intent is signed and verified: the stored
credential is used as the platform's, not the caller's. `grantPlatformDelivery`
at enrollment lifts that: from then on a signed-in caller's deployments are
applied with the stored credential, with the caller's identity recorded as the
author. Grant it only where the callers are as trusted as the cluster's own
administrator.

## Troubleshooting

Every failure names its `reason`; this table says what each means and what to
do.

| `reason`                                                     | What to do                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bundle-missing`, `bundle-malformed`                         | the cluster's side sent no bundle secret, or a value that is not one; check the `Secret` the manifest made on the cluster and the manifest version                                                                                                                                                                                                                                                                                                 |
| `bundle-unknown`, `bundle-secret-mismatch`, `bundle-expired` | the bundle was revoked, the secret is wrong, or the bundle's time is over; make a new bundle and apply its manifest                                                                                                                                                                                                                                                                                                                                |
| `rate-limited`                                               | wait as `Retry-After` says; failures from one address, or under one cluster name, do not shut out the others                                                                                                                                                                                                                                                                                                                                       |
| `verifier-unavailable`                                       | the bundle covers a verifier this resource manager no longer has; make a new bundle                                                                                                                                                                                                                                                                                                                                                                |
| `cluster-withdrawn`                                          | the cluster was withdrawn after the bundle was made; make a new bundle, or forget the record first                                                                                                                                                                                                                                                                                                                                                 |
| `hub-not-connected`                                          | connect the hub again; the bundle covers its clusters while the connection exists                                                                                                                                                                                                                                                                                                                                                                  |
| `cluster-not-on-hub`                                         | the hub does not know a cluster of that name; check the name and the cluster's registration with the hub                                                                                                                                                                                                                                                                                                                                           |
| `cluster-not-accepted`                                       | accept the cluster's join request on the hub                                                                                                                                                                                                                                                                                                                                                                                                       |
| `hub-not-synced`                                             | wait; the hub's cluster list is read at connection and followed while connected                                                                                                                                                                                                                                                                                                                                                                    |
| `no-recorded-address`                                        | the hub records no address for the cluster; give the cluster's registration agent its external API address, or enroll the cluster with a bundle of its own                                                                                                                                                                                                                                                                                         |
| `ca-mismatch`                                                | the cluster presents a CA the hub records for none of its addresses; check which CA the cluster's API serves and what the hub recorded. An address the hub records without a CA accepts the presented CA                                                                                                                                                                                                                                           |
| `credential-rejected`                                        | the cluster, or the hub being connected, does not recognize the credential: it was rotated there, or it is a token of another account                                                                                                                                                                                                                                                                                                              |
| `renewal-denied`                                             | the cluster's RBAC no longer lets the account request its own token; re-apply the access piece                                                                                                                                                                                                                                                                                                                                                     |
| `cluster-unreachable`, `hub-unreachable`                     | check the address and the CA: what the resource manager dials must resolve and serve a certificate under the given CA; an address that resolves only to loopback, link-local (where cloud instance metadata lives) or another range a cluster is not at is refused before it is dialed, and an operator whose cluster really is there permits it with `--cluster-egress-allow` (or `OCM_RM_CLUSTER_EGRESS_ALLOW`), a comma-separated list of CIDRs |
| `not-a-hub`                                                  | the address serves no ManagedCluster API; check that it is the hub's API                                                                                                                                                                                                                                                                                                                                                                           |
| `access-denied`                                              | the credential may not list and watch managed clusters; apply the hub manifest again on that hub                                                                                                                                                                                                                                                                                                                                                   |
| `not-a-service-account`                                      | present a service account's token, not a user's                                                                                                                                                                                                                                                                                                                                                                                                    |
| `unexpected-identity`                                        | the token belongs to another account than the one expected; check `serviceAccount` and the access piece                                                                                                                                                                                                                                                                                                                                            |
| `bundle-id-in-use`                                           | read the bundle that `details.name` names, or create with another bundle id                                                                                                                                                                                                                                                                                                                                                                        |
| `cluster-id-in-use`                                          | choose another cluster id, or remove the provider-made target that holds it                                                                                                                                                                                                                                                                                                                                                                        |
| `cluster-already-known`                                      | the cluster already belongs to another target, at this address or at another one; reuse the id it was enrolled under, or delete the provider-made target where it is managed                                                                                                                                                                                                                                                                       |
| `hub-id-in-use`, `hub-already-connected`                     | disconnect the other connection, or connect with the same id to refresh it; a hub that moved to another address is accepted under its own id once its account can read the hub's identifier (apply the current access manifest)                                                                                                                                                                                                                    |
| `weak-link`                                                  | the hub knows the cluster by name only; let the hub record the cluster's API address, or enroll the cluster through that hub                                                                                                                                                                                                                                                                                                                       |
| `not-enrolled`                                               | the target was made by a provider; it cannot be withdrawn — delete it where it is managed                                                                                                                                                                                                                                                                                                                                                          |
| `deployments-resolve-to-target`                              | delete the deployments `details.deployments` names, or withdraw with `leaveDeployments`                                                                                                                                                                                                                                                                                                                                                            |
| `still-enrolled`                                             | withdraw the cluster before forgetting it                                                                                                                                                                                                                                                                                                                                                                                                          |
| `not-found`                                                  | the id or the connection does not exist: check the name, and that the record was not forgotten                                                                                                                                                                                                                                                                                                                                                     |
| `invalid-argument`                                           | read the message: it names the field and the rule                                                                                                                                                                                                                                                                                                                                                                                                  |
| `timeout`                                                    | the cluster or hub did not answer within the resource manager's request budget; check the address and the CA, then retry                                                                                                                                                                                                                                                                                                                           |
| `internal`                                                   | the resource manager's log has the cause                                                                                                                                                                                                                                                                                                                                                                                                           |

**A cluster that needs enrollment.** A cluster that a connected hub reports is
_observed_: listed, searchable, and shown, but not a delivery target. The
refusal names it plainly — a deployment to it fails with a message saying the
target is unknown or not enrolled — and the hub's `ManagedCluster` record says
`enrollment.state: observed`; there is no `clusters/{id}` record until it is
enrolled. (`needs-enrollment` on a cluster record means something else: an
enrolled cluster whose credential lapsed.) To enroll it, use the hub's add-on
route (`hubs/{id}:generateEnrollmentAddon`, then enable the add-on for the cluster on
the hub) or the manifest route (`enrollmentBundles`, then apply the manifest
on the cluster). Both end with the cluster as an ordinary `kubernetes` target.

**A hub that is not reachable.** The hub record's `Connected` condition turns
`False` with a reason, and `Ready` with it; enrolled clusters keep delivering
and their hub labels stay usable, and each such cluster's record says, with
`HubFactsCurrent` at `False`, that those labels are the last ones seen. The
hub's observed clusters are marked stale, also when the resource manager was
started while the hub was away. Other connected hubs are not affected. Check,
in this order: the address the resource manager dials
(`apiServer` in the record), the CA it trusts (`caFingerprint`), the account on
the hub, and the hub's own health. A connection is repaired by connecting
again with the same `hubId`, which replaces the stored credential; a hub that
moved to another address is the same connection when its identifier is the
same.

**A delivery that is out of reach.** A deployment whose delivery fails with
`is out of reach of deliveries on this cluster` names an object in a namespace
of Open Cluster Management, in the resource manager's own access namespace, or,
on a hub's own cluster, an object of the hub (see Security). Change the
manifest; the delivery is tried again by itself and goes through once the
manifest no longer names such an object.

## Limits

The [implementation document](design/addons/existing-clusters-and-ocm-hubs.md)'s
"Limits and what is not built" is the full list; these are the ones a caller
of these requests meets:

- Every cluster of a connected hub is observed. A connection cannot be limited
  to some of a hub's clusters, and no cluster of a connected hub can be left
  out of observation.
- A link between an observed cluster and an enrolled one rests on the
  enrollment's record, on the cluster's identifier where the hub publishes it
  (the cluster claim `id.k8s.io`), on the API address and CA, or, where the hub
  records no address, on the name of exactly one known cluster — and on no name
  that another connected hub also has without an address. Aliases and
  corrections of a link are not built.
- Several hubs can be connected, and everyone who can sign in sees and uses
  all of them: access per hub connection is not built.
- A cluster that two connected hubs report is one cluster. It carries the
  labels of one hub, the one it enrolled through or was last adopted by
  (`hubs/{id}:adoptCluster`); labels of two hubs are not merged.
- The access form is direct: the resource manager dials the cluster. A cluster
  that accepts no inbound connection is not supported.
- The enrollment and hub actions (`accessManifest`, `hubAccessManifest`,
  `clusters:enroll`, `enrollments:present`, `hubs:connect`, `hubs:disconnect`,
  `hubs/{id}:generateEnrollmentAddon`, `hubs/{id}:adoptCluster`, and the
  deletions) are plain JSON handlers, not
  part of the protobuf API. `fleetctl hub` reaches `hubAccessManifest`,
  `hubs:connect`, the list of hubs, `hubs/{id}:generateEnrollmentAddon`, and
  `hubs:disconnect`; the others have no CLI command. The record reads and
  `-:queryResources` are part of the protobuf API, and the CLI reaches them.
- A record list answers `{}` when empty and carries at most 50 records unless
  `pageSize` asks for more, 500 at most; a record leaves out the fields that
  do not apply.
- Deleting a deployment removes what the resource manager wrote on the target.
  An object another manager wrote, or one the stored credential may neither
  read nor delete, is left in place and named, and the removal reports
  `partial` rather than `delivered`. Leaving what was delivered on purpose
  needs a withdrawal with `leaveDeployments`.

## Quick checklist

1. Sign in and keep the token: every request but `enrollments:present` needs
   it.
2. Remember the three authentication answers so a failure is recognizable;
   the record reads answer `text/plain`, the handlers and the query answer
   JSON.
3. Check `reason` on every failure; the request's table says what each word
   means.
4. Connect the hub before presenting anything it must vouch for. Connect each
   hub once, under an id of its own; a cluster of a hub is then
   `<hubId>.<name>`.
5. Give a cluster the access piece, then enroll it with a token of the access
   account.
6. Give the cluster's side either the add-on manifest or a bundle manifest;
   never copy the enrollment secret anywhere else. Say with `rollout` which
   clusters of the hub the add-on reaches.
7. Withdraw before forgetting; delete deployments or set `leaveDeployments`.
8. Revoke bundles that are done; disconnect hubs this resource manager no
   longer uses. After a hub is disconnected and connected again, generate its
   add-on again and apply it.
9. Treat the enrollment secret and the stored credential as secrets;
   `read-only` excludes Secrets on the cluster, while `workloads` (Kubernetes
   `edit`) and `cluster-admin` can read and write them.
10. Read the answer's condition, `state`, and `reason` rather than polling a
    fixed number of times; every wait in these examples has a bound.
