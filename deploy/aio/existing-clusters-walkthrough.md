# Existing clusters and OCM hubs: a walkthrough

This walkthrough takes a fresh clone to a running OCM Fleet resource manager that

- connects to an Open Cluster Management (OCM) hub with a read-only account and
  shows the hub's clusters without changing anything on the hub;
- enrolls one of those clusters through the hub's own add-on mechanism, then
  delivers to it, indexes it, searches it, and shows it in the console;
- leaves OCM-io working on the same cluster;
- enrolls a second cluster with a plain manifest and a read-only profile, with
  no hub object at all;
- ends an enrollment from either side, and removes what it created.

Every command is written out. Run the blocks in order, in one bash shell, from
the root of your clone. About 45 minutes, most of it building images and waiting
for the first index.

The resource manager never writes to the hub. What a hub or a cluster needs is
returned as a manifest for its administrator to apply; the commands below are
that administrator's, run through `kubectl`.

## 0. Get ready

### 0.1 What you need

- **Linux** with a systemd user session, and **podman** with its socket
  enabled. Rootless: `systemctl --user enable --now podman.socket`; the blocks
  below mount `$XDG_RUNTIME_DIR/podman/podman.sock` (set `PODMAN_SOCKET` if
  yours is somewhere else).
- **bash** with GNU coreutils and util-linux: the blocks use `base64 -w0`,
  `xargs -r`, and `column`.
- **kubectl** and **kind** (the all-in-one image carries kind v0.31.0 and
  kubectl v1.33.0), **clusteradm** (v1.4 or later), **jq**, **curl**, **git**.
- **Node.js 24 and npm** only if you build with the repository's Nx target;
  the direct Dockerfile commands below need only podman.
- **Memory**: three kind clusters (a hub and two managed clusters) take about
  6 GB together. If you already have a hub and two clusters, use them instead
  and skip the kind path.
- **Ports** 8085 and 50051 free on `127.0.0.1`.
- A clone of this repository.

The names below are placeholders; change them at the start if you like. The
commands use them everywhere.

| Variable    | Default                 | What it names                        |
| ----------- | ----------------------- | ------------------------------------ |
| `HUB_ID`    | `my-hub`                | the hub in the resource manager      |
| `CLUSTER1`  | `cluster1`              | the cluster enrolled through the hub |
| `CLUSTER2`  | `cluster2`              | the cluster enrolled by manifest     |
| `CONTAINER` | `ocm-fleet-walkthrough` | the resource manager container       |

### 0.2 Open a shell and define the helpers

Paste the whole block. It only defines names and functions and makes a work
directory; it writes nothing to your hub or clusters.

```bash
# Run everything below from the root of your clone, in one bash shell.
cd "$(git rev-parse --show-toplevel)"

HUB_ID=${HUB_ID:-my-hub}
CLUSTER1=${CLUSTER1:-cluster1}
CLUSTER2=${CLUSTER2:-cluster2}
CONTAINER=${CONTAINER:-ocm-fleet-walkthrough}
AIO_IMAGE=${AIO_IMAGE:-localhost/ocm-fleet:latest}
AGENT_IMAGE=${AGENT_IMAGE:-localhost/ocm-fleet-enrollment-agent:latest}
TARGET1=k8s-$HUB_ID.$CLUSTER1
TARGET2=k8s-$CLUSTER2
PT=${PT:-$(mktemp -d)}
mkdir -p "$PT"

FLEET=https://ocm-fleet.localhost:8085
K8S=/apis/kubernetes.open-cluster-management.io/v1alpha
OCM=/apis/ocm.open-cluster-management.io/v1alpha
CORE=/apis/open-cluster-management.io/v1alpha
CURL=(curl -sS --cacert "$PT/ca.crt" --resolve ocm-fleet.localhost:8085:127.0.0.1)

# fleet_login: sign in as the demo operator; the token stays in this shell.
fleet_login() {
  TOKEN=$("${CURL[@]}" -X POST $FLEET/idp/token -d grant_type=password \
    -d username=ops@ocm-fleet.local -d password=ocm-fleet-ops -d client_id=fleetctl \
    -d 'scope=openid profile email groups audience:server:client_id:resource-manager' | jq -r .id_token)
  if [ -n "$TOKEN" ] && [ "$TOKEN" != null ]; then echo "signed in"; else echo "sign-in failed"; fi
}
# fleet METHOD PATH [JSON]: one call to the resource manager's API.
fleet() {
  local method=$1 path=$2 body=${3:-}
  if [ -n "$body" ]; then
    "${CURL[@]}" -X "$method" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data-binary "$body" "$FLEET$path"
  else
    "${CURL[@]}" -X "$method" -H "Authorization: Bearer $TOKEN" "$FLEET$path"
  fi
  echo
}
# fleet_search FILTER [PAGE_SIZE]: the inventory query the console's search uses.
fleet_search() {
  "${CURL[@]}" -G -H "Authorization: Bearer $TOKEN" --data-urlencode "filter=$1" \
    --data-urlencode "pageSize=${2:-200}" "$FLEET$CORE/-:queryResources"
  echo
}
# fleet_count CLUSTER KIND: how many objects of that kind the inventory holds.
fleet_count() {
  fleet_search "resourceType == \"kubernetes.open-cluster-management.io/Object\" && name.startsWith(\"//kubernetes.open-cluster-management.io/clusters/$1/\") && resource.observation.kind == \"$2\"" 500 | jq '.resources | length'
}
# fleet_deploy ID PLACEMENT FILE...: a deployment of the Kubernetes objects in the files (JSON).
fleet_deploy() {
  local id=$1 placement=$2 f manifests='[]'; shift 2
  for f in "$@"; do
    manifests=$(jq -c --arg raw "$(base64 -w0 "$f")" '. + [{manifestType:"kubernetes",raw:$raw}]' <<<"$manifests")
  done
  fleet POST "/v1alpha/deployments?deploymentId=$id" "$(jq -cn --argjson m "$manifests" --argjson p "$placement" \
    '{manifestStrategy:{type:"TYPE_INLINE",manifests:$m},placementStrategy:$p}')"
}
# kubectl as the hub's administrator and as each cluster's administrator.
kc_hub() { kubectl --kubeconfig "$KUBECONFIG_HUB" "$@"; }
kc_c1()  { kubectl --kubeconfig "$KUBECONFIG_C1" "$@"; }
kc_c2()  { kubectl --kubeconfig "$KUBECONFIG_C2" "$@"; }
PLACEMENT1=$(jq -cn --arg t "$TARGET1" '{type:"TYPE_STATIC",targetIds:[$t]}')
PLACEMENT_LABEL='{"type":"TYPE_SELECTOR","targetSelector":{"matchLabels":{"walkthrough":"yes"}}}'
echo "work files: $PT"
```

### 0.3 Get a hub and two clusters

Pick one path. If you already have an OCM hub and two managed clusters, use
the second; it is quicker and needs no extra memory.

**Path A: create them with kind.** These three clusters take a few minutes. The
hub is initialized with `clusteradm`, and each cluster joins it in turn; the
hub approves each join (the certificate request), which the loop below does.
This path is for a machine with nothing on it.

<!-- skip-in-dry-run -->

```bash
export KIND_EXPERIMENTAL_PROVIDER=podman
export KUBECONFIG_HUB=${KUBECONFIG_HUB:-$PT/hub.kubeconfig}
export KUBECONFIG_C1=${KUBECONFIG_C1:-$PT/cluster1.kubeconfig}
export KUBECONFIG_C2=${KUBECONFIG_C2:-$PT/cluster2.kubeconfig}
export HUB_APISERVER=${HUB_APISERVER:-https://hub-control-plane:6443}
export CLUSTER2_APISERVER=${CLUSTER2_APISERVER:-https://${CLUSTER2}-control-plane:6443}
export HUB_NODE=${HUB_NODE:-hub-control-plane}

kind create cluster --name hub --kubeconfig "$KUBECONFIG_HUB" --wait 5m
kind create cluster --name "$CLUSTER1" --kubeconfig "$KUBECONFIG_C1" --wait 5m
kind create cluster --name "$CLUSTER2" --kubeconfig "$KUBECONFIG_C2" --wait 5m

clusteradm --kubeconfig "$KUBECONFIG_HUB" init --wait

# register NAME KUBECONFIG: join the hub, approve its request, wait until available
register() {
  local name=$1 kube=$2 n=0 hubtoken hubapi
  hubtoken=$(clusteradm --kubeconfig "$KUBECONFIG_HUB" get token -o json | jq -r '."hub-token"')
  hubapi=$(kubectl --kubeconfig "$KUBECONFIG_HUB" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  clusteradm --kubeconfig "$kube" join --hub-token "$hubtoken" --hub-apiserver "$hubapi" \
    --cluster-name "$name" --force-internal-endpoint-lookup
  until kubectl --kubeconfig "$KUBECONFIG_HUB" wait managedcluster/"$name" \
        --for=condition=ManagedClusterConditionAvailable --timeout=10s > /dev/null 2>&1; do
    (( ++n > 30 )) && { echo "gave up after five minutes: $name did not register (look at its certificate request on the hub)"; return 1; }
    kubectl --kubeconfig "$KUBECONFIG_HUB" get csr -o name \
      | xargs -r kubectl --kubeconfig "$KUBECONFIG_HUB" certificate approve > /dev/null 2>&1
    clusteradm --kubeconfig "$KUBECONFIG_HUB" accept --clusters "$name" > /dev/null 2>&1
    sleep 10
  done
  echo "$name is registered"
}

register "$CLUSTER1" "$KUBECONFIG_C1"
register "$CLUSTER2" "$KUBECONFIG_C2"
kc_hub get managedclusters
```

**Path B: use clusters you have.** Export the values below before you start, or
fill them in here. `KUBECONFIG_*` are files `kubectl` can read, with the
cluster's certificate authority embedded as `certificate-authority-data` (as
`kind` and `clusteradm` write them): the steps below read the CA from the file.
Path A writes the kubeconfigs under `$PT`, the work directory this walkthrough
creates; Path B uses your own files where they already are.
`HUB_APISERVER` and `CLUSTER2_APISERVER` are addresses the resource manager
container can reach on the `kind` network, such as
`https://cluster1-control-plane:6443`. The first cluster is enrolled through the
hub, so the address the hub records for it must be reachable from the container
too. `HUB_NODE` is the kind node of the hub, for the step that freezes it; leave
it empty if the hub is not a kind cluster.

```bash
KUBECONFIG_HUB=${KUBECONFIG_HUB:?set KUBECONFIG_HUB to the kubeconfig of your hub}
KUBECONFIG_C1=${KUBECONFIG_C1:?set KUBECONFIG_C1 to the kubeconfig of the first cluster}
KUBECONFIG_C2=${KUBECONFIG_C2:?set KUBECONFIG_C2 to the kubeconfig of the second cluster}
HUB_APISERVER=${HUB_APISERVER:?set HUB_APISERVER to the hub API address the container can reach}
CLUSTER2_APISERVER=${CLUSTER2_APISERVER:?set CLUSTER2_APISERVER to the second cluster API address the container can reach}
HUB_NODE=${HUB_NODE:-}
```

### 0.4 Build the images

The resource manager runs as the all-in-one container: API, console, and a peer
identity provider. The enrollment agent is a separate image that runs on a
cluster when it enrolls. With Node.js, `npm ci` installs the workspace and the
repository's Nx target builds the all-in-one image:

<!-- skip-in-dry-run -->

```bash
npm ci
npx nx run image:aio
podman build -f Dockerfile.enrollment-agent -t localhost/ocm-fleet-enrollment-agent:latest .
```

Without Node.js, the same Dockerfiles can be built directly. This is what the
rest of the walkthrough assumes: the images are tagged `localhost/ocm-fleet:latest`
and `localhost/ocm-fleet-enrollment-agent:latest`.

```bash
podman build -t localhost/ocm-fleet-server:latest .
podman build -f Dockerfile.local -t localhost/ocm-fleet-server-local:latest .
podman build --ulimit nofile=65536:65536 -f Dockerfile.web -t localhost/ocm-fleet-web:latest .
podman build -f Dockerfile.aio \
  --build-arg SERVER_IMAGE=localhost/ocm-fleet-server-local:latest \
  --build-arg WEB_IMAGE=localhost/ocm-fleet-web:latest \
  -t localhost/ocm-fleet:latest .
podman build -f Dockerfile.enrollment-agent -t localhost/ocm-fleet-enrollment-agent:latest .
```

The agent image has to be available to the clusters. With kind:

```bash
podman save -o "$PT/enrollment-agent.tar" "$AGENT_IMAGE"
for c in "$CLUSTER1" "$CLUSTER2"; do
  kind load image-archive "$PT/enrollment-agent.tar" --name "$c"
done
```

For clusters that are not kind, push the image to a registry those clusters can
pull from, and set `AGENT_IMAGE` to that reference before starting the container.

### 0.5 Start the resource manager on the kind network

The container needs the podman socket to reach the clusters, and the network
alias `ocm-fleet` so that the clusters can reach it. It keeps its data inside
the container; removing the container removes the data.

```bash
podman network create kind 2> /dev/null || true
export PODMAN_SOCKET=${PODMAN_SOCKET:-$XDG_RUNTIME_DIR/podman/podman.sock}
podman run -d --name "$CONTAINER" --privileged \
  -p 127.0.0.1:8085:8085 -p 127.0.0.1:50051:50051 \
  -v /tmp:/tmp -v "$PODMAN_SOCKET:/var/run/docker.sock" \
  --network kind:alias=ocm-fleet \
  -e OCM_RM_ENROLLMENT_AGENT_IMAGE="$AGENT_IMAGE" \
  "$AIO_IMAGE"

n=0; until curl -sk --resolve ocm-fleet.localhost:8085:127.0.0.1 \
      https://ocm-fleet.localhost:8085/readyz > /dev/null 2>&1; do
  (( ++n > 60 )) && { echo "gave up after two minutes: the resource manager is not ready (podman logs $CONTAINER)"; break; }
  sleep 2
done
podman cp "$CONTAINER:/data/sandbox/pki/ca.crt" "$PT/ca.crt"
```

The certificate is from the container's own sandbox authority. `curl` below
uses the copy in `$PT`; the browser takes the same certificate from the console
page itself.

### 0.6 Sign in

The all-in-one image starts a peer identity provider with two public sandbox
users. The operator is `ops@ocm-fleet.local` / `ocm-fleet-ops` (see
[the all-in-one README](README.md#demo-users-dex-on)).

```bash
fleet_login
fleet GET $CORE/targets | jq -r '.targets[]?.id'
```

You should see `signed in`. The helper functions above stand for you; `kc_hub`,
`kc_c1`, and `kc_c2` are plain `kubectl` with the administrator's kubeconfig of
that cluster, so they stand for the hub's and the clusters' administrators. The
resource manager has no such kubeconfig; it reads the hub through a read-only
account, and a cluster not at all until it enrolls.

Open **https://ocm-fleet.localhost:8085/app/** and accept the certificate
warning. Log in with the same user. The left menu has **Clusters**, **Hubs**,
**Targets**, and the search box; the other entries are demo content.

---

## 1. Connect the hub and look

The hub's administrator applies a small read-only account, then gives the
resource manager its address, certificate, and a token of that account. The
resource manager writes nothing.

### 1.1 Apply the read-only account and connect

```bash
fleet GET $OCM/hubAccessManifest | jq -r .manifest > "$PT/hub-access.yaml"
kc_hub apply -f "$PT/hub-access.yaml"

HUB_CA=$(kubectl --kubeconfig "$KUBECONFIG_HUB" config view --minify --raw \
  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d)
HUB_TOKEN=$(kc_hub -n ocm-fleet-hub-connection create token ocm-fleet-hub-reader --duration=720h)
fleet POST $OCM/hubs:connect "$(jq -cn \
  --arg id "$HUB_ID" --arg name "Walkthrough hub" --arg api "$HUB_APISERVER" \
  --arg ca "$HUB_CA" --arg token "$HUB_TOKEN" \
  '{hubId:$id, displayName:$name, apiServer:$api, caCert:$ca, token:$token}')" \
  | jq -c '{hub: .hub.name, reason, error}'

n=0; until [ "$(fleet GET $OCM/hubs/$HUB_ID | jq -r .conditions.Ready.status 2> /dev/null)" = True ]; do
  (( ++n > 60 )) && { echo "gave up after two minutes: the hub is not ready (check the address, the certificate, and the token)"; break; }
  sleep 2
done; (( n <= 60 )) && echo "hub is connected"
fleet GET $OCM/hubs/$HUB_ID | jq -c '{connected: .conditions.Connected.status, ready: .conditions.Ready.status, account: .observation.serviceAccount, canWriteToHub: .observation.writeAccessFound, clusters: .observation.clusterCount, enrolled: .observation.enrolledCount}'
```

```text
{"hub":"hubs/my-hub","reason":null,"error":null}
hub is connected
{"connected":"True","ready":"True","account":"ocm-fleet-hub-connection/ocm-fleet-hub-reader","canWriteToHub":false,"clusters":2,"enrolled":0}
```

The account lives in a namespace of its own on the hub,
`ocm-fleet-hub-connection`, apart from the access piece a cluster gets later
(`ocm-fleet-system`): a hub that is registered with itself can be enrolled like
any cluster, and the two must not share a namespace.

In the console, **Hubs** shows the hub with its cluster count, and **Clusters**
lists the hub's clusters as **Observed**. The count in the sample is this
walkthrough's two clusters; your hub's is its own. Nothing has been written to
the hub.

### 1.2 See the clusters

The hub's clusters come from the hub's own records; the resource manager did not
create them.

```bash
fleet_search 'resourceType == "ocm.open-cluster-management.io/ManagedCluster"' \
  | jq -r '.resources[].resource.observation | [.name, .enrollment.state] | @tsv' | column -t
echo "---"
fleet GET $CORE/targets | jq -r '.targets[] | [.id, .state] | @tsv' | column -t
```

```text
cluster1  observed
cluster2  observed
---
kind-local  ready
```

Every cluster the hub knows appears as `observed`: the resource manager sees
them only through the hub and has created none of them. A hub that is registered
with itself as a managed cluster shows its own `local-cluster` in this list too,
and a hub with more clusters shows them; the resource manager can enroll a hub's
own record like any other cluster. A cluster that this resource manager created
would show as `created`. `kind-local` is the resource manager's own local
target, not a hub cluster; your installation may show other targets of its own.
Neither `cluster1` nor `cluster2` is a target yet: they can be seen and
searched, and nothing can be delivered to them.

### 1.3 A change on the hub follows

Label a cluster as its hub's administrator, and search for the label. The
resource manager follows the hub within a few seconds.

```bash
kc_hub label managedcluster "$CLUSTER2" walkthrough=hello
n=0; until fleet_search 'resourceType == "ocm.open-cluster-management.io/ManagedCluster" && "walkthrough=hello" in resource.observation.searchTerms' \
      | jq -e '.resources | length > 0' > /dev/null 2>&1; do
  (( ++n > 30 )) && { echo "gave up after a minute: the label did not arrive"; break; }
  sleep 2
done
fleet_search 'resourceType == "ocm.open-cluster-management.io/ManagedCluster" && "walkthrough=hello" in resource.observation.searchTerms' \
  | jq -r '.resources[].resource | "\(.observation.name)  \(.localLabels)"'
kc_hub label managedcluster "$CLUSTER2" walkthrough-
```

```text
cluster2  {"cluster.open-cluster-management.io/clusterset":"default","walkthrough":"hello"}
```

The same label shows in the console's search box.

### 1.4 The account can only read

Ask the hub itself what the account may do:

```bash
SA=system:serviceaccount:ocm-fleet-hub-connection:ocm-fleet-hub-reader
for q in "list managedclusters" "watch managedclusters" "update managedclusters" "delete managedclusters" \
         "create manifestworks -n $CLUSTER1" "create managedclusteraddons -n $CLUSTER1" "get secrets -A"; do
  printf '%-42s %s\n' "$q" "$(kc_hub auth can-i $q --as=$SA 2>&1 | tail -n 1)"
done
printf '%s access piece: ' "$CLUSTER1"; kc_c1 get namespace ocm-fleet-system > /dev/null 2>&1 && echo present || echo absent
printf '%s access piece: ' "$CLUSTER2"; kc_c2 get namespace ocm-fleet-system > /dev/null 2>&1 && echo present || echo absent
```

The account answers `yes` to listing and watching and `no` to everything that
would change the hub; no cluster has the access piece yet. Beside managed
clusters and cluster sets the account may read one namespace, `kube-system`,
by name: its UID is how the resource manager recognizes this hub when it is
connected again, also at another address.

---

## 2. Enroll one cluster through the hub

`cluster1` opts in. The hub's administrator uses OCM-io's own add-on mechanism;
the resource manager only returns what to apply.

### 2.1 Note the OCM-io agent, to compare later

```bash
kc_c1 -n open-cluster-management-agent get pods \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[*].restartCount' \
  | tee "$PT/klusterlet-before.txt"
```

### 2.2 Generate the enrollment add-on

The request names the access profile (`workloads`: deploy and read workloads,
read the rest) and grants the stored credential the right to apply deployments
that are not signed. It returns one manifest for the hub.

```bash
fleet POST $OCM/hubs/$HUB_ID:generateEnrollmentAddon '{"profile":"workloads","grantPlatformDelivery":true}' > "$PT/addon.json"
jq -r .manifest "$PT/addon.json" > "$PT/addon.yaml"
kc_hub apply -f "$PT/addon.yaml"
```

```text
addontemplate.addon.open-cluster-management.io/ocm-fleet-enrollment created
clustermanagementaddon.addon.open-cluster-management.io/ocm-fleet-enrollment created
```

Nothing has changed on a cluster yet; the hub now has the add-on for all its
clusters, and the resource manager made the enrollment secret that the add-on
carries. This walkthrough enables the add-on for one cluster by hand, which is
the default. A request with `"rollout":"every-cluster"` returns a manifest
that also brings a `Placement` selecting every cluster of the hub, and
`"rollout":"placement"` follows a `Placement` you name; OCM-io's add-on manager
then enables the add-on for those clusters by itself ([the API
reference](../../docs/existing-clusters-api.md#post-hubsidgenerateenrollmentaddon)
has the three ways).

### 2.3 Enable it for `cluster1`

This is the second half of what the console's **Enroll clusters** dialog
returns: a `ManagedClusterAddOn` in the cluster's namespace on the hub.

```bash
kc_hub apply -f - <<EOF
apiVersion: addon.open-cluster-management.io/v1alpha1
kind: ManagedClusterAddOn
metadata:
  name: ocm-fleet-enrollment
  namespace: $CLUSTER1
spec: {}
EOF

n=0; until fleet GET $CORE/targets | jq -e --arg id "$TARGET1" '.targets[] | select(.id==$id and .state=="ready")' > /dev/null 2>&1; do
  (( ++n > 60 )) && { echo "gave up after a minute: $CLUSTER1 is not enrolled (look at the add-on and the agent)"; break; }
  sleep 1
done; (( n <= 60 )) && echo "$CLUSTER1 is enrolled"
```

`cluster1 is enrolled` appears after a few seconds. Every wait in this
walkthrough gives up after a stated time and says so, instead of waiting
forever.

### 2.4 Look at what happened

```bash
fleet GET $CORE/targets | jq --arg id "$TARGET1" '.targets[] | select(.id==$id) | {id, state, origin, accessForm, deliveryCredential, labels}'
fleet GET $K8S/clusters/$HUB_ID.$CLUSTER1 | jq '.observation | {state, profile, rights, serviceAccount, source, credentialExpiresAt}'
kc_c1 -n ocm-fleet-system get serviceaccount,role,rolebinding,deployment,secret
kc_hub -n "$CLUSTER1" get managedclusteraddon,manifestwork
kc_c1 -n open-cluster-management-agent get pods \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[*].restartCount' \
  | diff "$PT/klusterlet-before.txt" - && echo "OCM-io agent pods: unchanged"
```

The target carries the labels the cluster has on the hub, the rights are what
the cluster itself reports (`workloads` yes, cluster administrator no), and the
credential runs out in 12 hours; the resource manager renews it by itself.

On the cluster: one namespace `ocm-fleet-system` with the access account
`ocm-fleet-access`, a role for its own token, two cluster role bindings (the
built-in `edit` role and a small reading role), and the enrollment agent with
its own account and secret. On the hub: the two add-on objects and, for this
cluster, a `ManagedClusterAddOn` and the `ManifestWork` with which OCM-io's own
add-on manager installs the agent. The resource manager created none of them.
`OCM-io agent pods: unchanged` means the klusterlet was not touched.

---

## 3. Use the enrolled cluster

### 3.1 The inventory is the cluster

The first index takes up to a minute and fills in kind by kind, in no fixed
order. The loop waits for each kind and prints the counts.

```bash
for kind in Pod Namespace Node; do
  n=0; until [ "$(fleet_count "$HUB_ID.$CLUSTER1" $kind 2> /dev/null)" = "$(kc_c1 get $kind -A --no-headers 2> /dev/null | wc -l)" ]; do
    (( ++n > 90 )) && { echo "$kind: gave up after three minutes"; break; }
    sleep 2
  done
  echo "$kind: cluster has $(kc_c1 get $kind -A --no-headers | wc -l), inventory has $(fleet_count "$HUB_ID.$CLUSTER1" $kind)"
done
```

### 3.2 Deploy a workload by naming the target

A ConfigMap and a Deployment, as JSON files. The deployment API takes JSON, not
YAML.

```bash
cat > "$PT/web-config.json" <<'EOF'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"walkthrough-config","namespace":"default"},"data":{"from":"ocm-fleet"}}
EOF
cat > "$PT/web.json" <<'EOF'
{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"walkthrough-web","namespace":"default","labels":{"app":"walkthrough-web"}},
 "spec":{"replicas":1,"selector":{"matchLabels":{"app":"walkthrough-web"}},
  "template":{"metadata":{"labels":{"app":"walkthrough-web"}},"spec":{"containers":[{"name":"pause","image":"registry.k8s.io/pause:3.10"}]}}}}
EOF
fleet_deploy walkthrough-web "$PLACEMENT1" "$PT/web-config.json" "$PT/web.json" | jq -c '{name, state}'
sleep 5
fleet GET /v1alpha/deployments/walkthrough-web | jq -c '{state, resolvedTargetIds}'
kc_c1 -n default get deployment,pod -l app=walkthrough-web
kc_c1 -n default get configmap walkthrough-config --show-managed-fields -o json \
  | jq -c '{managers: [.metadata.managedFields[] | "\(.manager)/\(.operation)"], owners: (.metadata.ownerReferences // []), labels: (.metadata.labels // {}), annotations: (.metadata.annotations // {})}'
```

```text
{"name":"deployments/walkthrough-web","state":"STATE_CREATING"}
{"state":"STATE_ACTIVE","resolvedTargetIds":["k8s-my-hub.cluster1"]}
```

The objects were written by `resource-manager` with server-side apply, and
nothing was injected into them: no owner reference, no label, no annotation.
The pod runs Kubernetes' public pause image, which keeps no code: the step is
about delivery, not about the workload. If your clusters cannot pull that image,
use one they can.

### 3.3 The hub holds nothing for the delivery

```bash
echo "works in $CLUSTER1: $(kc_hub -n "$CLUSTER1" get manifestworks -o name)"
echo "works that mention walkthrough: $(kc_hub get manifestworks -A -o name | grep -c walkthrough)"
```

The only work in the cluster's namespace is the add-on's own, which installs the
agent; the OCM-io `ManifestWork` route would have cost one object on the hub and
one on the cluster for every delivery.

### 3.4 Deploy by a label that lives on the hub

The hub's administrator labels the cluster on the hub; the label reaches the
target; a deployment selects by it.

```bash
kc_hub label managedcluster "$CLUSTER1" walkthrough=yes
n=0; until fleet GET $CORE/targets | jq -e --arg id "$TARGET1" '.targets[] | select(.id==$id) | .labels.walkthrough=="yes"' > /dev/null 2>&1; do
  (( ++n > 60 )) && { echo "gave up after a minute: the label is not on the target"; break; }
  sleep 1
done; (( n <= 60 )) && echo "label is on the target"
cat > "$PT/by-label.json" <<'EOF'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"walkthrough-by-label","namespace":"default"},"data":{"placed-by":"hub label walkthrough=yes"}}
EOF
fleet_deploy walkthrough-by-label "$PLACEMENT_LABEL" "$PT/by-label.json" | jq -c '{name, state}'
sleep 5
fleet GET /v1alpha/deployments/walkthrough-by-label | jq -c '{state, resolvedTargetIds}'
kc_c1 -n default get configmap walkthrough-by-label -o jsonpath='{.data}'; echo
```

### 3.5 Find it

```bash
fleet_search 'resourceType == "kubernetes.open-cluster-management.io/Object" && resource.observation.kind == "Pod" && resource.observation.metadata.name.startsWith("walkthrough-web-")' | jq -r '.resources[].name'
```

The console's search box finds the pod, and **Targets** lists the cluster's
deployments.

### 3.6 Delete a deployment: what it delivered goes with it

```bash
fleet DELETE /v1alpha/deployments/walkthrough-by-label | jq -c '{name, state}'
sleep 5
kc_c1 -n default get configmap walkthrough-by-label
```

The deployment moves to `STATE_DELETING`, and the ConfigMap is gone from the
cluster.

---

## 4. OCM-io and the resource manager on the same cluster

OCM-io keeps working for `cluster1`. The hub's administrator delivers something
the OCM-io way, then both systems' objects are compared.

```bash
kc_hub apply -f - <<EOF
apiVersion: work.open-cluster-management.io/v1
kind: ManifestWork
metadata:
  name: walkthrough-from-ocm
  namespace: $CLUSTER1
spec:
  workload:
    manifests:
    - apiVersion: v1
      kind: ConfigMap
      metadata:
        name: walkthrough-from-ocm
        namespace: default
      data:
        from: ocm-io
EOF
sleep 5
for cm in walkthrough-from-ocm walkthrough-config; do
  printf '%-24s ' "$cm"; kc_c1 -n default get configmap "$cm" --show-managed-fields -o json \
    | jq -c '{from: .data.from, managers: [.metadata.managedFields[].manager], owners: [(.metadata.ownerReferences // [])[].kind]}'
done
```

```text
walkthrough-from-ocm      {"from":"ocm-io","managers":["work-agent"],"owners":["AppliedManifestWork"]}
walkthrough-config        {"from":"ocm-fleet","managers":["resource-manager"],"owners":[]}
```

Now ask the resource manager to change the object that belongs to OCM-io:

```bash
cat > "$PT/clash.json" <<'EOF'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"walkthrough-from-ocm","namespace":"default"},"data":{"from":"ocm-fleet-wants-this"}}
EOF
fleet_deploy walkthrough-clash "$PLACEMENT1" "$PT/clash.json" | jq -c '{name, state}'
sleep 15
fleet GET /v1alpha/deployments/walkthrough-clash \
  | jq -c --arg target "$TARGET1" '{state, refused: [.targetStatuses[] | select(.targetId==$target and .message != "") | {target: .targetId, message}]}'
echo "on the cluster: from=$(kc_c1 -n default get configmap walkthrough-from-ocm -o jsonpath='{.data.from}')"
```

```text
{"state":"STATE_CREATING","refused":[{"target":"k8s-my-hub.cluster1","message":"apply manifest 1: apply ConfigMap default/walkthrough-from-ocm: Apply failed with 1 conflict: conflict with \"work-agent\" using v1: .data.from"}]}
on the cluster: from=ocm-io
```

The resource manager never forces. The deployment does not become active,
OCM-io's value is untouched, and the deployment itself says why: the delivery
on the target failed, and its message names the other manager. The
orchestration keeps retrying (the failed `attempts` grow) and the reason stays
until a delivery succeeds. Delete the test objects:

```bash
fleet DELETE /v1alpha/deployments/walkthrough-clash | jq -c '{name, state}'
sleep 3
echo "still there, still OCM-io's: from=$(kc_c1 -n default get configmap walkthrough-from-ocm -o jsonpath='{.data.from}')"
kc_hub -n "$CLUSTER1" delete manifestwork walkthrough-from-ocm
```

---

## 5. Take the add-on away; the cluster stays enrolled

The hub's objects were needed to get the access piece onto the cluster. They are
not needed to keep it: the resource manager talks to the cluster's own API with
the credential the cluster issued.

```bash
kc_hub -n "$CLUSTER1" delete managedclusteraddon ocm-fleet-enrollment
sleep 5
echo "--- on the hub, for $CLUSTER1:"; kc_hub -n "$CLUSTER1" get managedclusteraddons,manifestworks
echo "--- on $CLUSTER1:";              kc_c1 -n ocm-fleet-system get serviceaccount,deployment,pod
echo "--- still a target?";            fleet GET $CORE/targets | jq -c --arg id "$TARGET1" '.targets[] | select(.id==$id) | {id, state, origin}'
```

The hub holds nothing for the cluster; the agent is gone and nothing of the
resource manager runs there; the access account stays. Renew the credential and
deploy with nothing running on the cluster:

```bash
fleet GET $K8S/clusters/$HUB_ID.$CLUSTER1 | jq -c '.observation | {credentialRenewedAt, credentialExpiresAt}'
fleet POST $K8S/clusters/$HUB_ID.$CLUSTER1:renewCredential '{}' > /dev/null
fleet GET $K8S/clusters/$HUB_ID.$CLUSTER1 | jq -c '.observation | {credentialRenewedAt, credentialExpiresAt}'
cat > "$PT/after.json" <<'EOF'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"walkthrough-after-addon","namespace":"default"},"data":{"from":"ocm-fleet"}}
EOF
fleet_deploy walkthrough-after-addon "$PLACEMENT1" "$PT/after.json" | jq -c '{name, state}'
sleep 5
fleet GET /v1alpha/deployments/walkthrough-after-addon | jq -c '{state}'
```

The two times change, and the deployment becomes active.

---

## 6. Try to cheat

An enrollment is a cluster presenting a credential. Get an enrollment secret and
try four lies with it. `cluster2` is the cluster the lies name; it is not
enrolled yet.

```bash
fleet POST $OCM/hubs/$HUB_ID:generateEnrollmentAddon '{"profile":"workloads"}' > "$PT/cheat.json"
SECRET=$(jq -r .manifest "$PT/cheat.json" | awk '/^ +token: eb-/ {print $2; exit}')
present() { "${CURL[@]}" -X POST -H "Authorization: Bearer $1" -H 'Content-Type: application/json' --data-binary "$2" -w '  -> HTTP %{http_code}\n' $FLEET$K8S/enrollments:present; }
ca()      { kubectl --kubeconfig "$1" config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d; }
body()    { jq -cn --arg n "$1" --arg ca "$2" --arg t "$3" '{clusterName:$n, caCert:$ca, token:$t}'; }

echo "1. a wrong secret:";                              present "${SECRET%%.*}.wrong" "$(body "$CLUSTER2" "$(ca "$KUBECONFIG_C2")" x)"
echo "2. a cluster the hub does not know:";            present "$SECRET" "$(body no-such-cluster "$(ca "$KUBECONFIG_C2")" x)"
echo "3. $CLUSTER2's name, another cluster's certificate:"; present "$SECRET" "$(body "$CLUSTER2" "$(ca "$KUBECONFIG_C1")" x)"
echo "4. $CLUSTER2, but the token of some other account:";  present "$SECRET" "$(body "$CLUSTER2" "$(ca "$KUBECONFIG_C2")" "$(kc_c2 -n default create token default --duration=10m)")"
echo "targets now: $(fleet GET $CORE/targets | jq -r '[.targets[].id] | join(" ")')"
fleet DELETE $K8S/enrollmentBundles/$(jq -r .bundle.id "$PT/cheat.json") > /dev/null
```

```text
1. a wrong secret:
{"error":"the enrollment bundle secret is not valid","reason":"bundle-secret-mismatch"}
  -> HTTP 401
2. a cluster the hub does not know:
{"error":"hub \"my-hub\" does not know a cluster named \"no-such-cluster\"","reason":"cluster-not-on-hub"}
  -> HTTP 403
3. cluster2's name, another cluster's certificate:
{"error":"the CA presented for cluster \"cluster2\" is not the set of CAs hub \"my-hub\" records for any of its addresses","reason":"ca-mismatch"}
  -> HTTP 422
4. cluster2, but the token of some other account:
{"error":"the cluster says the credential belongs to \"system:serviceaccount:default:default\"; the access account \"ocm-fleet-access\" is expected","reason":"unexpected-identity"}
  -> HTTP 422
targets now: k8s-my-hub.cluster1 kind-local
```

Each attempt is refused with its reason, and no target is created. The list is
the one from step 1 plus `k8s-my-hub.cluster1`; your installation may show
other targets of its own, but nothing for `cluster2` appears. The secret is
thrown away by the last line.

---

## 7. Enroll a second cluster with no hub object, and with fewer rights

`cluster2` is enrolled by its own administrator with a plain manifest. The hub
is not involved; the profile is `read-only`.

```bash
fleet POST $K8S/enrollmentBundles "$(jq -cn --arg id "$CLUSTER2" --arg api "$CLUSTER2_APISERVER" \
  '{clusterId:$id, apiServer:$api, profile:"read-only", grantPlatformDelivery:true}')" > "$PT/cluster2.json"
jq -r .manifest "$PT/cluster2.json" > "$PT/cluster2.yaml"
kc_c2 apply -f "$PT/cluster2.yaml"
n=0; until fleet GET $CORE/targets | jq -e --arg id "$TARGET2" '.targets[] | select(.id==$id and .state=="ready")' > /dev/null 2>&1; do
  (( ++n > 60 )) && { echo "gave up after a minute: $CLUSTER2 is not enrolled (look at the job on the cluster)"; break; }
  sleep 1
done; (( n <= 60 )) && echo "$CLUSTER2 is enrolled"
```

A job runs once on the cluster and finishes. Look at what the read-only profile
sees:

```bash
sleep 20
for kind in Pod Namespace Node; do
  n=0; until [ "$(fleet_count "$CLUSTER2" $kind 2> /dev/null)" = "$(kc_c2 get $kind -A --no-headers 2> /dev/null | wc -l)" ]; do
    (( ++n > 90 )) && { echo "$kind: gave up after three minutes"; break; }
    sleep 2
  done
done
kc_c2 -n ocm-fleet-system get job
echo "hub objects for $CLUSTER2: $(kc_hub -n "$CLUSTER2" get managedclusteraddons,manifestworks --no-headers 2> /dev/null | wc -l)"
fleet GET $K8S/clusters/$CLUSTER2 | jq -c '.observation | {state, profile, rights}'
echo "indexed: $(fleet_count "$CLUSTER2" Pod) pods, $(fleet_count "$CLUSTER2" Node) nodes, $(fleet_count "$CLUSTER2" Namespace) namespaces, $(fleet_count "$CLUSTER2" Secret) secrets (the cluster has $(kc_c2 get secrets -A --no-headers | wc -l) secrets)"
```

```text
{"state":"enrolled","profile":"read-only","rights":{"canRenew":true,"clusterAdmin":false,"namespaces":false,"readCluster":true,"workloads":false}}
```

The read-only profile reads the cluster but not its secrets, and cannot change
it. Try to deploy to it anyway:

```bash
cat > "$PT/ro.json" <<'EOF'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"walkthrough-readonly","namespace":"default"},"data":{"from":"ocm-fleet"}}
EOF
fleet_deploy walkthrough-readonly "$(jq -cn --arg t "$TARGET2" '{type:"TYPE_STATIC",targetIds:[$t]}')" "$PT/ro.json" | jq -c '{name, state}'
sleep 8
fleet GET /v1alpha/deployments/walkthrough-readonly | jq -r .pauseReason
kc_c2 -n default get configmap walkthrough-readonly
fleet DELETE /v1alpha/deployments/walkthrough-readonly > /dev/null
```

The deployment pauses with the cluster's own refusal in `pauseReason`, and
nothing is created.

---

## 8. Freeze the hub

This step needs control of the hub's node; it is written for a kind hub. Skip it
if you use a hub you cannot pause. Keep the hub frozen for a minute or two, not
longer, and do not skip the block that brings it back.

```bash
if [ -n "${HUB_NODE:-}" ]; then
  podman pause "$HUB_NODE"
  n=0; until [ "$(fleet GET $OCM/hubs/$HUB_ID | jq -r .conditions.Connected.status 2> /dev/null)" = False ]; do
    (( ++n > 60 )) && { echo "gave up after two minutes: the hub is not reported unreachable (unpause it with the block below)"; break; }
    sleep 2
  done
  fleet GET $OCM/hubs/$HUB_ID | jq -c '{connected: .conditions.Connected.status, reason: .conditions.Connected.reason}'
  echo "observed cluster2 is stale: $(fleet_search 'resourceType == "ocm.open-cluster-management.io/ManagedCluster"' | jq -r '[.resources[].resource.observation | select(.name=="'"$CLUSTER2"'") | .enrollment.state] | join(" ")')"
  echo "enrolled cluster1 stays ready: $(fleet GET $CORE/targets | jq -r --arg id "$TARGET1" '[.targets[] | select(.id==$id) | .state] | join(" ")')"
  cat > "$PT/away.json" <<'EOF'
{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"walkthrough-hub-away","namespace":"default"},"data":{"when":"hub frozen"}}
EOF
  fleet_deploy walkthrough-hub-away "$PLACEMENT_LABEL" "$PT/away.json" | jq -c '{name, state}'
  sleep 5
  fleet GET /v1alpha/deployments/walkthrough-hub-away | jq -c '{state, resolvedTargetIds}'
  kc_c1 -n default get configmap walkthrough-hub-away -o jsonpath='{.data}'; echo
  podman unpause "$HUB_NODE"
  n=0; until [ "$(fleet GET $OCM/hubs/$HUB_ID | jq -r .conditions.Ready.status 2> /dev/null)" = True ]; do
    (( ++n > 60 )) && { echo "gave up after two minutes: the hub is not back (unpause it: podman unpause $HUB_NODE)"; break; }
    sleep 2
  done; (( n <= 60 )) && echo "hub is back"
  kc_hub get managedclusters
fi
```

Once a cluster is enrolled, delivering to it does not depend on its hub. The
labels that came from the hub stay usable while the hub is away, and the
resource manager says plainly that the hub does not answer instead of showing
its clusters as gone. The record of an enrolled cluster says the same about its
labels: once the hub has not answered for about a minute, the record's
condition `HubFactsCurrent` turns `False`, and back when the hub answers again.

### 8.1 More than one hub

This walkthrough has one hub. A second hub is connected the same way, with its
own read-only account and under an id of its own, and what you have seen holds
for each of them:

- A cluster is named `<hub id>.<name on the hub>`, so two hubs that each have a
  `cluster1`, or each have a `local-cluster`, have two clusters here, with two
  targets.
- Each hub has its own enrollment add-on and its own enrollment secret; a
  cluster of one hub cannot enroll with the other hub's secret.
- A label selects across hubs. The target labels
  `ocm.open-cluster-management.io/hub` and
  `ocm.open-cluster-management.io/hub-clusterset` narrow a selector to one hub
  or to one hub's cluster set.
- A hub that stops answering, as in this step, changes nothing for the other
  hubs.
- The same hub cannot be connected twice: under a second id it is refused, at
  whatever address it is given.

On the cluster a hub runs on, when it is enrolled, the resource manager refuses
to deliver into the namespaces of Open Cluster Management and to write the
hub's own objects. [The API
reference](../../docs/existing-clusters-api.md#security-for-operators) says
what is out of reach.

---

## 9. End it, from either side

### 9.1 The cluster's administrator takes `cluster2` back

No call to the resource manager: the administrator removes the access piece on
the cluster.

```bash
kc_c2 delete namespace ocm-fleet-system
kc_c2 delete clusterrolebinding,clusterrole -l app.kubernetes.io/part-of=ocm-fleet
n=0; until [ "$(fleet GET $K8S/clusters/$CLUSTER2 | jq -r .observation.state 2> /dev/null)" = withdrawn ]; do
  (( ++n > 100 )) && { echo "gave up after five minutes: $CLUSTER2 is not reported withdrawn"; break; }
  sleep 3
done
fleet GET $K8S/clusters/$CLUSTER2 | jq -c '.observation | {state, withdrawnBy, withdrawnReason}'
echo "targets: $(fleet GET $CORE/targets | jq -r '[.targets[].id] | join(" ")')"
echo "the hub says: $(kc_hub get managedcluster "$CLUSTER2" --no-headers)"
```

After one to two minutes the resource manager reports the cluster as withdrawn
by the cluster, drops its target and indexed objects, and leaves the hub's
record alone.

### 9.2 The resource manager's side withdraws `cluster1`

```bash
kc_c1 -n open-cluster-management-agent get pods \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[*].restartCount' > "$PT/klusterlet-before-withdraw.txt"
fleet POST $K8S/clusters/$HUB_ID.$CLUSTER1:withdraw '{"reason":"walkthrough"}' | jq -c '{reason, error, deployments: .details.deployments}'
fleet POST $K8S/clusters/$HUB_ID.$CLUSTER1:withdraw '{"reason":"walkthrough","leaveDeployments":true}' | jq -c '{state: .cluster.observation.state}'
sleep 3
echo "targets: $(fleet GET $CORE/targets | jq -r '[.targets[].id] | join(" ")')"
echo "--- still running on $CLUSTER1:"; kc_c1 -n default get deployment,configmap -o name | grep walkthrough
echo "--- the hub says:";               kc_hub get managedcluster "$CLUSTER1" --no-headers
kc_c1 -n open-cluster-management-agent get pods \
  -o custom-columns='NAME:.metadata.name,UID:.metadata.uid,RESTARTS:.status.containerStatuses[*].restartCount' \
  | diff "$PT/klusterlet-before-withdraw.txt" - && echo "OCM-io agent pods: unchanged"
```

The first call is refused and names the deployments that still point at the
cluster; the second leaves on the cluster what they delivered. The target, the
stored credential, and the indexed objects go; the workloads stay; the
cluster's registration with its hub and its OCM-io agent are never touched.

The console offers the same withdrawal on an enrolled cluster's row and page:
it shows the refusal's deployment names and a **Withdraw and leave them**
choice, and once the record is withdrawn, **Forget record** removes it. A
bundle generated there enrolls a cluster with no hub the same way step 7 does.

---

## 10. Remove what the walkthrough created

This block works from any point after step 1. It prints what it deletes; lines
that say nothing was found are fine.

```bash
# in case the hub is still frozen (step 8)
if [ -n "${HUB_NODE:-}" ]; then podman unpause "$HUB_NODE" 2> /dev/null || true; fi
# the hub's administrator: the add-on, the test label, the test work
kc_hub -n "$CLUSTER1" delete managedclusteraddon ocm-fleet-enrollment --ignore-not-found
kc_hub delete clustermanagementaddon,addontemplate ocm-fleet-enrollment --ignore-not-found
kc_hub -n "$CLUSTER1" delete manifestwork walkthrough-from-ocm --ignore-not-found
kc_hub label managedcluster "$CLUSTER1" walkthrough- > /dev/null 2>&1
kc_hub label managedcluster "$CLUSTER2" walkthrough- > /dev/null 2>&1
# the resource manager's side: bundles, deployments, then the clusters and their records
for b in $(fleet GET $K8S/enrollmentBundles | jq -r '.enrollmentBundles[]?.name | sub("^enrollmentBundles/";"")'); do
  fleet DELETE $K8S/enrollmentBundles/$b > /dev/null
done
for d in $(fleet GET "/v1alpha/deployments?pageSize=200" | jq -r '.deployments[]?.name | select(startswith("deployments/walkthrough")) | sub("^deployments/";"")'); do
  fleet DELETE /v1alpha/deployments/$d > /dev/null
done
sleep 5
for c in "$HUB_ID.$CLUSTER1" "$CLUSTER2"; do
  fleet POST $K8S/clusters/$c:withdraw '{"reason":"walkthrough cleanup","leaveDeployments":true}' > /dev/null
  fleet DELETE $K8S/clusters/$c > /dev/null
done
# the clusters' administrators: the access piece and what was left on the clusters
for k in "$KUBECONFIG_C1" "$KUBECONFIG_C2"; do
  kubectl --kubeconfig "$k" delete namespace ocm-fleet-system --ignore-not-found
  kubectl --kubeconfig "$k" delete clusterrolebinding,clusterrole -l app.kubernetes.io/part-of=ocm-fleet --ignore-not-found
done
kc_c1 -n default delete deployment/walkthrough-web configmap/walkthrough-config configmap/walkthrough-by-label \
  configmap/walkthrough-after-addon configmap/walkthrough-hub-away configmap/walkthrough-from-ocm --ignore-not-found
# the hub connection and the read-only account this walkthrough created
fleet POST $OCM/hubs/$HUB_ID:disconnect '{}' > /dev/null
sleep 3
echo "hubs: $(fleet GET $OCM/hubs | jq -c '[.hubs[]?.name]')"
echo "targets: $(fleet GET $CORE/targets | jq -r '[.targets[]?.id] | join(" ")')"
echo "clusters: $(fleet GET $K8S/clusters | jq -c .)   bundles: $(fleet GET $K8S/enrollmentBundles | jq -c .)"
echo "left over: $(fleet GET "/v1alpha/deployments?pageSize=200" | jq -r '[.deployments[].name | select(startswith("deployments/walkthrough"))] | length') walkthrough deployments"
echo "works that mention walkthrough: $(kc_hub get manifestworks -A -o name | grep -c walkthrough)"
podman rm -f "$CONTAINER" > /dev/null 2>&1 || true
podman rmi localhost/ocm-fleet:latest localhost/ocm-fleet-server:latest \
  localhost/ocm-fleet-server-local:latest localhost/ocm-fleet-web:latest \
  localhost/ocm-fleet-enrollment-agent:latest > /dev/null 2>&1 || true
rm -rf "$PT"
```

The read-only account on the hub is separate: it is the hub administrator's to
remove, and removing it ends every connection that uses it. If the hub is yours
alone, and no other resource manager uses the account, uncomment and run:

<!-- skip-in-dry-run -->

```bash
# kc_hub delete namespace ocm-fleet-hub-connection --ignore-not-found
# kc_hub delete clusterrole,clusterrolebinding ocm-fleet-hub-reader --ignore-not-found
```

If step 0.3 created these kind clusters, uncomment and run this; do not run it
against a hub or clusters you already had.

<!-- skip-in-dry-run -->

```bash
# kind delete cluster --name hub
# kind delete cluster --name "$CLUSTER1"
# kind delete cluster --name "$CLUSTER2"
```
