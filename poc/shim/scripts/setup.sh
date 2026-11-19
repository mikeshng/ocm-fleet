#!/usr/bin/env bash
# Creates the environment of the demo on this machine, or starts it again: two Open Cluster
# Management hubs with two managed clusters each, as kind clusters, and one OCM Fleet.
#
#   scripts/setup.sh         create or start whatever is missing, then put the demo at its
#                            starting state. Safe to repeat, also after a restart of the
#                            machine. Creating everything takes about seven minutes; the
#                            very first run takes longer, because it also downloads kind's
#                            node image and builds the images of OCM Fleet.
#   scripts/setup.sh down    delete the six kind clusters and the OCM Fleet container
#
# It needs kind, podman (rootless, with its socket enabled), kubectl, clusteradm (v1.4 or
# later), jq, and curl, and about 8 GB of free memory.
#
# What it creates:
#   kind cluster hub1            a hub (clusteradm init)
#   kind cluster hub1-cluster1   a managed cluster of hub1, named cluster1 on the hub
#   kind cluster hub1-cluster2   a managed cluster of hub1, named cluster2 on the hub
#   kind cluster hub2            a second hub
#   kind cluster hub2-cluster1   a managed cluster of hub2, named cluster1 on the hub
#   kind cluster hub2-cluster2   a managed cluster of hub2, named cluster2 on the hub
#   container ocm-fleet-aio      OCM Fleet (the all-in-one image), on the kind network
# Open Cluster Management is installed as released, with clusteradm. No hub is registered
# with itself: every managed cluster is a cluster that runs workloads.
#
# Environment: AIO_IMAGE and AGENT_IMAGE (by default localhost/ocm-fleet:latest and
# localhost/ocm-fleet-enrollment-agent:latest, which are built from this clone when they are
# missing), and PODMAN_SOCKET.
set -uo pipefail

. "$(dirname "$(readlink -f "$0")")/lib.sh"

AIO_IMAGE=${AIO_IMAGE:-localhost/ocm-fleet:latest}
AGENT_IMAGE=${AGENT_IMAGE:-localhost/ocm-fleet-enrollment-agent:latest}
PODMAN_SOCKET=${PODMAN_SOCKET:-${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock}
export KIND_EXPERIMENTAL_PROVIDER=podman

own() { kubectl --kubeconfig "$WORK/kubeconfigs/$1" "${@:2}"; }
exists() { kind get clusters 2> /dev/null | grep -qx "$1"; }

# Every container of every pod in a kind node takes a key from the kernel's quota for one
# user (kernel.keys.maxkeys, 200 by default), because the container runtime gives each
# container a keyring of its own. Six clusters with Open Cluster Management on them do not
# fit into that. With NoNewKeyring the runtime makes no keyring per container.
kind_config() {
  cat > "$WORK/kind.yaml" << 'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
      NoNewKeyring = true
EOF
}

create_cluster() { # NAME
  local name=$1 node=$1-control-plane
  if exists "$name"; then
    if [[ $(podman inspect -f '{{.State.Status}}' "$node" 2> /dev/null) != running ]]; then
      podman start "$node" > /dev/null || return 1
      echo "kind cluster $name: started again"
    else
      echo "kind cluster $name: exists"
    fi
  else
    kind_config
    if ! kind create cluster --name "$name" --config "$WORK/kind.yaml" --kubeconfig "$WORK/kubeconfigs/$name" --wait 5m > "$WORK/kind-$name.log" 2>&1; then
      tail -n 5 "$WORK/kind-$name.log"
      return 1
    fi
    # Fewer pods per cluster: one DNS replica, no storage provisioner. The demo uses neither.
    own "$name" -n kube-system scale deploy/coredns --replicas=1 > /dev/null
    own "$name" -n local-path-storage scale deploy/local-path-provisioner --replicas=0 > /dev/null
    echo "kind cluster $name: created"
  fi
  kind get kubeconfig --name "$name" > "$WORK/kubeconfigs/$name" 2> /dev/null || return 1
  chmod 600 "$WORK/kubeconfigs/$name"
}

# One kubeconfig with a context per cluster, named like the kind cluster, and no default
# context: a kubectl command without --context fails instead of reaching some cluster.
write_kubeconfig() {
  local c files="" tmp
  for c in "${CLUSTERS[@]}"; do files+=${files:+:}$WORK/kubeconfigs/$c; done
  tmp=$(mktemp "$WORK/kubeconfig.XXXXXX") || return 1
  KUBECONFIG=$files kubectl config view --flatten > "$tmp" || return 1
  for c in "${CLUSTERS[@]}"; do
    kubectl --kubeconfig "$tmp" config rename-context "$(own "$c" config current-context)" "$c" > /dev/null || return 1
  done
  kubectl --kubeconfig "$tmp" config unset current-context > /dev/null
  chmod 600 "$tmp" && mv "$tmp" "$WORK/kubeconfig" || return 1
  # The CA certificate of each hub's API. OCM Fleet is given it when a hub is connected.
  for c in "${HUBS[@]}"; do
    own "$c" config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' \
      | base64 -d > "$WORK/$c-ca.crt" || return 1
  done
}

init_hub() { # NAME
  if ! own "$1" get clustermanager cluster-manager > /dev/null 2>&1; then
    clusteradm --kubeconfig "$WORK/kubeconfigs/$1" init --wait 2>&1 | grep -v -i token | tail -n 2
  fi
  if own "$1" -n open-cluster-management-hub wait deploy --all --for=condition=Available --timeout=5m > /dev/null 2>&1; then
    echo "hub $1: Open Cluster Management is installed"
  else
    echo "gave up: Open Cluster Management did not start on $1"
  fi
}

# register HUB NAME: the kind cluster HUB-NAME joins the hub under NAME (clusteradm join, then
# the hub's administrator approves and accepts), until the hub reports it as available.
register() {
  local hub=$1 name=$2 n=0 token api
  if [[ $(own "$hub" get managedcluster "$name" -o jsonpath='{.status.conditions[?(@.type=="ManagedClusterConditionAvailable")].status}' 2> /dev/null) != True ]]; then
    if ! own "$hub" get managedcluster "$name" > /dev/null 2>&1; then
      token=$(clusteradm --kubeconfig "$WORK/kubeconfigs/$hub" get token -o json | jq -r '."hub-token"')
      api=$(own "$hub" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
      clusteradm --kubeconfig "$WORK/kubeconfigs/$hub-$name" join --hub-token "$token" --hub-apiserver "$api" \
        --cluster-name "$name" --force-internal-endpoint-lookup > /dev/null 2>&1
    fi
    until own "$hub" wait "managedcluster/$name" --for=condition=ManagedClusterConditionAvailable --timeout=10s > /dev/null 2>&1; do
      ((++n > 40)) && { echo "gave up: $name did not register on $hub"; return 1; }
      own "$hub" get csr -o name | xargs -r kubectl --kubeconfig "$WORK/kubeconfigs/$hub" certificate approve > /dev/null 2>&1
      clusteradm --kubeconfig "$WORK/kubeconfigs/$hub" accept --clusters "$name" > /dev/null 2>&1
      sleep 5
    done
  fi
  # A hub's admission webhook may still be starting, after a restart for example.
  wait_for 300 "hub $hub does not accept a change of $name" \
    own "$hub" patch managedcluster "$name" --type merge -p "{\"spec\":{\"leaseDurationSeconds\":$LEASE_SECONDS}}" || return 1
  echo "$name (kind cluster $hub-$name) is available on $hub"
}

build_images() {
  local root
  root=$(cd "$DEMO_DIR/../.." && pwd)
  echo "building the images of OCM Fleet from $root (the first build takes a while; the output is in $WORK/build.log)"
  (
    cd "$root" || exit 1
    podman build -t localhost/ocm-fleet-server:latest . &&
      podman build -f Dockerfile.local -t localhost/ocm-fleet-server-local:latest . &&
      podman build --ulimit nofile=65536:65536 -f Dockerfile.web -t localhost/ocm-fleet-web:latest . &&
      podman build -f Dockerfile.aio \
        --build-arg SERVER_IMAGE=localhost/ocm-fleet-server-local:latest \
        --build-arg WEB_IMAGE=localhost/ocm-fleet-web:latest \
        -t localhost/ocm-fleet:latest . &&
      podman build -f Dockerfile.enrollment-agent -t localhost/ocm-fleet-enrollment-agent:latest .
  ) > "$WORK/build.log" 2>&1
}

fleet_up() { curl -sk --max-time 5 --resolve ocm-fleet.localhost:8085:127.0.0.1 -o /dev/null -f "$FLEET_URL/readyz"; }

start_fleet() {
  if podman container exists "$CONTAINER"; then
    [[ $(podman inspect -f '{{.State.Status}}' "$CONTAINER") == running ]] || podman start "$CONTAINER" > /dev/null || return 1
  else
    if ! podman image exists "$AIO_IMAGE" || ! podman image exists "$AGENT_IMAGE"; then
      if [[ $AIO_IMAGE == localhost/ocm-fleet:latest && $AGENT_IMAGE == localhost/ocm-fleet-enrollment-agent:latest ]]; then
        build_images || { echo "the build failed: see $WORK/build.log"; return 1; }
      else
        echo "there is no image $AIO_IMAGE or no image $AGENT_IMAGE"
        return 1
      fi
    fi
    # The container needs the podman socket to reach the clusters, and the name ocm-fleet on
    # the kind network so that the clusters can reach it. Its data stays inside it.
    podman run -d --name "$CONTAINER" --privileged \
      -p 127.0.0.1:8085:8085 -p 127.0.0.1:50051:50051 \
      -v /tmp:/tmp -v "$PODMAN_SOCKET:/var/run/docker.sock" \
      --network kind:alias=ocm-fleet \
      -e OCM_RM_ENROLLMENT_AGENT_IMAGE="$AGENT_IMAGE" \
      "$AIO_IMAGE" > /dev/null || return 1
  fi
  wait_for 240 "OCM Fleet is not ready (podman logs $CONTAINER)" fleet_up || return 1
  podman cp "$CONTAINER:/data/sandbox/pki/ca.crt" "$FLEET_CA" && chmod 644 "$FLEET_CA"
  echo "OCM Fleet answers at $FLEET_URL"
}

# The enrollment agent runs on the clusters, and a kind node cannot pull from this machine's
# image store: the image that OCM Fleet names in its add-on is loaded into every node.
load_agent_image() {
  local image tar=$WORK/enrollment-agent.tar c
  image=$(podman inspect "$CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^OCM_RM_ENROLLMENT_AGENT_IMAGE=//p')
  [[ -n $image ]] || { echo "$CONTAINER names no enrollment agent image"; return 1; }
  in_node() { podman exec "$1-control-plane" crictl images --no-trunc 2> /dev/null | grep -F "${image%:*}" | grep -qF " ${image##*:} "; }
  for c in "${CLUSTERS[@]}"; do
    in_node "$c" && continue
    [[ -f $tar ]] || podman save -o "$tar" "$image" > /dev/null 2>&1 || return 1
    kind load image-archive "$tar" --name "$c" > /dev/null 2>&1 || { echo "loading $image into $c failed"; rm -f "$tar"; return 1; }
    in_node "$c" || { echo "$image is not in $c after loading it"; rm -f "$tar"; return 1; }
  done
  rm -f "$tar"
  echo "the enrollment agent image $image is in every cluster"
}

case ${1:-up} in
  up)
    began=$(date -u +%s)
    for tool in kind podman kubectl clusteradm jq curl; do
      command -v "$tool" > /dev/null || { echo "$tool is not installed, or this terminal cannot see it"; exit 1; }
    done
    mkdir -p "$WORK/kubeconfigs" && chmod 700 "$WORK" || exit 1
    for c in "${CLUSTERS[@]}"; do create_cluster "$c" || exit 1; done
    write_kubeconfig || exit 1
    for c in "${CLUSTERS[@]}"; do
      wait_for 300 "the API of $c does not answer" kc "$c" get --raw /readyz || exit 1
    done
    # The two hubs at once; then each hub's clusters one after the other, the two hubs at once.
    for hub in "${HUBS[@]}"; do init_hub "$hub" > "$WORK/init-$hub.log" 2>&1 & done
    wait
    for hub in "${HUBS[@]}"; do tail -n 1 "$WORK/init-$hub.log"; done
    if grep -q '^gave up' "$WORK"/init-*.log; then exit 1; fi
    for hub in "${HUBS[@]}"; do
      (for name in "${MANAGED[@]}"; do register "$hub" "$name" || exit 1; done) > "$WORK/join-$hub.log" 2>&1 &
    done
    wait
    for hub in "${HUBS[@]}"; do cat "$WORK/join-$hub.log"; done
    if grep -q '^gave up' "$WORK"/join-*.log; then exit 1; fi
    start_fleet || exit 1
    load_agent_image || exit 1
    echo "waiting until every controller of Open Cluster Management is at work (a few minutes after a restart)"
    # At work means: heard from since this script began. See ocm_waiting.
    start=$SECONDS
    told=$SECONDS
    while waiting=$(ocm_waiting "$began"); [[ -n $waiting ]]; do
      if (( SECONDS - start > 600 )); then
        echo "$waiting"
        echo "gave up after ten minutes"
        exit 1
      fi
      if (( SECONDS - told >= 60 )); then
        echo "still waiting for $(wc -l <<< "$waiting") of them; one is: $(head -n 1 <<< "$waiting")"
        told=$SECONDS
      fi
      sleep 10
    done
    exec "$DEMO_DIR/scripts/reset.sh"
    ;;
  down)
    for c in "${CLUSTERS[@]}"; do
      if exists "$c"; then kind delete cluster --name "$c" 2>&1 | tail -n 1; fi
    done
    podman rm -f "$CONTAINER" > /dev/null 2>&1 && echo "removed the container $CONTAINER"
    rm -rf "$WORK"
    ;;
  *)
    sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
