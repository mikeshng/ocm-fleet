# Shared by setup.sh and reset.sh. Not meant to be run by itself.
#
# The environment of the demo: two Open Cluster Management hubs, hub1 and hub2, each with two
# managed clusters that the hub calls cluster1 and cluster2, and one OCM Fleet. All six
# clusters are kind clusters. The kind cluster of a managed cluster, and its kubectl context,
# carry the hub's name in front: hub1-cluster1 is cluster1 of hub1.

DEMO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=$DEMO_DIR/.work
HUBS=(hub1 hub2)
MANAGED=(cluster1 cluster2)
CLUSTERS=(hub1 hub2 hub1-cluster1 hub1-cluster2 hub2-cluster1 hub2-cluster2)
# The demo starts with this hub connected; connecting the other one is a step of the demo.
START_HUB=hub1
CONTAINER=${CONTAINER:-ocm-fleet-aio}
FLEET_URL=https://ocm-fleet.localhost:8085
# A managed cluster reports to its hub this often. Open Cluster Management's default is 60.
# A hub calls a cluster Unknown when five reports in a row are missing: about one minute with
# 10, five minutes with the default. The demo stops a cluster's agent and waits for that.
LEASE_SECONDS=10
ADDON=ocm-fleet-enrollment
DEMO_LABEL=app.kubernetes.io/part-of=fleet-demo
OCM_API=/apis/ocm.open-cluster-management.io/v1alpha
K8S_API=/apis/kubernetes.open-cluster-management.io/v1alpha
CORE_API=/apis/open-cluster-management.io/v1alpha

export KUBECONFIG=$WORK/kubeconfig
# The certificate of the sandbox's own CA, which signs OCM Fleet's certificate. setup.sh
# copies it out of the container.
FLEET_CA=$WORK/ca.crt
# The sandbox's operator: see deploy/aio/README.md.
FLEET_USER=ops@ocm-fleet.local
FLEET_PASSWORD=ocm-fleet-ops
# The read-only account of manifests/hub-reader.yaml.
READER_NAMESPACE=ocm-fleet-hub-connection
READER_ACCOUNT=ocm-fleet-hub-reader

hub_api() { echo "https://$1-control-plane:6443"; }
hub_display() {
  case $1 in
    hub1) echo "Team A" ;;
    hub2) echo "Team B" ;;
  esac
}

# kc CONTEXT ARGS...: kubectl as that cluster's administrator, for reads.
kc() { local c=$1; shift; timeout 30 kubectl --context "$c" --request-timeout=20s "$@"; }
# kcw CONTEXT ARGS...: the same for commands that wait (delete, wait).
kcw() { local c=$1; shift; timeout 200 kubectl --context "$c" "$@"; }

# wait_for SECONDS WHAT COMMAND...: runs the command every two seconds until it succeeds.
wait_for() {
  local limit=$1 what=$2 start=$SECONDS; shift 2
  until "$@" > /dev/null 2>&1; do
    if (( SECONDS - start >= limit )); then
      [[ -n $what ]] && echo "gave up after $limit s: $what" >&2
      return 1
    fi
    sleep 2
  done
}

# fleet_curl ARGS...: curl to OCM Fleet. Its name is resolved to this machine, and its
# certificate is checked against the sandbox's CA.
fleet_curl() {
  curl -s --max-time 60 --cacert "$FLEET_CA" --resolve ocm-fleet.localhost:8085:127.0.0.1 "$@"
}

# fleet_login: signs in as the sandbox's operator and keeps the token in FLEET_TOKEN.
fleet_login() {
  FLEET_TOKEN=$(fleet_curl "$FLEET_URL/idp/token" -d grant_type=password -d client_id=fleetctl \
    --data-urlencode "username=$FLEET_USER" --data-urlencode "password=$FLEET_PASSWORD" \
    --data-urlencode "scope=openid profile email groups audience:server:client_id:resource-manager" \
    | jq -r '.id_token // empty' 2> /dev/null)
  [[ -n $FLEET_TOKEN ]]
}

# fleet_json METHOD PATH [JSON]: one call to OCM Fleet's API as the sandbox's operator, the
# answer on stdout. A refusal fails, with the server's words on stderr; the status is 4 when
# the server says "not found".
fleet_json() {
  local method=$1 path=$2 body=${3:-} data=() answer status try
  [[ -n $body ]] && data=(-H "Content-Type: application/json" --data-binary @-)
  for try in 1 2; do
    if [[ -z ${FLEET_TOKEN:-} ]] && ! fleet_login; then
      echo "signing in to OCM Fleet at $FLEET_URL failed" >&2
      return 1
    fi
    answer=$(fleet_curl -X "$method" -H "Authorization: Bearer $FLEET_TOKEN" "${data[@]}" \
      -w '\n%{http_code}' "$FLEET_URL$path" <<< "$body")
    status=${answer##*$'\n'}
    answer=${answer%$'\n'*}
    # The first call after the identity provider changed its signing key is refused, and it
    # makes OCM Fleet fetch the new key: a fresh sign-in and one more try go through.
    [[ $status == 401 && $try == 1 ]] || break
    FLEET_TOKEN=
  done
  if [[ $status == 2?? ]]; then
    [[ -z $answer ]] || printf '%s\n' "$answer"
    return 0
  fi
  echo "OCM Fleet answered $status to $method $path: ${answer:0:300}" >&2
  [[ $status != 404 ]] || return 4
  return 1
}

# fleet_gone METHOD PATH [JSON]: the same call where "not found" is as good as done.
fleet_gone() {
  local words status
  words=$(fleet_json "$@" 2>&1 > /dev/null)
  status=$?
  [[ $status != 0 && $status != 4 ]] || return 0
  echo "$words" >&2
  return 1
}

# hub_ready HUB: OCM Fleet is connected to the hub and has read its clusters.
hub_ready() {
  [[ $(fleet_json GET "$OCM_API/hubs/$1" 2> /dev/null | jq -r '.conditions.Ready.status // ""' 2> /dev/null) == True ]]
}

# connect_hub HUB: what the demo's step 2 does by hand. The hub's administrator applies the
# read-only account; OCM Fleet is given the hub's address, its CA certificate, and a token of
# that account. OCM Fleet renews the token itself; a long first one survives a machine that
# was asleep for a while.
connect_hub() {
  local hub=$1 token body
  kcw "$hub" apply -f "$DEMO_DIR/manifests/hub-reader.yaml" > /dev/null || return 1
  token=$(kc "$hub" -n "$READER_NAMESPACE" create token "$READER_ACCOUNT" --duration=720h) || return 1
  body=$(jq -n --arg id "$hub" --arg api "$(hub_api "$hub")" --arg name "$(hub_display "$hub")" \
    --arg token "$token" --rawfile ca "$WORK/$hub-ca.crt" \
    '{hubId: $id, apiServer: $api, displayName: $name, token: $token, caCert: $ca}') || return 1
  fleet_json POST "$OCM_API/hubs:connect" "$body" > /dev/null || return 1
  wait_for 120 "OCM Fleet has not read the clusters of $hub" hub_ready "$hub" || return 1
  echo "$hub is connected to OCM Fleet, with a read-only account"
}

# demo_deployments: one line "NAME PAUSE-REASON" for every deployment that is not the
# sandbox's own.
demo_deployments() {
  local list
  list=$(fleet_json GET "/v1alpha/deployments?pageSize=200") || return 1
  jq -r '.deployments[]? | (.name | sub("^deployments/"; "")) as $n
    | select($n != "idp-trust-default" and $n != "fleet-demo") | "\($n | @uri) \(.pauseReason // "")"' <<< "$list"
}

# fleet_clean: removes what a demo left in OCM Fleet: enrollment secrets, enrollments, and
# deployments. It touches no hub and no cluster. It works from any state, also a broken one,
# so it does not rely on OCM Fleet removing what it delivered: the enrolled clusters are
# withdrawn first and keep what was delivered to them (reset.sh deletes that as each
# cluster's administrator), and the deployments then have nothing left to remove.
fleet_clean() {
  local list clusters left id names start
  list=$(fleet_json GET "$K8S_API/enrollmentBundles?pageSize=500") || return 1
  for id in $(jq -r '.enrollmentBundles[]? | (.observation.id // (.name | sub("^.*/"; "")))' <<< "$list"); do
    fleet_gone DELETE "$K8S_API/enrollmentBundles/$id" || return 1
    echo "revoked enrollment secret $id"
  done
  clusters=$(fleet_json GET "$K8S_API/clusters?pageSize=500") || return 1
  for id in $(jq -r '.clusters[]? | select((.observation.state // "") != "withdrawn") | .name | sub("^clusters/"; "")' <<< "$clusters"); do
    fleet_gone POST "$K8S_API/clusters/$id:withdraw" '{"reason":"demo reset","leaveDeployments":true}' || return 1
    echo "withdrew $id"
  done
  left=$(demo_deployments) || return 1
  names=$(cut -d' ' -f1 <<< "$left" | paste -sd' ')
  for id in $names; do
    fleet_gone DELETE "/v1alpha/deployments/$id" || return 1
  done
  start=$SECONDS
  while [[ -n $left ]]; do
    if (( SECONDS - start >= 420 )); then
      echo "gave up after 420 s: these deployments are still being deleted: $(cut -d' ' -f1 <<< "$left" | paste -sd' ')" >&2
      return 1
    fi
    # A paused deployment waits for someone to resume it, also when it is being deleted.
    for id in $(awk '$2 != "" { print $1 }' <<< "$left"); do
      fleet_json POST "/v1alpha/deployments/$id:resume" '{}' > /dev/null 2>&1
    done
    sleep 2
    left=$(demo_deployments) || return 1
  done
  [[ -z $names ]] || echo "deleted the deployments: $names"
  for id in $(jq -r '.clusters[]?.name | sub("^clusters/"; "")' <<< "$clusters"); do
    fleet_gone DELETE "$K8S_API/clusters/$id" || return 1
    echo "removed the record of $id"
  done
  no_enrolled_target() {
    local targets
    targets=$(fleet_json GET "$CORE_API/targets") || return 1
    [[ $(jq -r '[.targets[]? | select(.origin == "enrolled")] | length' <<< "$targets") == 0 ]]
  }
  wait_for 420 "targets of enrolled clusters are still there" no_enrolled_target || return 1
  echo "OCM Fleet's side is clean"
}

# klusterlet_stopped CONTEXT: whether the cluster's agent of Open Cluster Management, or the
# operator that keeps it running, is scaled to zero. The demo does that to one cluster.
klusterlet_stopped() {
  local zero
  [[ $(kc "$1" -n open-cluster-management get deployment klusterlet -o jsonpath='{.spec.replicas}' 2> /dev/null) == 0 ]] && return 0
  zero=$(kc "$1" -n open-cluster-management-agent get deployments -o json 2> /dev/null | jq '[.items[] | select(.spec.replicas == 0)] | length' 2> /dev/null)
  [[ ${zero:-0} != 0 ]]
}
klusterlet_start() {
  kc "$1" -n open-cluster-management scale deployment klusterlet --replicas=1 > /dev/null || return 1
  kc "$1" -n open-cluster-management-agent scale deployment --all --replicas=1 > /dev/null || return 1
  kcw "$1" -n open-cluster-management wait deployment klusterlet --for=condition=Available --timeout=180s > /dev/null || return 1
  kcw "$1" -n open-cluster-management-agent wait deployment --all --for=condition=Available --timeout=180s > /dev/null
}

# ocm_waiting [SINCE]: one line for every part of Open Cluster Management that is not at
# work, and nothing when all of it is. It only reads.
#
# After a restart the pods run and the hub's lists look right within half a minute, but every
# controller first waits for its old leader lock to run out, about two and a half minutes,
# and does nothing until then. "At work" is therefore read from the leases the controllers
# keep themselves: a leader lock is renewed every 26 seconds, and a managed cluster reports
# to its hub every LEASE_SECONDS. A lock older than 90 seconds, or a report older than six
# periods, counts as not at work. Right after a short outage a lock still looks fresh while
# nobody holds it: with SINCE (seconds since the epoch) a lease also has to be renewed after
# that moment.
ocm_waiting() {
  local c mc now leases wanted since=${1:-0}
  now=$(date -u +%s)
  for c in "${CLUSTERS[@]}"; do
    if [[ " ${HUBS[*]} " == *" $c "* ]]; then
      wanted="open-cluster-management-hub registration-controller-lock 90 the registration controller of hub $c
open-cluster-management-hub manager-lock 90 the add-on manager of hub $c"
      for mc in "${MANAGED[@]}"; do
        wanted+="
$mc managed-cluster-lease $((LEASE_SECONDS * 6)) the reports of $mc to hub $c"
      done
    else
      wanted="open-cluster-management-agent registration-agent-lock 90 the registration agent on $c
open-cluster-management-agent work-agent-lock 90 the work agent on $c"
    fi
    if ! leases=$(kc "$c" get leases -A -o json 2> /dev/null); then
      echo "$c: its API does not answer"
      continue
    fi
    jq -r --arg wanted "$wanted" --argjson now "$now" --argjson since "$since" '
      (reduce .items[] as $i ({}; .["\($i.metadata.namespace) \($i.metadata.name)"] = ($i.spec.renewTime // ""))) as $have
      | $wanted | split("\n")[] | split(" ")
      | {key: "\(.[0]) \(.[1])", limit: (.[2] | tonumber), who: (.[3:] | join(" "))}
      | ($have[.key] // "") as $t
      | if $t == "" then "\(.who): no sign of life yet"
        else ($t | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) as $e
          | if ($now - $e) > .limit then "\(.who): last sign of life \($now - $e) s ago"
            elif $e < $since then "\(.who): no sign of life since the start"
            else empty end
        end' <<< "$leases"
  done
}

# not_ready: one line for everything that differs from the state the demo starts from, and
# nothing when the environment is in that state. It only reads.
not_ready() {
  local hubs hub mc good targets records bundles deployments
  if ! hubs=$(fleet_json GET "$OCM_API/hubs?pageSize=100" 2> /dev/null); then
    echo "OCM Fleet does not answer at $FLEET_URL"
    return
  fi
  [[ $(jq -r '[.hubs[]? | (.observation.id // (.name | sub("^hubs/"; "")))] | sort | join(" ")' <<< "$hubs") == "$START_HUB" ]] \
    || echo "the hubs connected to OCM Fleet are not $START_HUB alone"
  [[ $(jq -r --arg id "$START_HUB" '[.hubs[]? | select((.observation.id // "") == $id)][0] | "\(.conditions.Ready.status // "?") \(.observation.clusterCount // 0)"' <<< "$hubs") == "True ${#MANAGED[@]}" ]] \
    || echo "$START_HUB is not connected with its ${#MANAGED[@]} clusters"
  for hub in "${HUBS[@]}"; do
    good=$(kc "$hub" get managedclusters -o json 2> /dev/null | jq -r '[.items[] | select(.spec.hubAcceptsClient == true and ([.status.conditions[]? | select(.type == "ManagedClusterConditionAvailable" and .status == "True")] | length == 1)) | .metadata.name] | sort | join(" ")')
    [[ $good == "${MANAGED[*]}" ]] || echo "$hub reports as available: ${good:-no cluster} (expected ${MANAGED[*]})"
    for mc in "${MANAGED[@]}"; do
      klusterlet_stopped "$hub-$mc" && echo "the agent of Open Cluster Management on $hub-$mc is stopped"
    done
  done
  ocm_waiting
  targets=$(fleet_json GET "$CORE_API/targets" 2> /dev/null | jq -r '[.targets[]? | select(.id != "kind-local") | .id] | join(" ")')
  records=$(fleet_json GET "$K8S_API/clusters?pageSize=500" 2> /dev/null | jq -r '[.clusters[]?.name | sub("^clusters/"; "")] | join(" ")')
  bundles=$(fleet_json GET "$K8S_API/enrollmentBundles?pageSize=500" 2> /dev/null | jq -r '[.enrollmentBundles[]?.name] | length')
  [[ -z $targets && -z $records && $bundles == 0 ]] \
    || echo "still enrolled: targets [${targets:-none}], cluster records [${records:-none}], enrollment secrets ${bundles:-?}"
  deployments=$(fleet_json GET "/v1alpha/deployments?pageSize=200" 2> /dev/null | jq -r '[.deployments[]?.name | sub("^deployments/"; "") | select(. != "idp-trust-default")] | join(" ")')
  [[ -z $deployments ]] || echo "deployments left: $deployments"
}
