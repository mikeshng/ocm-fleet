#!/usr/bin/env bash
# Puts the demo back at its starting state: hub1 connected to OCM Fleet, hub2 not, no cluster
# enrolled, nothing delivered. Run it before every demo; a minute or two.
#
# It is safe at any time, also in the middle of a demo that went wrong. It removes only what
# the demo makes: the enrollment add-on, demo labels, and the read-only account of hub2 on
# the hubs; OCM Fleet's access and the demo's namespaces on the clusters; enrollments and
# deployments in OCM Fleet. It starts the agent of a cluster again when the demo stopped it.
# It needs kubectl, jq, and curl.

. "$(dirname "$(readlink -f "$0")")/lib.sh"

step() { echo; echo "== $*"; }
# kcd CONTEXT ARGS...: a kubectl delete of whatever a label selects. kubectl says "No
# resources found" when there is nothing to remove, and that is not worth a line.
kcd() { kcw "$@" 2>&1 | grep -v '^No resources found' || true; }

[[ -r $KUBECONFIG && -r $FLEET_CA && -r $WORK/$START_HUB-ca.crt ]] || { echo "run scripts/setup.sh first: it creates the environment and $WORK" >&2; exit 1; }
for hub in "${HUBS[@]}"; do
  wait_for 120 "the API of $hub does not answer (after a restart of the machine, run scripts/setup.sh)" kc "$hub" get --raw /readyz || exit 1
done
wait_for 120 "OCM Fleet does not answer (after a restart of the machine, run scripts/setup.sh)" fleet_json GET "$OCM_API/hubs" || exit 1

step "The agents of Open Cluster Management on the managed clusters"
for hub in "${HUBS[@]}"; do
  for mc in "${MANAGED[@]}"; do
    if klusterlet_stopped "$hub-$mc"; then
      echo "the agent on $hub-$mc is stopped: starting it"
      klusterlet_start "$hub-$mc" || { echo "the agent on $hub-$mc did not start" >&2; exit 1; }
    fi
    wait_for 120 "hub $hub does not accept a change of $mc (after a restart of the machine, run scripts/setup.sh)" \
      kc "$hub" patch managedcluster "$mc" --type merge -p "{\"spec\":{\"leaseDurationSeconds\":$LEASE_SECONDS}}" || exit 1
  done
done
echo "every managed cluster has its agent running"

step "OCM Fleet: enrollment secrets, enrollments, deployments"
fleet_clean || echo "(continuing: the end says what is left)" >&2

step "The hubs, as their administrators: the add-on and the demo's labels"
for hub in "${HUBS[@]}"; do
  # The add-on itself goes first: while it follows a Placement, the hub's add-on manager
  # would enable it again for every cluster it is disabled for.
  kcw "$hub" delete clustermanagementaddon "$ADDON" --ignore-not-found --timeout=60s
  kc "$hub" get managedclusteraddons -A -o json 2> /dev/null \
    | jq -r --arg a "$ADDON" '.items[] | select(.metadata.name == $a) | .metadata.namespace' \
    | while read -r ns; do kcw "$hub" -n "$ns" delete managedclusteraddon "$ADDON" --ignore-not-found --timeout=120s; done
  kcw "$hub" delete addontemplate "$ADDON" --ignore-not-found --timeout=60s
  kcw "$hub" delete namespace ocm-fleet-enrollment --ignore-not-found --timeout=120s
  kcd "$hub" delete manifestworks -A -l "$DEMO_LABEL" --ignore-not-found --timeout=120s
  for label in env team; do
    kc "$hub" get managedclusters -o json 2> /dev/null \
      | jq -r --arg l "$label" '.items[] | select(.metadata.labels[$l] != null) | .metadata.name' \
      | while read -r mc; do kc "$hub" label managedcluster "$mc" "$label-"; done
  done
done

step "The clusters, as their administrators: OCM Fleet's access and the demo's namespaces"
for c in "${CLUSTERS[@]}"; do
  kcw "$c" delete namespace ocm-fleet-system --ignore-not-found --timeout=150s
  kcd "$c" delete clusterrolebinding,clusterrole -l app.kubernetes.io/part-of=ocm-fleet --ignore-not-found --timeout=60s
  kcd "$c" delete namespaces -l "$DEMO_LABEL" --ignore-not-found --timeout=150s
done

step "The hub connections"
for hub in "${HUBS[@]}"; do
  if fleet_json GET "$OCM_API/hubs/$hub" > /dev/null 2>&1; then
    fleet_json POST "$OCM_API/hubs/$hub:disconnect" '{}' > /dev/null || exit 1
  fi
  if [[ $hub == "$START_HUB" ]]; then
    # Connected anew every time, so that the hub's record carries nothing of an earlier demo.
    connect_hub "$hub" || exit 1
  else
    echo "$hub is not connected: connecting it is a step of the demo"
    kcw "$hub" delete namespace "$READER_NAMESPACE" --ignore-not-found --timeout=120s
    kcd "$hub" delete clusterrolebinding,clusterrole -l app.kubernetes.io/part-of=ocm-fleet-hub-connection --ignore-not-found --timeout=60s
  fi
done

# The add-on manifests that the steps of the demo wrote. Their enrollment secrets are revoked.
rm -f "$WORK"/*-enrollment-addon.yaml

step "Check"
# The lists settle a moment after the last deletion, and an agent that was started again
# needs up to a minute before its hub hears from it.
problems=$(not_ready)
start=$SECONDS
while [[ -n $problems ]] && (( SECONDS - start < 240 )); do
  sleep 5
  problems=$(not_ready)
done
if [[ -n $problems ]]; then
  echo "$problems"
  echo
  echo "Not ready. Run scripts/reset.sh again; after a restart of the machine, scripts/setup.sh."
  exit 1
fi
kc hub1 get managedclusters
kc hub2 get managedclusters
echo
echo "Ready: hub1 is connected to OCM Fleet, hub2 is not, and no cluster is enrolled."
