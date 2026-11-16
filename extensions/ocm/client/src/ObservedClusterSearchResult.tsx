import type { InventoryResource, SearchResultRender } from "@ocm-fleet/common";
import {
  linkedClusterId,
  observedClusterId,
  observedClusterRouteId,
  observedClusterStatus,
} from "@ocm-fleet/common";

const ENROLLMENT_TEXT: Record<string, string> = {
  observed: "observed",
  enrolled: "enrolled",
  created: "created here",
  withdrawn: "withdrawn",
};

/**
 * A cluster that a connected hub reports, as a search result. Its title
 * is the hub and the cluster's name on it, `<hub>.<name>`: two hubs can
 * each have a cluster of one name, and the results must tell them apart
 * at a glance. The line below says whether the cluster is only observed
 * or also enrolled, and under which id when that differs. It leads to
 * the cluster's page.
 */
export function resolveObservedCluster(
  resource: InventoryResource,
): SearchResultRender {
  const observation = (resource.resource.observation ?? {}) as {
    hub?: string;
    name?: string;
    enrollment?: { state?: string };
  };
  const state = observation.enrollment?.state ?? "observed";
  const title = observedClusterId(resource);
  const linked = linkedClusterId(resource);
  const here =
    linked && linked !== title
      ? `${ENROLLMENT_TEXT[state] ?? state} as ${linked}`
      : (ENROLLMENT_TEXT[state] ?? state);
  return {
    scope: "core-plugin",
    module: "ClustersModule",
    to: observedClusterRouteId(resource),
    title,
    description: `Hub ${observation.hub ?? "unknown"} · ${here} · ${observedClusterStatus(resource).text}`,
  };
}

export { default as ObservedClusterIcon } from "./HubIcon";
