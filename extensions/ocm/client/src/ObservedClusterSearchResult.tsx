import type { InventoryResource, SearchResultRender } from "@ocm-fleet/common";
import {
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
 * A cluster that a connected hub reports, as a search result: its name on
 * the hub, the hub, and whether it is only observed or also enrolled. It
 * leads to the cluster's page.
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
  return {
    scope: "core-plugin",
    module: "ClustersModule",
    to: observedClusterRouteId(resource),
    title: observation.name,
    description: `Hub ${observation.hub ?? "unknown"} · ${ENROLLMENT_TEXT[state] ?? state} · ${observedClusterStatus(resource).text}`,
  };
}

export { default as ObservedClusterIcon } from "./HubIcon";
