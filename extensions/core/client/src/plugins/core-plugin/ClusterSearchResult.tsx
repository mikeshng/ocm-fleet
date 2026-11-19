import type { InventoryResource, SearchResultRender } from "@ocm-fleet/common";
import { enrolledClusterStatus, extractClusterId } from "@ocm-fleet/common";

export function resolveEnrolledCluster(
  resource: InventoryResource,
): SearchResultRender {
  return {
    scope: "core-plugin",
    module: "ClustersModule",
    to: extractClusterId(resource.resource.name),
    description: `Enrolled · ${enrolledClusterStatus(resource).text}`,
  };
}

export { CubesIcon as EnrolledClusterIcon } from "@patternfly/react-icons";
