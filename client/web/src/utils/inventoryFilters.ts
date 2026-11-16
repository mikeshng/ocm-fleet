import {
  ENROLLED_CLUSTER_TYPE,
  KIND_CLUSTER_TYPE,
  KUBERNETES_OBJECT_TYPE,
  OBSERVED_CLUSTER_TYPE,
} from "@ocm-fleet/common";

import { escapeCelString } from "./celEscape";

/**
 * inventorySearchFilters builds the CEL filters for the four inventory
 * searches of one term. The term comes from a search box, so it is escaped
 * before it goes inside a CEL string literal.
 *
 * Enrolled clusters and the clusters of connected hubs are found by the
 * start of their name, or by a whole term: a label (key=value), a cluster
 * set, or a hub.
 */
export function inventorySearchFilters(term: string): {
  clusters: string;
  enrolled: string;
  observed: string;
  kubernetes: string;
} {
  const escaped = escapeCelString(term);
  return {
    clusters: [
      `(resourceType == "${KIND_CLUSTER_TYPE}")`,
      `&& (resource.name.startsWith("${escaped}") || resource.name.startsWith("clusters/${escaped}"))`,
    ].join(" "),
    enrolled: [
      `(resourceType == "${ENROLLED_CLUSTER_TYPE}")`,
      `&& (resource.name.startsWith("clusters/${escaped}") || "${escaped}" in resource.observation.searchTerms)`,
    ].join(" "),
    observed: [
      `(resourceType == "${OBSERVED_CLUSTER_TYPE}")`,
      `&& (resource.observation.name.startsWith("${escaped}") || "${escaped}" in resource.observation.searchTerms)`,
    ].join(" "),
    kubernetes: [
      `resourceType == "${KUBERNETES_OBJECT_TYPE}"`,
      `&& (resource.observation.kind == "Pod" || resource.observation.kind == "Namespace" || resource.observation.kind == "Node")`,
      `&& resource.observation.metadata.name.startsWith("${escaped}")`,
    ].join(" "),
  };
}
