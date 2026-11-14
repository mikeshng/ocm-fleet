import { extractClusterId } from "./cluster.js";

/** Record type of a cluster that the built-in provider created. */
export const KIND_CLUSTER_TYPE = "kind.open-cluster-management.io/Cluster";

/** Record type of a cluster that was enrolled. */
export const ENROLLED_CLUSTER_TYPE =
  "kubernetes.open-cluster-management.io/Cluster";

/** Record type of a cluster as a connected hub reports it. */
export const OBSERVED_CLUSTER_TYPE =
  "ocm.open-cluster-management.io/ManagedCluster";

/** Record type of a connected hub. */
export const HUB_TYPE = "ocm.open-cluster-management.io/Hub";

/** Record type of an object indexed from inside a cluster. */
export const KUBERNETES_OBJECT_TYPE =
  "kubernetes.open-cluster-management.io/Object";

/**
 * How a cluster came to be known: created by a provider here, enrolled,
 * or only observed through a connected hub.
 */
export type FleetClusterOrigin = "created" | "enrolled" | "observed";

/** The parts of a query result that the helpers here read. */
export interface ClusterRecord {
  resourceType: string;
  resource: { name: string; observation?: unknown; conditions?: unknown };
}

/** One cluster, whatever its origin, with the hub's record of it if any. */
export interface FleetClusterRow<T extends ClusterRecord = ClusterRecord> {
  /**
   * Identifier in console routes: the id of `clusters/<id>`, or
   * `<hub>.<name>` for a cluster that only a hub knows.
   */
  id: string;
  origin: FleetClusterOrigin;
  /** The cluster's own record, or the hub's record when there is no other. */
  result: T;
  /** The hub's record of the cluster, when a connected hub has one. */
  observed?: T;
  /** Id of the hub that reports the cluster, when one does. */
  hub?: string;
  /** Whether the resources inside the cluster are indexed. */
  indexed: boolean;
}

export type FleetStatusColor = "blue" | "green" | "orange" | "red" | "grey";

export interface FleetClusterStatus {
  text: string;
  color: FleetStatusColor;
  healthy: boolean;
  attention: boolean;
}

function text(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function observationOf(record: ClusterRecord): Record<string, unknown> {
  const observation = record.resource.observation;
  return observation && typeof observation === "object"
    ? (observation as Record<string, unknown>)
    : {};
}

function conditionStatus(record: ClusterRecord, type: string): string {
  const conditions = record.resource.conditions;
  if (!conditions || typeof conditions !== "object") return "";
  const condition = (conditions as Record<string, { status?: unknown }>)[type];
  return text(condition?.status);
}

function celString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/** CEL filter that matches the records of clusters of every origin. */
export function fleetClusterTypeFilter(): string {
  return [KIND_CLUSTER_TYPE, ENROLLED_CLUSTER_TYPE, OBSERVED_CLUSTER_TYPE]
    .map((type) => `resourceType == "${type}"`)
    .join(" || ");
}

/** CEL filter for a cluster's own record, created or enrolled. */
export function clusterRecordFilter(clusterId: string): string {
  const types = `resourceType == "${KIND_CLUSTER_TYPE}" || resourceType == "${ENROLLED_CLUSTER_TYPE}"`;
  return `(${types}) && resource.name == "clusters/${celString(clusterId)}"`;
}

/**
 * CEL filter for the hub records that belong to a console cluster id: the
 * record linked to `clusters/<id>`, and for an id of the form
 * `<hub>.<name>` the record of that name on that hub.
 */
export function observedRecordFilter(clusterId: string): string {
  const clauses = [
    `resource.observation.enrollment.cluster == "clusters/${celString(clusterId)}"`,
  ];
  const dot = clusterId.indexOf(".");
  if (dot > 0 && dot < clusterId.length - 1) {
    const hub = celString(clusterId.slice(0, dot));
    const name = celString(clusterId.slice(dot + 1));
    clauses.push(`resource.name == "hubs/${hub}/managedClusters/${name}"`);
  }
  return `resourceType == "${OBSERVED_CLUSTER_TYPE}" && (${clauses.join(" || ")})`;
}

/** Id of the cluster record that a hub's record is linked to, or "". */
export function linkedClusterId(observed: ClusterRecord): string {
  const enrollment = observationOf(observed).enrollment;
  if (!enrollment || typeof enrollment !== "object") return "";
  const cluster = text((enrollment as Record<string, unknown>).cluster);
  return cluster ? extractClusterId(cluster) : "";
}

/** Console id of a cluster that only a hub knows: `<hub>.<name>`. */
export function observedClusterId(observed: ClusterRecord): string {
  const observation = observationOf(observed);
  return `${text(observation.hub)}.${text(observation.name)}`;
}

/** Console id that a hub's record of a cluster leads to. */
export function observedClusterRouteId(observed: ClusterRecord): string {
  return linkedClusterId(observed) || observedClusterId(observed);
}

/**
 * Turns the records of clusters of every origin into one row per cluster.
 * A hub's record folds into the row of the cluster it is linked to, so a
 * cluster that is both known here and reported by a hub appears once.
 */
export function mergeFleetClusters<T extends ClusterRecord>(
  results: T[],
): FleetClusterRow<T>[] {
  const rows = new Map<string, FleetClusterRow<T>>();

  for (const result of results) {
    if (result.resourceType !== KIND_CLUSTER_TYPE) continue;
    const id = extractClusterId(result.resource.name);
    rows.set(id, { id, origin: "created", result, indexed: true });
  }

  for (const result of results) {
    if (result.resourceType !== ENROLLED_CLUSTER_TYPE) continue;
    const id = extractClusterId(result.resource.name);
    if (rows.has(id)) continue;
    rows.set(id, {
      id,
      origin: "enrolled",
      result,
      indexed: text(observationOf(result).state) !== "withdrawn",
    });
  }

  for (const result of results) {
    if (result.resourceType !== OBSERVED_CLUSTER_TYPE) continue;
    const hub = text(observationOf(result).hub);
    const id = observedClusterId(result);
    const known = rows.get(linkedClusterId(result)) ?? rows.get(id);
    if (known) {
      if (!known.observed) {
        known.observed = result;
        known.hub = hub;
      }
      continue;
    }
    rows.set(id, {
      id,
      origin: "observed",
      result,
      observed: result,
      hub,
      indexed: false,
    });
  }

  return [...rows.values()].sort((a, b) => a.id.localeCompare(b.id));
}

/**
 * Drops the records of clusters that a hub's record in the same list is
 * linked to, so that a search shows such a cluster once, with its hub.
 */
export function foldLinkedClusterResults<T extends ClusterRecord>(
  results: T[],
): T[] {
  const linked = new Set<string>();
  for (const result of results) {
    if (result.resourceType !== OBSERVED_CLUSTER_TYPE) continue;
    const id = linkedClusterId(result);
    if (id) linked.add(id);
  }
  if (linked.size === 0) return results;
  return results.filter(
    (result) =>
      !(
        (result.resourceType === KIND_CLUSTER_TYPE ||
          result.resourceType === ENROLLED_CLUSTER_TYPE) &&
        linked.has(extractClusterId(result.resource.name))
      ),
  );
}

/** State of an enrolled cluster, as shown in lists. */
export function enrolledClusterStatus(
  record: ClusterRecord,
): FleetClusterStatus {
  const state = text(observationOf(record).state);
  if (state === "withdrawn") {
    return {
      text: "Withdrawn",
      color: "grey",
      healthy: false,
      attention: false,
    };
  }
  if (state === "needs-enrollment") {
    return {
      text: "Needs enrollment",
      color: "orange",
      healthy: false,
      attention: true,
    };
  }
  const ready = conditionStatus(record, "Ready");
  if (ready === "True") {
    return { text: "Ready", color: "green", healthy: true, attention: false };
  }
  if (ready === "False") {
    return { text: "Not ready", color: "red", healthy: false, attention: true };
  }
  return { text: "Unknown", color: "grey", healthy: false, attention: false };
}

/** State of a cluster as its hub reports it, as shown in lists. */
export function observedClusterStatus(
  record: ClusterRecord,
): FleetClusterStatus {
  const observation = observationOf(record);
  if (conditionStatus(record, "Stale") === "True") {
    return { text: "Stale", color: "orange", healthy: false, attention: true };
  }
  if (observation.accepted !== true) {
    return {
      text: "Not accepted",
      color: "grey",
      healthy: false,
      attention: false,
    };
  }
  if (observation.available === "True") {
    return {
      text: "Available",
      color: "green",
      healthy: true,
      attention: false,
    };
  }
  if (observation.available === "False") {
    return {
      text: "Unavailable",
      color: "red",
      healthy: false,
      attention: true,
    };
  }
  return { text: "Unknown", color: "grey", healthy: false, attention: false };
}
