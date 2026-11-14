import type {
  ClusterResource,
  FleetClusterRow,
  FleetClusterStatus,
  ResourceResult,
} from "@ocm-fleet/common";
import {
  enrolledClusterStatus,
  observedClusterStatus,
} from "@ocm-fleet/common";

/** A cluster of any origin, as the cluster list and detail page hold it. */
export type ClusterRow = FleetClusterRow<ResourceResult<ClusterResource>>;

export interface ClusterRowStatus extends FleetClusterStatus {
  paused: boolean;
  transient: boolean;
}

const STATE_LABELS: Record<
  string,
  { text: string; color: "blue" | "green" | "orange" | "red" | "grey" }
> = {
  CREATING: { text: "Creating", color: "blue" },
  ACTIVE: { text: "Active", color: "green" },
  DELETING: { text: "Deleting", color: "orange" },
  FAILED: { text: "Failed", color: "red" },
  PAUSED_AUTH: { text: "Paused", color: "orange" },
  RUNNING: { text: "Running", color: "green" },
  PROVISIONING: { text: "Provisioning", color: "blue" },
  ERROR: { text: "Error", color: "red" },
  DEGRADED: { text: "Degraded", color: "orange" },
};

const DEFAULT_STATE_LABEL = { text: "Unknown", color: "grey" } as const;

const HEALTHY_STATES = new Set(["ACTIVE", "RUNNING"]);
const FAILURE_STATES = new Set(["ERROR", "DEGRADED", "FAILED"]);
const TRANSIENT_STATES = new Set(["CREATING", "DELETING", "PROVISIONING"]);

export function stateLabel(
  state: string | undefined,
  underlyingState?: string,
) {
  if (!state) return DEFAULT_STATE_LABEL;
  const label = STATE_LABELS[state] ?? DEFAULT_STATE_LABEL;
  if (state === "PAUSED_AUTH" && underlyingState) {
    const base = STATE_LABELS[underlyingState];
    if (base) {
      return { ...label, text: `${base.text} (Paused)` };
    }
  }
  return label;
}

export function isFailureState(state: string | undefined): boolean {
  return !!state && FAILURE_STATES.has(state);
}

export function isTransientState(state: string | undefined): boolean {
  return !!state && TRANSIENT_STATES.has(state);
}

export function formatTime(iso: string | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

export function extractService(canonicalName: string): string {
  const match = canonicalName.match(/^\/\/([^/]+)\//);
  return match?.[1] ?? "";
}

const SERVICE_LABELS: Record<string, string> = {
  "kind.open-cluster-management.io": "Kind",
  "kubernetes.open-cluster-management.io": "Kubernetes",
};

export function serviceLabel(service: string): string {
  return SERVICE_LABELS[service] ?? service;
}

export function isHealthyState(state: string | undefined): boolean {
  return !!state && HEALTHY_STATES.has(state);
}

export function buildAddonBasePath(service: string): string {
  if (!Object.prototype.hasOwnProperty.call(SERVICE_LABELS, service)) {
    throw new Error(`Unsupported cluster service: ${service}`);
  }
  return `/apis/${service}/v1alpha`;
}

export function deriveClusterState(
  resource: ClusterResource,
): string | undefined {
  if (resource.pauseReason) return "PAUSED_AUTH";
  if (resource.state) return resource.state;
  const ready = resource.conditions?.Ready;
  if (ready?.status === "True") return "RUNNING";
  if (ready?.status === "False") return "ERROR";
  return undefined;
}

/** What the list and the detail page show as a cluster's state. */
export function clusterRowStatus(row: ClusterRow): ClusterRowStatus {
  if (row.origin === "enrolled") {
    return {
      ...enrolledClusterStatus(row.result),
      paused: false,
      transient: false,
    };
  }
  if (row.origin === "observed") {
    return {
      ...observedClusterStatus(row.result),
      paused: false,
      transient: false,
    };
  }
  const resource = row.result.resource;
  const state = deriveClusterState(resource);
  const label = stateLabel(state, resource.state);
  return {
    text: label.text,
    color: label.color,
    healthy: isHealthyState(resource.state),
    attention: isFailureState(resource.state),
    paused: state === "PAUSED_AUTH",
    transient: isTransientState(state) || !!resource.reconciling,
  };
}

/** How the cluster came to be known, as a short label. */
export function originLabel(row: ClusterRow): string {
  if (row.origin === "enrolled") return "Enrolled";
  if (row.origin === "observed") return "Observed";
  return serviceLabel(extractService(row.result.name));
}

/** When the cluster itself was created, as far as its record tells. */
export function clusterCreatedTime(row: ClusterRow): string | undefined {
  if (row.origin === "observed") {
    const createdAt = row.result.resource.observation?.createdAt;
    if (typeof createdAt === "string" && createdAt) return createdAt;
  }
  return row.result.resource.createTime;
}
