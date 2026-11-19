import type {
  ClusterResource,
  FleetClusterRow,
  FleetClusterStatus,
  ResourceResult,
} from "@ocm-fleet/common";
import {
  enrolledClusterStatus,
  HUB_AVAILABLE_CONDITION,
  hubReportedProblem,
  observedClusterStatus,
} from "@ocm-fleet/common";

/** A cluster of any origin, as the cluster list and detail page hold it. */
export type ClusterRow = FleetClusterRow<ResourceResult<ClusterResource>>;

export interface ClusterRowStatus extends FleetClusterStatus {
  paused: boolean;
  transient: boolean;
  /**
   * What the cluster's hub says of it, when that is not "available" and
   * the cluster is known here in its own right.
   */
  onHub?: FleetClusterStatus;
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

/** A cluster's state by its own record, before its hub's word is added. */
function ownRowStatus(row: ClusterRow): ClusterRowStatus {
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

/**
 * What the list and the detail page show as a cluster's state. A cluster
 * that its hub does not report as available carries the hub's word as
 * well and is not counted as healthy, even while it answers here.
 */
export function clusterRowStatus(row: ClusterRow): ClusterRowStatus {
  const status = ownRowStatus(row);
  const onHub = hubReportedProblem(row);
  if (!onHub) return status;
  return {
    ...status,
    healthy: false,
    attention: status.attention || onHub.attention,
    onHub,
  };
}

/** How the cluster came to be known, as a short label. */
export function originLabel(row: ClusterRow): string {
  if (row.origin === "enrolled") return "Enrolled";
  if (row.origin === "observed") return "Observed";
  return serviceLabel(extractService(row.result.name));
}

/** What the cluster list can be narrowed by. */
export interface ClusterListFilters {
  /** Part of a cluster's id. */
  name: string;
  /** Part of the id of a hub that reports the cluster. */
  hub: string;
}

/**
 * Whether a row passes the list's filters. Text is compared without
 * regard to case. The hub filter matches any hub that reports the
 * cluster, so a cluster two hubs know is found under each of them.
 */
export function matchesClusterFilters(
  row: ClusterRow,
  filters: Partial<ClusterListFilters>,
): boolean {
  const name = (filters.name ?? "").trim().toLowerCase();
  if (name && !row.id.toLowerCase().includes(name)) return false;
  const hub = (filters.hub ?? "").trim().toLowerCase();
  if (!hub) return true;
  const hubs = row.hubs ?? (row.hub ? [row.hub] : []);
  return hubs.some((id) => id.toLowerCase().includes(hub));
}

/**
 * The Hub column of a row: the hub whose facts the cluster carries, and
 * in brackets how many other connected hubs report it as well.
 */
export function hubColumn(row: ClusterRow): string {
  if (!row.hub) return "—";
  const others = (row.hubs ?? []).filter((id) => id !== row.hub);
  if (others.length === 0) return row.hub;
  return `${row.hub} (also ${others.join(", ")})`;
}

export interface HubFactsNotice {
  title: string;
  body: string;
}

/**
 * What to say on a cluster's page when the labels its hub supplied are
 * not being kept up to date: the hub does not answer, its connection was
 * removed, or it no longer reports the cluster. The labels still select
 * the cluster for deployments, which is why it is said. Null while the
 * labels are current, and for a cluster that carries no hub's labels.
 */
export function hubFactsNotice(
  conditions: ClusterResource["conditions"],
): HubFactsNotice | null {
  const facts = conditions?.HubFactsCurrent;
  if (!facts || facts.status !== "False") return null;
  const why = facts.message ? `${facts.message}.` : "";
  return {
    title: "The labels from its hub are not current",
    body: `${why} Deployments still select this cluster by them.`.trim(),
  };
}

const HUB_VIEW_TITLES: Record<string, string> = {
  Unknown: "has lost contact with this cluster",
  Unavailable: "reports this cluster as unavailable",
  "Not accepted": "does not accept this cluster",
};

/**
 * What to say on the page of a cluster that is known here in its own
 * right when its hub does not report it as available. The hub hears
 * from an agent on the cluster and the cluster is reached from here
 * directly, so the two can differ: the notice gives the hub's reason
 * and says whether the cluster still answers here. Null while the hub
 * reports the cluster available, and while the hub itself does not
 * answer (the notice about its labels says that).
 */
export function hubViewNotice(row: ClusterRow): HubFactsNotice | null {
  const onHub = hubReportedProblem(row);
  const title = onHub ? HUB_VIEW_TITLES[onHub.text] : undefined;
  if (!onHub || !title) return null;
  const reason =
    onHub.text === "Not accepted"
      ? ""
      : (row.observed?.resource.conditions?.[HUB_AVAILABLE_CONDITION]
          ?.message ?? "");
  const sentences: string[] = [];
  if (reason) {
    sentences.push(`The hub's reason: ${reason.replace(/\.$/, "")}.`);
  }
  if (ownRowStatus(row).healthy) {
    sentences.push(
      "The cluster still answers here: it is indexed and deployments are delivered to it.",
    );
  }
  return { title: `Hub ${row.hub} ${title}`, body: sentences.join(" ") };
}

/** When the cluster itself was created, as far as its record tells. */
export function clusterCreatedTime(row: ClusterRow): string | undefined {
  if (row.origin === "observed") {
    const createdAt = row.result.resource.observation?.createdAt;
    if (typeof createdAt === "string" && createdAt) return createdAt;
  }
  return row.result.resource.createTime;
}
