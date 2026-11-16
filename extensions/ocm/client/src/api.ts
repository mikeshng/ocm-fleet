import {
  createResourceApi,
  HUB_TYPE,
  linkedClusterId,
  OBSERVED_CLUSTER_TYPE,
  observedRecordFilter,
  type ResourceResult,
} from "@ocm-fleet/common";

export const OCM_SERVICE = "ocm.open-cluster-management.io";

const OCM_BASE = `/apis/${OCM_SERVICE}/v1alpha`;

export interface Condition {
  status: string;
  reason?: string;
  message?: string;
}

/** How a hub's enrollment add-on reaches the hub's clusters. */
export interface EnrollmentRollout {
  /** manual, every-cluster, or placement. */
  mode: string;
  placementNamespace?: string;
  placementName?: string;
}

export interface HubObservation {
  id: string;
  displayName?: string;
  apiServer: string;
  caFingerprint?: string;
  serviceAccount?: string;
  connectedBy?: string;
  connectedAt?: string;
  lastSeenAt?: string;
  credentialExpiresAt?: string;
  writeAccessFound?: boolean;
  clusterCount?: number;
  acceptedCount?: number;
  availableCount?: number;
  enrolledCount?: number;
  /** How many of the hub's clusters were enrolled through its add-on. */
  enrolledThroughHubCount?: number;
  /** The rollout of the add-on last generated for this connection. */
  enrollmentRollout?: EnrollmentRollout;
  /** How many valid enrollment secrets cover the hub's clusters. */
  enrollmentSecrets?: number;
}

export interface HubResource {
  name: string;
  uid: string;
  observation?: HubObservation;
  conditions?: Record<string, Condition>;
}

export interface EnrollmentLink {
  /** observed, enrolled, created, or withdrawn. */
  state: string;
  cluster?: string;
  target?: string;
  basis?: string;
}

export interface ObservedClusterObservation {
  hub: string;
  name: string;
  accepted: boolean;
  available: string;
  clusterSet?: string;
  claims?: Record<string, string>;
  kubernetesVersion?: string;
  clientConfigs?: { url: string; caFingerprint?: string }[];
  taints?: string[];
  createdAt?: string;
  enrollment?: EnrollmentLink;
  conflict?: string;
}

export interface ObservedClusterResource {
  name: string;
  uid: string;
  localLabels?: Record<string, string>;
  observation?: ObservedClusterObservation;
  conditions?: Record<string, Condition>;
}

export type HubResult = ResourceResult<HubResource>;
export type ObservedClusterResult = ResourceResult<ObservedClusterResource>;

export interface HubAccessManifest {
  manifest: string;
  instructions: string[];
}

export interface ConnectHubRequest {
  hubId: string;
  displayName?: string;
  /** Either a kubeconfig, or the address, CA certificate, and token. */
  kubeconfig?: string;
  apiServer?: string;
  caCert?: string;
  token?: string;
}

export interface EnrollmentAddonRequest {
  profile?: string;
  grantPlatformDelivery?: boolean;
  removeAccessWithAddon?: boolean;
  /** manual, every-cluster, or placement; left out, the hub's last one. */
  rollout?: string;
  placementNamespace?: string;
  placementName?: string;
}

export interface EnrollmentBundle {
  id: string;
  profile: string;
  grantPlatformDelivery: boolean;
  createdAt?: string;
  expiresAt?: string;
}

export interface EnrollmentAddon {
  bundle: EnrollmentBundle;
  /** Applied once to the hub by its administrator. Carries a secret. */
  manifest: string;
  /** What enables the add-on for one cluster; empty unless the rollout is manual. */
  enableExample: string;
  /** The rollout the add-on was made for. */
  rollout?: EnrollmentRollout;
  instructions: string[];
}

function errorMessage(body: unknown, status: number): string {
  if (body && typeof body === "object") {
    const fields = body as Record<string, unknown>;
    for (const key of ["error", "message"]) {
      const value = fields[key];
      if (typeof value === "string" && value) return value;
    }
  }
  return `Request failed (${status})`;
}

async function ocmRequest<T>(path: string, body?: unknown): Promise<T> {
  const init: RequestInit =
    body === undefined
      ? { method: "GET" }
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        };
  const res = await fetch(`${OCM_BASE}${path}`, init);
  const payload: unknown = await res.json().catch(() => null);
  if (!res.ok) throw new Error(errorMessage(payload, res.status));
  return payload as T;
}

/** What a hub's administrator applies before the hub can be connected. */
export function getHubAccessManifest(): Promise<HubAccessManifest> {
  return ocmRequest("/hubAccessManifest");
}

export function connectHub(req: ConnectHubRequest): Promise<unknown> {
  return ocmRequest("/hubs:connect", req);
}

export function disconnectHub(hubId: string): Promise<unknown> {
  return ocmRequest(`/hubs/${encodeURIComponent(hubId)}:disconnect`, {});
}

/**
 * Creates an enrollment bundle for the hub's clusters and returns the
 * add-on that carries it, for the hub's administrator to apply.
 */
export function createEnrollmentAddon(
  hubId: string,
  req: EnrollmentAddonRequest,
): Promise<EnrollmentAddon> {
  return ocmRequest(
    `/hubs/${encodeURIComponent(hubId)}:generateEnrollmentAddon`,
    req,
  );
}

/**
 * Reads the live observation of a connected hub: what its watch sees now.
 * The hub records are written at most every few minutes while only the
 * last-heard time changes, so a view that shows that time reads it here.
 */
export function getHubObservation(hubId: string): Promise<HubObservation> {
  return ocmRequest(`/hubs/${encodeURIComponent(hubId)}/observation`);
}

const ENROLLMENT_BASE = "/apis/kubernetes.open-cluster-management.io/v1alpha";

/** Revokes an enrollment bundle; its manifest then enrolls nothing. */
export async function revokeEnrollmentBundle(id: string): Promise<void> {
  const res = await fetch(
    `${ENROLLMENT_BASE}/enrollmentBundles/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
  if (!res.ok) {
    const payload: unknown = await res.json().catch(() => null);
    throw new Error(errorMessage(payload, res.status));
  }
}

const hubApi = createResourceApi<HubResource>("-");
const observedApi = createResourceApi<ObservedClusterResource>("-");

function celString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

export function listHubClusters(
  hubId: string,
): Promise<ObservedClusterResult[]> {
  return observedApi.searchAll({
    filter: `resourceType == "${OBSERVED_CLUSTER_TYPE}" && resource.observation.hub == "${celString(hubId)}"`,
  });
}

/** The hub id a listed record is about. */
export function hubIdOf(hub: HubResult): string {
  return (
    hub.resource.observation?.id ?? hub.resource.name.replace(/^hubs\//, "")
  );
}

/**
 * How many of a hub's clusters the hub does not report as available. A
 * record that carries no counts yet has none.
 */
export function unavailableClusterCount(observation?: HubObservation): number {
  return Math.max(
    0,
    (observation?.clusterCount ?? 0) - (observation?.availableCount ?? 0),
  );
}

/**
 * Puts each hub's live last-heard time on its listed record. The records
 * are written at most every few minutes while only that time changes, so
 * a hub that answers could show an older time than one that does not; the
 * live observation of each hub answers the watch's time. A hub whose
 * observation cannot be read keeps its record's time.
 */
export function mergeLiveHubTimes(
  hubs: HubResult[],
  live: ReadonlyMap<string, string | undefined>,
): HubResult[] {
  return hubs.map((hub) => {
    const lastSeenAt = live.get(hubIdOf(hub));
    const observation = hub.resource.observation;
    if (!lastSeenAt || !observation || observation.lastSeenAt === lastSeenAt) {
      return hub;
    }
    return {
      ...hub,
      resource: {
        ...hub.resource,
        observation: { ...observation, lastSeenAt },
      },
    };
  });
}

async function liveHubTimes(
  hubs: HubResult[],
): Promise<Map<string, string | undefined>> {
  const pairs = await Promise.all(
    hubs.map(async (hub): Promise<[string, string | undefined]> => {
      const id = hubIdOf(hub);
      try {
        return [id, (await getHubObservation(id)).lastSeenAt];
      } catch {
        return [id, undefined];
      }
    }),
  );
  return new Map(pairs);
}

export async function listHubs(): Promise<HubResult[]> {
  const hubs = await hubApi.searchAll({
    filter: `resourceType == "${HUB_TYPE}"`,
  });
  return mergeLiveHubTimes(hubs, await liveHubTimes(hubs));
}

/**
 * Puts the hub records of one cluster in the order they are shown: the
 * records linked to the cluster before one that only shares its name, the
 * record of the preferred hub first among them, then by hub id.
 */
export function orderObservedClusters(
  records: ObservedClusterResult[],
  clusterId: string,
  preferredHub?: string,
): ObservedClusterResult[] {
  const linked = records.filter((r) => linkedClusterId(r) === clusterId);
  const shown = linked.length > 0 ? linked : records.slice(0, 1);
  const hubOf = (r: ObservedClusterResult) => r.resource.observation?.hub ?? "";
  return [...shown].sort(
    (a, b) =>
      Number(hubOf(b) === preferredHub) - Number(hubOf(a) === preferredHub) ||
      hubOf(a).localeCompare(hubOf(b)),
  );
}

/**
 * The hubs' records of the cluster that a console cluster id stands for:
 * one per connected hub that reports it.
 */
export async function findObservedClusters(
  clusterId: string,
  preferredHub?: string,
): Promise<ObservedClusterResult[]> {
  const { resources } = await observedApi.search({
    filter: observedRecordFilter(clusterId),
    pageSize: 20,
  });
  return orderObservedClusters(resources, clusterId, preferredHub);
}
