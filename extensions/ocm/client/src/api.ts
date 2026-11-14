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
  /** What enables the add-on for one cluster. */
  enableExample: string;
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
  return ocmRequest(`/hubs/${encodeURIComponent(hubId)}:enrollmentAddon`, req);
}

const hubApi = createResourceApi<HubResource>("-");
const observedApi = createResourceApi<ObservedClusterResource>("-");

function celString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

export function listHubs(): Promise<HubResult[]> {
  return hubApi.searchAll({ filter: `resourceType == "${HUB_TYPE}"` });
}

export function listHubClusters(
  hubId: string,
): Promise<ObservedClusterResult[]> {
  return observedApi.searchAll({
    filter: `resourceType == "${OBSERVED_CLUSTER_TYPE}" && resource.observation.hub == "${celString(hubId)}"`,
  });
}

/** The hub's record of the cluster that a console cluster id stands for. */
export async function findObservedCluster(
  clusterId: string,
): Promise<ObservedClusterResult | undefined> {
  const { resources } = await observedApi.search({
    filter: observedRecordFilter(clusterId),
    pageSize: 5,
  });
  // The record linked to the cluster comes before one that only shares
  // its name.
  return (
    resources.find((r) => linkedClusterId(r) === clusterId) ?? resources[0]
  );
}
