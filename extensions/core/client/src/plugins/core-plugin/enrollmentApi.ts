import { createApiClient, ResourceApiError } from "@ocm-fleet/common";

import { buildAddonBasePath, type ClusterRow } from "./clusterTypes";

/** The service whose enrollment routes the console acts through. */
export const ENROLLMENT_SERVICE = "kubernetes.open-cluster-management.io";

/** A cluster record as the enrollment routes answer with it. */
export interface EnrollmentClusterRecord {
  id: string;
  name: string;
  labels?: Record<string, string>;
  observation?: Record<string, unknown>;
}

interface EnrollmentResponse {
  cluster: EnrollmentClusterRecord;
}

export interface WithdrawOptions {
  /** Why the cluster is being let go; the record keeps it. */
  reason?: string;
  /**
   * Withdraw although deployments still resolve to the cluster's
   * target. What they delivered stays on the cluster, and the record
   * names them.
   */
  leaveDeployments?: boolean;
}

function enrollmentClient(service: string) {
  return createApiClient(buildAddonBasePath(service));
}

/**
 * Ask the cluster for a new credential now, with the one held for it,
 * and return the record that resulted.
 */
export async function renewClusterCredential(
  service: string,
  id: string,
): Promise<EnrollmentClusterRecord> {
  const response = await enrollmentClient(service).post<EnrollmentResponse>(
    `/clusters/${encodeURIComponent(id)}:renewCredential`,
    {},
  );
  return response.cluster;
}

/** The expiry a renewal answered with, or "" when it named none. */
export function credentialExpiresAt(cluster: EnrollmentClusterRecord): string {
  const expiresAt = cluster.observation?.credentialExpiresAt;
  return typeof expiresAt === "string" ? expiresAt : "";
}

/**
 * Withdraw an enrolled cluster: the record stays, marked withdrawn,
 * and everything the resource manager held for the cluster is gone.
 * Nothing on the cluster itself is touched.
 */
export async function withdrawCluster(
  service: string,
  id: string,
  options: WithdrawOptions = {},
): Promise<EnrollmentClusterRecord> {
  const body: WithdrawOptions = {};
  const reason = options.reason?.trim() ?? "";
  if (reason !== "") body.reason = reason;
  if (options.leaveDeployments) body.leaveDeployments = true;
  const response = await enrollmentClient(service).post<EnrollmentResponse>(
    `/clusters/${encodeURIComponent(id)}:withdraw`,
    body,
  );
  return response.cluster;
}

/** Forget a withdrawn cluster's record, so its id can be enrolled again. */
export async function forgetCluster(
  service: string,
  id: string,
): Promise<void> {
  await enrollmentClient(service).delete(`/clusters/${encodeURIComponent(id)}`);
}

/** What a bundle of one cluster asks for. */
export interface ManifestEnrollmentInput {
  clusterId: string;
  apiServer: string;
  profile: string;
  /**
   * Let the stored credential deliver deployments that are not signed.
   * Off by default, as at any other enrollment.
   */
  grantPlatformDelivery?: boolean;
}

/**
 * The body for POST enrollmentBundles: fields are trimmed, and a blank
 * one is left out so the server's validation names what is missing.
 */
export function manifestEnrollmentBody(
  input: ManifestEnrollmentInput,
): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  const clusterId = input.clusterId.trim();
  if (clusterId !== "") body.clusterId = clusterId;
  const apiServer = input.apiServer.trim();
  if (apiServer !== "") body.apiServer = apiServer;
  const profile = input.profile.trim();
  if (profile !== "") body.profile = profile;
  if (input.grantPlatformDelivery) body.grantPlatformDelivery = true;
  return body;
}

/** A bundle's record; its secret lives in the manifest, never here. */
export interface EnrollmentBundle {
  id: string;
  profile?: string;
  tokenLifetimeSeconds?: number;
}

export interface EnrollmentBundleAnswer {
  name: string;
  bundle: EnrollmentBundle;
  manifest: string;
  instructions: string[];
  expiresAt?: string;
}

/**
 * Create a bundle for one cluster with no hub, and receive the manifest
 * its administrator applies. Nothing changes on the cluster until then.
 */
export async function createManifestBundle(
  service: string,
  input: ManifestEnrollmentInput,
): Promise<EnrollmentBundleAnswer> {
  return enrollmentClient(service).post<EnrollmentBundleAnswer>(
    "/enrollmentBundles",
    manifestEnrollmentBody(input),
  );
}

/** Revoke a bundle, so its manifest no longer enrolls anything. */
export async function revokeEnrollmentBundle(
  service: string,
  id: string,
): Promise<void> {
  await enrollmentClient(service).delete(
    `/enrollmentBundles/${encodeURIComponent(id)}`,
  );
}

export interface WithdrawConflict {
  deployments: string[];
  target: string;
}

/**
 * The deployments that still resolve to the cluster's target, when a
 * withdrawal was refused for them. Any other failure is not a
 * conflict the dialog can resolve, and gives null.
 */
export function withdrawConflict(error: unknown): WithdrawConflict | null {
  if (!(error instanceof ResourceApiError)) return null;
  if (
    error.status !== 409 ||
    error.reason !== "deployments-resolve-to-target"
  ) {
    return null;
  }
  const details = error.details;
  if (!details || typeof details !== "object" || Array.isArray(details)) {
    return null;
  }
  const { deployments, target } = details as Record<string, unknown>;
  if (!Array.isArray(deployments) || deployments.length === 0) return null;
  if (!deployments.every((d): d is string => typeof d === "string")) {
    return null;
  }
  if (typeof target !== "string" || target === "") return null;
  return { deployments, target };
}

/** The state an enrollment record carries, or "" when it has none. */
export function enrolledState(row: ClusterRow): string {
  const observation = row.result.resource.observation;
  if (!observation || typeof observation !== "object") return "";
  const state = (observation as Record<string, unknown>).state;
  return typeof state === "string" ? state : "";
}

export interface EnrollmentActions {
  renew: boolean;
  withdraw: boolean;
  forget: boolean;
}

/**
 * What can be done to a cluster's enrollment record. Renewal needs a
 * credential the cluster still accepts, so only an enrolled cluster can
 * be renewed. A cluster whose credential lapsed (needs-enrollment) can
 * still be withdrawn: the target is enrolled, and withdrawal is how the
 * record is let go. Only a withdrawn record can be forgotten. A created
 * cluster is deleted, and an observed one belongs to its hub.
 */
export function enrollmentActions(row: ClusterRow): EnrollmentActions {
  const state = row.origin === "enrolled" ? enrolledState(row) : "";
  return {
    renew: state === "enrolled",
    withdraw: state === "enrolled" || state === "needs-enrollment",
    forget: state === "withdrawn",
  };
}
