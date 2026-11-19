const MGMT_BASE = "/v1alpha";
const TARGETS_PATH = "/apis/open-cluster-management.io/v1alpha/targets";
/** A defensive bound on a walk the server never ends; 10k pages is 500k deployments. */
const MAX_DEPLOYMENT_LIST_PAGES = 10_000;

async function apiFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const msg =
      (body as Record<string, string>).message ||
      (body as Record<string, string>).error ||
      `Management API error (${res.status})`;
    throw new Error(msg);
  }
  return res.json();
}

export function mgmtFetch<T>(path: string, init?: RequestInit): Promise<T> {
  return apiFetch(`${MGMT_BASE}${path}`, init);
}

// --- Targets ---

export interface MgmtTarget {
  id: string;
  name: string;
  type: string;
  state: string;
  labels: Record<string, string>;
  acceptedManifestTypes: string[];
  /** How the target came to exist, for example "provisioned" or "enrolled". */
  origin: string;
  /** The cluster record the target's objects are indexed under. */
  cluster?: string;
  accessForm?: string;
  deliveryCredential?: string;
}

export interface ListTargetsResponse {
  targets: MgmtTarget[];
}

export function listTargets(): Promise<ListTargetsResponse> {
  return apiFetch(TARGETS_PATH);
}

export interface RegistrySubjectMapping {
  registryId: string;
  expression: string;
}

export interface OIDCConfig {
  issuerUrl: string;
  audience: string;
  keyEnrollmentAudience?: string;
  authorizationEndpoint?: string;
  tokenEndpoint?: string;
  jwksUri?: string;
  registrySubjectMapping?: RegistrySubjectMapping;
}

export interface AuthMethod {
  name: string;
  type: "TYPE_UNSPECIFIED" | "TYPE_OIDC";
  oidcConfig?: OIDCConfig;
}

export function getAuthMethod(name: string): Promise<AuthMethod> {
  return mgmtFetch(`/authMethods/${encodeURIComponent(name)}`);
}

// --- Deployments ---

export type DeploymentState =
  | "STATE_UNSPECIFIED"
  | "STATE_CREATING"
  | "STATE_ACTIVE"
  | "STATE_DELETING"
  | "STATE_FAILED";

export type TargetDeliveryState =
  | "STATE_UNSPECIFIED"
  | "STATE_PENDING"
  | "STATE_ACCEPTED"
  | "STATE_PROGRESSING"
  | "STATE_DELIVERED"
  | "STATE_FAILED"
  | "STATE_PARTIAL"
  | "STATE_AUTH_FAILED";

/** The kind of work a target delivery status describes. */
export type TargetDeliveryOperation =
  "OPERATION_UNSPECIFIED" | "OPERATION_DELIVER" | "OPERATION_REMOVE";

/** Delivery state of one deployment on one target. */
export interface TargetDeliveryStatus {
  targetId: string;
  state: TargetDeliveryState;
  /**
   * Whether the recorded work applies the manifests to the target or
   * removes them from it. A missing value is a delivery.
   */
  operation?: TargetDeliveryOperation;
  /** Reason the last delivery attempt failed; empty once delivered. */
  message?: string;
  /** Failed delivery attempts at the current generation. */
  attempts?: number;
  updateTime?: string;
}

export interface Manifest {
  manifestType: string;
  raw: string;
}

export interface ManifestStrategy {
  type: "TYPE_UNSPECIFIED" | "TYPE_INLINE";
  manifests?: Manifest[];
}

export interface TargetSelector {
  matchLabels: Record<string, string>;
}

export interface PlacementStrategy {
  type: "TYPE_UNSPECIFIED" | "TYPE_STATIC" | "TYPE_ALL" | "TYPE_SELECTOR";
  targetIds?: string[];
  targetSelector?: TargetSelector;
}

export interface RolloutStrategy {
  type: "TYPE_UNSPECIFIED" | "TYPE_IMMEDIATE";
}

export interface MgmtDeployment {
  name: string;
  uid: string;
  manifestStrategy: ManifestStrategy;
  placementStrategy: PlacementStrategy;
  rolloutStrategy?: RolloutStrategy;
  resolvedTargetIds: string[];
  state: DeploymentState;
  reconciling: boolean;
  createTime: string;
  updateTime: string;
  etag: string;
  /** Delivery state on each resolved target, ordered by target ID. */
  targetStatuses?: TargetDeliveryStatus[];
}

export interface ListDeploymentsResponse {
  deployments: MgmtDeployment[];
  nextPageToken: string;
}

export async function listDeployments(): Promise<ListDeploymentsResponse> {
  // The server answers one page at a time: follow nextPageToken and merge
  // the pages so callers see every deployment. A server whose pages never
  // end is reported rather than followed forever: a token answered twice is
  // a cycle, and a page cap bounds a server that keeps minting new tokens.
  const deployments: MgmtDeployment[] = [];
  const seenTokens = new Set<string>();
  let pageToken: string | undefined;
  do {
    if (seenTokens.size >= MAX_DEPLOYMENT_LIST_PAGES) {
      throw new Error(
        `The deployment list did not end after ${MAX_DEPLOYMENT_LIST_PAGES} pages`,
      );
    }
    const params = new URLSearchParams();
    if (pageToken) params.set("pageToken", pageToken);
    const response = await mgmtFetch<ListDeploymentsResponse>(
      `/deployments${params.size ? `?${params}` : ""}`,
    );
    deployments.push(...(response.deployments ?? []));
    const next = response.nextPageToken || undefined;
    if (next && seenTokens.has(next)) {
      throw new Error(
        `Cyclic pagination detected: repeated nextPageToken "${next}"`,
      );
    }
    if (next) seenTokens.add(next);
    pageToken = next;
  } while (pageToken);
  return { deployments, nextPageToken: "" };
}

export interface CreateDeploymentRequest {
  deploymentId: string;
  deployment: {
    manifestStrategy: ManifestStrategy;
    placementStrategy: PlacementStrategy;
    rolloutStrategy?: RolloutStrategy;
  };
  /** Base64-encoded ECDSA-P256-SHA256 ASN.1 DER signature. */
  userSignature?: string;
  /** ISO 8601 timestamp for signing envelope reconstruction. */
  validUntil?: string;
  expectedGeneration?: number;
}

export function createDeployment(
  req: CreateDeploymentRequest,
): Promise<MgmtDeployment> {
  const params = new URLSearchParams();
  params.set("deployment_id", req.deploymentId);
  if (req.validUntil) {
    params.set("valid_until", req.validUntil);
  }
  if (req.expectedGeneration) {
    params.set("expected_generation", String(req.expectedGeneration));
  }
  if (req.userSignature) {
    params.set("user_signature", req.userSignature);
  }
  return mgmtFetch(`/deployments?${params}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req.deployment),
  });
}

// --- Signer Enrollments ---

export interface SignerEnrollment {
  name: string;
  subject: string;
  issuer: string;
  registrySubject: string;
  registryId: string;
  createTime: string;
  expireTime: string;
}

export interface CreateSignerEnrollmentRequest {
  signerEnrollmentId: string;
  identityToken: string;
  registryId?: string;
}

export function createSignerEnrollment(
  req: CreateSignerEnrollmentRequest,
): Promise<SignerEnrollment> {
  return mgmtFetch("/signerEnrollments", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      signer_enrollment_id: req.signerEnrollmentId,
      identity_token: req.identityToken,
      ...(req.registryId && { registry_id: req.registryId }),
    }),
  });
}
