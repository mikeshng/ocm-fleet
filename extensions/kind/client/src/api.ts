import { createApiClient } from "@ocm-fleet/common";

const client = createApiClient("/apis/kind.open-cluster-management.io/v1alpha");

export interface KindNodeSpec {
  role: "control-plane" | "worker";
  image?: string;
}

export interface KindNetworkingSpec {
  apiServerPort?: number;
  podSubnet?: string;
  serviceSubnet?: string;
}

export interface KindClusterSpec {
  name?: string;
  nodes?: KindNodeSpec[];
  networking?: KindNetworkingSpec;
}

export interface KindCluster {
  name: string;
  uid: string;
  spec: KindClusterSpec;
  state: string;
  createTime: string;
  updateTime: string;
  etag: string;
  generation: string;
}

/** The name charset kind accepts for a cluster id. */
const KIND_CLUSTER_ID = /^[a-z0-9.-]+$/;

/** The most characters an id can have behind the server's ownership prefix. */
const MAX_CLUSTER_ID_LENGTH = 39;

/**
 * The reason a name cannot be used as a kind cluster id, or null when it
 * can. This mirrors the server's own check, so the wizard refuses a name
 * the provider would reject before sending the request.
 */
export function kindClusterNameError(name: string): string | null {
  if (name === "") return "Enter a cluster name.";
  if (!KIND_CLUSTER_ID.test(name)) {
    return "Use only lowercase letters, digits, '.', and '-'.";
  }
  if (name.length > MAX_CLUSTER_ID_LENGTH) {
    return `Use at most ${MAX_CLUSTER_ID_LENGTH} characters.`;
  }
  return null;
}

/** Create a kind cluster; a refusal keeps the server's own sentence. */
export async function createKindCluster(
  clusterId: string,
  spec: KindClusterSpec,
): Promise<KindCluster> {
  return client.post<KindCluster>(
    `/clusters?cluster_id=${encodeURIComponent(clusterId)}`,
    { spec },
  );
}
