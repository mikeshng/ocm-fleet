/**
 * Base URL for the resource query API.
 * Matches the proto: `/apis/open-cluster-management.io/v1alpha/{scope}:queryResources`
 */
const QUERY_BASE = "/apis/open-cluster-management.io/v1alpha";

// ---------------------------------------------------------------------------
// Response / result types (from proto: ocm/v1alpha/resource_result.proto)
// ---------------------------------------------------------------------------

/**
 * Common envelope fields always present inside the `resource` Struct,
 * regardless of which capabilities the addon declares.
 */
export interface ResourceCommon {
  name: string;
  uid: string;
  labels: Record<string, string>;
  createTime: string;
  updateTime: string;
  etag: string;
}

/**
 * A single resource returned by QueryResources.
 *
 * The generic `Props` parameter types the addon-specific fields inside
 * the `resource` Struct (spec, state, etc.). The `resource` body always
 * includes {@link ResourceCommon} fields plus whatever the addon schema
 * defines.
 */
export interface ResourceResult<Props = Record<string, unknown>> {
  /** Canonical full resource name, e.g. `"//kind.open-cluster-management.io/clusters/prod"`. */
  name: string;
  /** Stable type identity, e.g. `"kind.open-cluster-management.io/Cluster"`. */
  resourceType: string;
  /** Dynamic resource body (protojson-encoded Struct). */
  resource: ResourceCommon & Props;
}

/** Paginated response from QueryResources. */
export interface QueryResourcesResponse<Props = Record<string, unknown>> {
  resources: ResourceResult<Props>[];
  /** Empty string when there are no more pages. */
  nextPageToken: string;
}

// ---------------------------------------------------------------------------
// Request parameters
// ---------------------------------------------------------------------------

/** Parameters accepted by {@link ResourceApi.search}. */
export interface QueryResourcesParams {
  /** CEL filter expression. Empty matches everything. */
  filter?: string;
  /** Max results per page (server default applies if unset). */
  pageSize?: number;
  /** Token from a previous response to fetch the next page. */
  pageToken?: string;
  /** Ordering. Empty for server default. v0 supports `"resource_type,name"`. */
  orderBy?: string;
}

// ---------------------------------------------------------------------------
// API error
// ---------------------------------------------------------------------------

/** Structured error body returned by the gRPC-gateway. */
export interface RpcStatus {
  code: number;
  message: string;
  details?: unknown[];
}

/** A parsed error body, when it is a plain object. */
function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

/** A non-empty string field of a parsed error body. */
function stringField(value: unknown, key: string): string | undefined {
  const field = asRecord(value)?.[key];
  return typeof field === "string" && field !== "" ? field : undefined;
}

/**
 * Error thrown when an API request returns a non-OK response.
 *
 * Query API errors carry the gRPC-gateway `RpcStatus`; add-on routes
 * answer their own envelopes (for example
 * `{error, reason, details}`), which are kept whole in {@link body} so
 * a caller can read the server's sentence, its stable reason, and any
 * structured detail. Both are `null` when the body is absent or not a
 * plain object. Some record routes answer as `text/plain`; that sentence
 * is kept as the message.
 */
export class ResourceApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly rpcStatus: RpcStatus | null,
    public readonly body: Record<string, unknown> | null = null,
    plainText?: string,
  ) {
    super(ResourceApiError.messageFor(status, rpcStatus, body, plainText));
    this.name = "ResourceApiError";
  }

  private static messageFor(
    status: number,
    rpcStatus: RpcStatus | null,
    body: Record<string, unknown> | null,
    plainText?: string,
  ): string {
    return (
      stringField(rpcStatus, "message") ??
      stringField(body, "error") ??
      stringField(body, "message") ??
      (plainText?.trim() || undefined) ??
      `Query API error ${status}`
    );
  }

  /** The stable reason an add-on gives for a refusal, if any. */
  get reason(): string | undefined {
    return stringField(this.body, "reason");
  }

  /** The refusal's structured detail, as the server sent it. */
  get details(): unknown {
    return asRecord(this.body)?.details;
  }
}

/**
 * Parse a non-OK response into a {@link ResourceApiError}.
 *
 * The body is read once as text: valid JSON keeps its envelope, and a
 * body that is not JSON (the dynamic API's record routes answer
 * `text/plain`) is kept as the message.
 */
async function errorFromResponse(res: Response): Promise<ResourceApiError> {
  let text = "";
  try {
    text = await res.text();
  } catch {
    text = "";
  }
  let parsed: unknown = null;
  let plainText: string | undefined;
  if (text !== "") {
    try {
      parsed = JSON.parse(text);
    } catch {
      plainText = text;
    }
  }
  return new ResourceApiError(
    res.status,
    parsed as RpcStatus | null,
    asRecord(parsed),
    plainText,
  );
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function buildQueryUrl(scope: string, params?: QueryResourcesParams): string {
  const qp = new URLSearchParams();
  if (params?.filter) qp.set("filter", params.filter);
  if (params?.pageSize != null) qp.set("pageSize", String(params.pageSize));
  if (params?.pageToken) qp.set("pageToken", params.pageToken);
  if (params?.orderBy) qp.set("orderBy", params.orderBy);
  const qs = qp.toString();
  const path = `${QUERY_BASE}/${encodeURIComponent(scope)}:queryResources`;
  return qs ? `${path}?${qs}` : path;
}

/** Escape a value for embedding in a CEL string literal (backslash + double-quote). */
function escapeCelString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

async function queryRequest<Props>(
  scope: string,
  params?: QueryResourcesParams,
): Promise<QueryResourcesResponse<Props>> {
  const url = buildQueryUrl(scope, params);
  const res = await fetch(url);
  if (!res.ok) {
    throw await errorFromResponse(res);
  }
  return res.json() as Promise<QueryResourcesResponse<Props>>;
}

// ---------------------------------------------------------------------------
// Public API object
// ---------------------------------------------------------------------------

/** Typed client returned by {@link createResourceApi}. */
export interface ResourceApi<Props = Record<string, unknown>> {
  /**
   * Query resources with an optional CEL filter and pagination.
   * Maps directly to `ResourceQueryService.QueryResources`.
   */
  search(params?: QueryResourcesParams): Promise<QueryResourcesResponse<Props>>;

  /**
   * List all resources (no filter). Convenience wrapper around
   * {@link search} with only pagination parameters.
   */
  list(
    params?: Pick<QueryResourcesParams, "pageSize" | "pageToken">,
  ): Promise<QueryResourcesResponse<Props>>;

  /**
   * Fetch a single resource by its full resource name.
   *
   * Uses a CEL filter `name == "{resourceName}"` under the hood.
   * Returns `undefined` when no match is found.
   */
  get(resourceName: string): Promise<ResourceResult<Props> | undefined>;

  /**
   * Fetch all pages of results and return a flat array.
   * Automatically follows `nextPageToken` until exhausted.
   */
  searchAll(
    params?: Omit<QueryResourcesParams, "pageToken">,
  ): Promise<ResourceResult<Props>[]>;
}

/**
 * Create a typed resource-query API client scoped to a resource
 * hierarchy location.
 *
 * @example
 * ```ts
 * interface ClusterResource { state: string }
 * const clusterApi = createResourceApi<ClusterResource>("-");
 *
 * // query with a CEL filter
 * const { resources } = await clusterApi.search({
 *   filter: 'resource_type == "kind.open-cluster-management.io/Cluster"',
 *   pageSize: 20,
 * });
 *
 * // auto-paginate
 * const all = await clusterApi.searchAll();
 * ```
 *
 * @param scope - Scope to search within (use `"-"` for global).
 */
export function createResourceApi<Props = Record<string, unknown>>(
  scope: string,
): ResourceApi<Props> {
  return {
    search(params) {
      return queryRequest<Props>(scope, params);
    },

    list(params) {
      return queryRequest<Props>(scope, params);
    },

    async get(resourceName) {
      const { resources } = await queryRequest<Props>(scope, {
        filter: `name == "${escapeCelString(resourceName)}"`,
        pageSize: 1,
      });
      return resources[0];
    },

    async searchAll(params) {
      const allResults: ResourceResult<Props>[] = [];
      let pageToken: string | undefined;
      const seenTokens = new Set<string>();
      do {
        const response = await queryRequest<Props>(scope, {
          ...params,
          pageToken,
        });
        allResults.push(...response.resources);
        pageToken = response.nextPageToken || undefined;
        if (pageToken && seenTokens.has(pageToken)) {
          throw new Error(
            `Cyclic pagination detected: repeated nextPageToken "${pageToken}"`,
          );
        }
        if (pageToken) seenTokens.add(pageToken);
      } while (pageToken);
      return allResults;
    },
  };
}

// ---------------------------------------------------------------------------
// Generic REST API client
// ---------------------------------------------------------------------------

/**
 * Generic REST client for non-search API calls (CRUD operations).
 *
 * Replaces per-plugin `makeRequest` / `mgmtFetch` wrappers with a shared,
 * typed client that uses the same structured {@link ResourceApiError} error
 * handling as the query client.
 */
export interface ApiClient {
  /** GET `basePath + path` with optional query params. */
  get<T>(path: string, params?: Record<string, string>): Promise<T>;
  /** POST `basePath + path` with optional JSON body. */
  post<T>(path: string, body?: unknown): Promise<T>;
  /** PUT `basePath + path` with optional JSON body. */
  put<T>(path: string, body?: unknown): Promise<T>;
  /** PATCH `basePath + path` with optional JSON body. */
  patch<T>(path: string, body?: unknown): Promise<T>;
  /** DELETE `basePath + path`. Returns `undefined` for 204 No Content. */
  delete<T = void>(path: string): Promise<T>;
}

async function apiRequest<T>(
  basePath: string,
  path: string,
  init?: RequestInit,
): Promise<T> {
  const res = await fetch(`${basePath}${path}`, init);
  if (!res.ok) {
    throw await errorFromResponse(res);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

function jsonInit(method: string, body?: unknown): RequestInit {
  if (body == null) return { method };
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

/**
 * Create a generic REST client scoped to `basePath`.
 *
 * @example
 * ```ts
 * const mgmt = createApiClient("/v1alpha");
 * const deployments = await mgmt.get<ListDeploymentsResponse>("/deployments");
 *
 * const kind = createApiClient("/apis/kind.open-cluster-management.io/v1alpha");
 * const cluster = await kind.post<KindCluster>(
 *   "/clusters?cluster_id=my-cluster",
 *   { spec },
 * );
 * ```
 */
export function createApiClient(basePath: string): ApiClient {
  return {
    get<T>(path: string, params?: Record<string, string>): Promise<T> {
      const qs = params ? `?${new URLSearchParams(params).toString()}` : "";
      return apiRequest<T>(basePath, `${path}${qs}`);
    },
    post<T>(path: string, body?: unknown): Promise<T> {
      return apiRequest<T>(basePath, path, jsonInit("POST", body));
    },
    put<T>(path: string, body?: unknown): Promise<T> {
      return apiRequest<T>(basePath, path, jsonInit("PUT", body));
    },
    patch<T>(path: string, body?: unknown): Promise<T> {
      return apiRequest<T>(basePath, path, jsonInit("PATCH", body));
    },
    delete<T>(path: string): Promise<T> {
      return apiRequest<T>(basePath, path, { method: "DELETE" });
    },
  };
}

// ---------------------------------------------------------------------------
// Backward-compat type aliases (PR #39 names)
// ---------------------------------------------------------------------------

/** @deprecated Use {@link ResourceResult} instead. */
export type ResourceSearchResult<Props = Record<string, unknown>> =
  ResourceResult<Props>;

/** @deprecated Use {@link QueryResourcesResponse} instead. */
export type SearchResourcesResponse<Props = Record<string, unknown>> =
  QueryResourcesResponse<Props>;

/** @deprecated Use {@link QueryResourcesParams} instead. */
export type SearchResourcesParams = QueryResourcesParams;

/** @deprecated Use {@link ResourceCommon} instead. */
export type Condition = {
  type: string;
  status: string;
  reason: string;
  message: string;
  lastTransitionTime: string;
};
