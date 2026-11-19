import type { ConnectHubRequest } from "./api";

/** The server measures the display name in bytes. */
export const DISPLAY_NAME_MAX_BYTES = 120;

/** The server's hub id rule: a DNS label of at most 40 characters. */
const HUB_ID = /^[a-z0-9]([-a-z0-9]{0,38}[a-z0-9])?$/;

/** What the server asks of a hub id. */
export const HUB_ID_RULE =
  "The hub ID is 1 to 40 lowercase letters, digits, or '-', starting and ending with a letter or digit.";

/** The reason a hub id would be refused, or null when the server takes it. */
export function hubIdError(hubId: string): string | null {
  return HUB_ID.test(hubId.trim()) ? null : HUB_ID_RULE;
}

export interface HubConnectionDetails {
  hubId: string;
  displayName?: string;
  mode: "fields" | "kubeconfig";
  apiServer?: string;
  caCert?: string;
  token?: string;
  kubeconfig?: string;
}

/**
 * Shapes the connect request. The hub id and the display name are
 * trimmed; an empty display name is left out, so that a reconnect does
 * not replace a name stored earlier with nothing.
 */
export function connectHubRequest(
  details: HubConnectionDetails,
): ConnectHubRequest {
  const request: ConnectHubRequest = { hubId: details.hubId.trim() };
  const displayName = details.displayName?.trim() ?? "";
  if (displayName !== "") {
    request.displayName = displayName;
  }
  if (details.mode === "kubeconfig") {
    request.kubeconfig = details.kubeconfig ?? "";
  } else {
    request.apiServer = details.apiServer?.trim() ?? "";
    request.caCert = details.caCert ?? "";
    request.token = details.token?.trim() ?? "";
  }
  return request;
}

/**
 * The server limits the display name by bytes (Go's `len`), while an
 * input's `maxlength` counts UTF-16 units; validate what is sent.
 */
export function displayNameError(displayName: string): string | null {
  const bytes = new TextEncoder().encode(displayName.trim()).length;
  if (bytes <= DISPLAY_NAME_MAX_BYTES) return null;
  return `The display name is ${bytes} bytes; at most ${DISPLAY_NAME_MAX_BYTES} are allowed.`;
}
