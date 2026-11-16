import { flagString, type ParsedArgs } from "../../argv";

export const hubApi = "/apis/ocm.open-cluster-management.io/v1alpha";

export interface HubCondition {
  status?: string;
}

export interface HubRecord {
  name?: string;
  displayName?: string;
  conditions?: Record<string, HubCondition>;
  observation?: {
    id?: string;
    clusterCount?: number;
    availableCount?: number;
    enrolledCount?: number;
    writeAccessFound?: boolean;
  };
}

export function requiredHubId(args: ParsedArgs): string {
  const id = flagString(args, "id") || args.positionals[0];
  if (!id) throw new Error("--id is required");
  return id;
}

export function hubIdOf(hub: HubRecord): string {
  return hub.observation?.id || (hub.name ?? "").replace(/^hubs\//, "");
}

export function hubPath(id: string): string {
  return `${hubApi}/hubs/${encodeURIComponent(id)}`;
}

export function hubStatus(hub: HubRecord): string {
  const status = (type: string) => hub.conditions?.[type]?.status ?? "";
  if (status("Ready") === "True") return "Connected";
  if (status("CredentialValid") === "False") return "Credential not valid";
  if (status("Connected") === "False") return "Not reachable";
  if (status("Synced") === "False") return "Reading";
  return "Not ready";
}

export function hubAccess(hub: HubRecord): string {
  return hub.observation?.writeAccessFound ? "can write" : "read-only";
}

/** A manifest ends with a newline, and so does every line the CLI prints. */
export function withoutFinalNewline(text: string): string {
  return text.replace(/\n$/, "");
}
