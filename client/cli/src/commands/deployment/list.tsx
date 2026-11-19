import { Box, Text } from "ink";
import Table from "ink-table";
import React from "react";

import { flagString } from "../../argv";
import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import type {
  DeploymentListItem,
  DeploymentListResponse,
  TargetDeliveryState,
  TargetDeliveryStatus,
} from "./types";
const FAILED_STATES: readonly TargetDeliveryState[] = [
  "STATE_FAILED",
  "STATE_PARTIAL",
  "STATE_AUTH_FAILED",
];

/** A defensive bound on a walk the server never ends; 10k pages is 500k deployments. */
const maxDeploymentListPages = 10_000;

export const listCommand = {
  path: "deployment list",
  description: "List deployments",
  implemented: true,
  present: (data) => <DeploymentListView data={data} />,
  run: async ({ args }) => {
    // The server answers one page at a time: list every page so that the
    // command shows all deployments, not just the first page.
    const client = await clientForArgs(args);
    const pageSize = flagString(args, "page-size");
    const deployments: NonNullable<DeploymentListResponse["deployments"]> = [];
    // A server whose pages never end would be followed forever. A token
    // answered twice is reported as a cycle rather than returned as a
    // silently truncated list, and a page cap bounds a server that keeps
    // minting new tokens.
    const seenTokens = new Set<string>();
    let pageToken: string | undefined;
    do {
      if (seenTokens.size >= maxDeploymentListPages) {
        throw new Error(
          `The deployment list did not end after ${maxDeploymentListPages} pages`,
        );
      }
      const query = new URLSearchParams();
      if (pageSize) query.set("pageSize", pageSize);
      if (pageToken) query.set("pageToken", pageToken);
      const response = await client.request<DeploymentListResponse>(
        `/v1alpha/deployments${query.size ? `?${query}` : ""}`,
        {
          method: "GET",
        },
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
    return deployments;
  },
} satisfies CommandSpec<NonNullable<DeploymentListResponse["deployments"]>>;

function isRemoval(status: TargetDeliveryStatus): boolean {
  return status.operation === "OPERATION_REMOVE";
}

/**
 * Summarize delivery and removal work separately: `STATE_DELIVERED` means
 * "applied" for a delivery but "gone" for a removal, and a deployment can
 * briefly have both kinds of work recorded (a shrinking placement).
 */
export function deliverySummary(statuses?: TargetDeliveryStatus[]): string {
  if (!statuses || statuses.length === 0) return "NONE";
  // A record without the operation is a delivery.
  const deliveries = statuses.filter((status) => !isRemoval(status));
  const removals = statuses.filter(isRemoval);
  const delivered = deliveries.filter(
    (status) => status.state === "STATE_DELIVERED",
  ).length;
  const removed = removals.filter(
    (status) => status.state === "STATE_DELIVERED",
  ).length;
  const parts: string[] = [];
  if (deliveries.length > 0) {
    parts.push(`${delivered}/${deliveries.length} delivered`);
  }
  if (removals.length > 0) parts.push(`${removed}/${removals.length} removed`);
  return parts.join(", ");
}

function stateLabel(status: TargetDeliveryStatus): string {
  return (status.state ?? "STATE_UNSPECIFIED")
    .replace(/^STATE_/, "")
    .toLowerCase()
    .replaceAll("_", " ");
}

/**
 * A target is worth reporting when its last attempt failed or left a reason.
 * The reason is retained during the retry cycle, so filtering on it too keeps
 * a failure visible while the next attempt is pending.
 */
function needsAttention(status: TargetDeliveryStatus): boolean {
  return (
    (status.state !== undefined && FAILED_STATES.includes(status.state)) ||
    Boolean(status.message)
  );
}

interface DeliveryProblem {
  /** ink-table renders records, so the shape carries a string index signature. */
  [key: string]: string | number;
  Deployment: string;
  Target: string;
  State: string;
  Attempts: number;
  Reason: string;
}

function deliveryProblems(
  deployments: NonNullable<DeploymentListResponse["deployments"]>,
): DeliveryProblem[] {
  return deployments.flatMap((deployment: DeploymentListItem) =>
    (deployment.targetStatuses ?? []).filter(needsAttention).map((status) => ({
      Deployment: deployment.name,
      Target: status.targetId ?? "UNKNOWN",
      // A removal has its own state vocabulary: "failed" alone would read
      // as a failed delivery of the deployment rather than a failed removal.
      State: isRemoval(status)
        ? `remove ${stateLabel(status)}`
        : stateLabel(status),
      Attempts: status.attempts ?? 0,
      Reason: status.message || "NONE",
    })),
  );
}

export function DeploymentListView({
  data: deployments,
}: {
  data: NonNullable<DeploymentListResponse["deployments"]>;
}): React.ReactElement {
  const tableData = deployments.map((deployment) => ({
    Deployment: deployment.name,
    State: deployment.state ?? "UNKNOWN",
    Reconciling: Boolean(deployment.reconciling),
    Created: deployment.createTime ?? "UNKNOWN",
    Updated: deployment.updateTime ?? "UNKNOWN",
    Targets: (deployment.resolvedTargetIds ?? ["NONE"]).join(", "),
    Deliveries: deliverySummary(deployment.targetStatuses),
    "Pause reason": deployment.pauseReason ?? "NONE",
  }));
  const problems = deliveryProblems(deployments);
  return (
    <Box flexDirection="column">
      <Table data={tableData} />
      {problems.length > 0 ? (
        <Box flexDirection="column">
          <Text bold>Deliveries needing attention</Text>
          <Table data={problems} />
        </Box>
      ) : null}
      <Text dimColor>Use --output json or yaml for full details.</Text>
    </Box>
  );
}
