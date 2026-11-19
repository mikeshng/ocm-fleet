import type {
  MgmtDeployment,
  TargetDeliveryOperation,
  TargetDeliveryState,
  TargetDeliveryStatus,
} from "./api";

export interface TargetDeployment {
  name: string;
  /** Delivery status of this deployment on the target, when reported. */
  status?: TargetDeliveryStatus;
}

/** Whether the recorded work removes the manifests instead of applying them. */
export function isRemoval(status?: TargetDeliveryStatus): boolean {
  return status?.operation === "OPERATION_REMOVE";
}

/** True once a removal finished: the manifests are no longer on the target. */
export function isRemovalCompleted(status?: TargetDeliveryStatus): boolean {
  return isRemoval(status) && status?.state === "STATE_DELIVERED";
}

/**
 * Group the deployments known to each target, merging each deployment's
 * delivery status on that target. Placement can name a target before the
 * first delivery record exists, so both sources seed the result.
 */
export function deploymentsByTarget(
  deployments: readonly MgmtDeployment[],
): Map<string, TargetDeployment[]> {
  const byTarget = new Map<string, Map<string, TargetDeployment>>();
  const deploymentFor = (targetId: string, name: string): TargetDeployment => {
    let perDeployment = byTarget.get(targetId);
    if (!perDeployment) {
      perDeployment = new Map();
      byTarget.set(targetId, perDeployment);
    }
    let entry = perDeployment.get(name);
    if (!entry) {
      entry = { name };
      perDeployment.set(name, entry);
    }
    return entry;
  };

  for (const deployment of deployments) {
    for (const targetId of deployment.resolvedTargetIds ?? []) {
      deploymentFor(targetId, deployment.name);
    }
    for (const status of deployment.targetStatuses ?? []) {
      deploymentFor(status.targetId, deployment.name).status = status;
    }
  }

  const grouped = new Map<string, TargetDeployment[]>();
  for (const [targetId, perDeployment] of byTarget) {
    grouped.set(
      targetId,
      Array.from(perDeployment.values()).sort((a, b) =>
        a.name.localeCompare(b.name),
      ),
    );
  }
  return grouped;
}

export function deliveryColor(
  state?: TargetDeliveryState,
  operation?: TargetDeliveryOperation,
): "green" | "red" | "orange" | "blue" | "grey" {
  switch (state) {
    case "STATE_DELIVERED":
      // A completed removal means the manifests are gone: neutral, not the
      // green that marks an active delivery.
      return operation === "OPERATION_REMOVE" ? "grey" : "green";
    case "STATE_FAILED":
    case "STATE_PARTIAL":
    case "STATE_AUTH_FAILED":
      return "red";
    case "STATE_ACCEPTED":
    case "STATE_PROGRESSING":
      return "blue";
    default:
      return "grey";
  }
}

export function deliveryStateLabel(
  state?: TargetDeliveryState,
  operation?: TargetDeliveryOperation,
): string {
  const removing = operation === "OPERATION_REMOVE";
  if (removing && state === "STATE_DELIVERED") {
    return "removed";
  }
  if (!state || state === "STATE_UNSPECIFIED") {
    return removing ? "removal pending" : "pending";
  }
  const label = state.replace("STATE_", "").toLowerCase().replaceAll("_", " ");
  return removing ? `removal ${label}` : label;
}
