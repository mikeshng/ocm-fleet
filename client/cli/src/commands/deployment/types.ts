export type DeploymentState =
  | "STATE_UNSPECIFIED"
  | "STATE_CREATING"
  | "STATE_ACTIVE"
  | "STATE_DELETING"
  | "STATE_FAILED";

/** Delivery lifecycle state of one target. */
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

/** Delivery state of a deployment on one target. */
export interface TargetDeliveryStatus {
  /** Target the status refers to. */
  targetId?: string;
  /** Current delivery state on the target. */
  state?: TargetDeliveryState;
  /**
   * Whether the recorded work applies the manifests to the target or
   * removes them from it. A missing value is treated as a delivery.
   */
  operation?: TargetDeliveryOperation;
  /**
   * Reason the last delivery attempt failed. Retained across retries and
   * cleared once an attempt delivers.
   */
  message?: string;
  /** Failed delivery attempts at the current generation. */
  attempts?: number;
  updateTime?: string;
}

/** JSON shape returned by DeploymentService.ListDeployments. */
export interface DeploymentListItem {
  name: string;
  state?: DeploymentState;
  reconciling?: boolean;
  resolvedTargetIds?: string[];
  createTime?: string;
  updateTime?: string;
  pauseReason?: string;
  /** Delivery state on each resolved target, ordered by target ID. */
  targetStatuses?: TargetDeliveryStatus[];
}

export interface DeploymentListResponse {
  deployments?: DeploymentListItem[];
  nextPageToken?: string;
}
