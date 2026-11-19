import type { EnrollmentRollout, HubObservation } from "./api";

/** Which clusters of a hub its enrollment add-on reaches. */
export type RolloutMode = "manual" | "every-cluster" | "placement";

export interface RolloutChoice {
  value: RolloutMode;
  label: string;
  description: string;
}

/** The choices of the enrollment dialog, in the order they are shown. */
export const ROLLOUT_CHOICES: RolloutChoice[] = [
  {
    value: "manual",
    label: "The clusters I enable it for",
    description:
      "The hub's administrator enables the add-on for one cluster at a time.",
  },
  {
    value: "every-cluster",
    label: "Every cluster of this hub",
    description:
      "The hub enables the add-on for every cluster it has, its own cluster included when that is registered with the hub, and for every cluster that joins later.",
  },
  {
    value: "placement",
    label: "The clusters a Placement selects",
    description:
      "The hub enables the add-on for the clusters that a Placement of the hub's administrator selects.",
  },
];

/** The mode the dialog starts with: the hub's last one, or one at a time. */
export function initialRolloutMode(last?: EnrollmentRollout): RolloutMode {
  const known = ROLLOUT_CHOICES.find((choice) => choice.value === last?.mode);
  return known ? known.value : "manual";
}

/** A namespace or object name as Kubernetes accepts it. */
const NAME = /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/;

/**
 * Why the rollout part of the form cannot be sent yet, in words for the
 * person filling it in, or null when it can.
 */
export function rolloutProblem(
  mode: RolloutMode,
  placementNamespace: string,
  placementName: string,
): string | null {
  if (mode !== "placement") return null;
  const namespace = placementNamespace.trim();
  const name = placementName.trim();
  if (!namespace || !name) {
    return "Give the namespace and the name of the Placement on the hub.";
  }
  if (!NAME.test(namespace) || namespace.includes(".") || !NAME.test(name)) {
    return "A namespace and a name use lowercase letters, digits, and '-'.";
  }
  return null;
}

/** The fields of the add-on request that say which clusters it reaches. */
export function rolloutRequest(
  mode: RolloutMode,
  placementNamespace: string,
  placementName: string,
): { rollout: string; placementNamespace?: string; placementName?: string } {
  if (mode !== "placement") return { rollout: mode };
  return {
    rollout: mode,
    placementNamespace: placementNamespace.trim(),
    placementName: placementName.trim(),
  };
}

/** One line that says which clusters an add-on reaches. */
export function rolloutSummary(rollout?: EnrollmentRollout): string {
  switch (rollout?.mode) {
    case "every-cluster":
      return "Every cluster of this hub";
    case "placement":
      return `The clusters that Placement ${rollout.placementNamespace ?? ""}/${rollout.placementName ?? ""} selects`;
    case "manual":
      return "The clusters it is enabled for, one at a time";
    default:
      return "None made for this connection";
  }
}

/** What the hub's page says while no enrollment secret of the hub is valid. */
export const NO_VALID_ADDON =
  "No cluster now: no enrollment secret of this hub is valid";

export interface EnrollmentState {
  /** Which clusters the add-on last made for the hub reaches now. */
  summary: string;
  /** Set when clusters of the hub can no longer repair their enrollment. */
  warning: string | null;
}

/**
 * What a hub's page says about the hub's enrollment add-on. The console
 * does not read the add-on on the hub: this is what the last add-on made
 * here says, and whether the secret it carries is still accepted.
 *
 * An add-on reaches a cluster only while its secret is valid. With no
 * valid secret the summary says so instead of repeating what the last
 * add-on was made for; the hub's last choice stays on its record and is
 * what the dialog starts with.
 *
 * It warns when clusters were enrolled through the hub's add-on and no
 * enrollment secret of the hub is valid: the add-on on the hub then
 * carries a dead secret, as after the hub was disconnected and connected
 * again. A hub nothing was enrolled through gets no warning, whatever its
 * secrets: someone who revoked a secret before using it knows.
 */
export function enrollmentState(
  observation: HubObservation | undefined,
): EnrollmentState {
  const last = observation?.enrollmentRollout;
  const secrets = observation?.enrollmentSecrets ?? 0;
  const dependent = observation?.enrolledThroughHubCount ?? 0;
  if (secrets > 0) {
    // A secret from before the choice was recorded belongs to an add-on
    // that is enabled one cluster at a time: there was no other kind.
    return {
      summary: rolloutSummary(last ?? { mode: "manual" }),
      warning: null,
    };
  }
  const summary = last ? NO_VALID_ADDON : rolloutSummary(undefined);
  if (dependent === 0) return { summary, warning: null };
  return {
    summary,
    warning: `No enrollment secret of this hub is valid any more. ${dependent === 1 ? "The cluster that was" : `The ${dependent} clusters that were`} enrolled through its add-on cannot repair ${dependent === 1 ? "its" : "their"} enrollment, and no other cluster of the hub can enroll, until the add-on is made again (Enroll clusters) and applied to the hub.`,
  };
}
