/**
 * Deleting a provisioned cluster terminates a real cluster, and the rows it is reached from
 * stand close together. The confirmation therefore asks for the cluster's name: the Delete
 * button stays disabled until this matches. Surrounding whitespace is ignored (a name pasted
 * with a stray space still confirms); everything else, including case, must match.
 */
export function deleteConfirmationMatches(
  typed: string,
  clusterId: string,
): boolean {
  return typed.trim() === clusterId;
}
