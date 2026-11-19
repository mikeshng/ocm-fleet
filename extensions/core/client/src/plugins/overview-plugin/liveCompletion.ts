/**
 * Completion of a setup step from what the server holds, not only from
 * the browser's own progress.
 *
 * An onboarding action may declare `completeWhenResourceExists`; the
 * step is then complete when the stored progress says so or when at
 * least one resource of that type exists. A failed read leaves the
 * stored progress to decide, so an unreachable server never marks a
 * step done.
 */

export interface LiveCompletionProperties {
  /** The action's id, keying the browser's own progress. */
  id?: string;
  completeWhenResourceExists?: string;
}

export function completionOf(
  properties: LiveCompletionProperties,
  progress: Record<string, boolean>,
  present: Record<string, boolean>,
): boolean {
  if (properties.id !== undefined && progress[properties.id]) return true;
  const type = properties.completeWhenResourceExists;
  return type !== undefined && present[type] === true;
}

/** The declared resource types, each once. */
export function resourceTypesToCheck(
  properties: LiveCompletionProperties[],
): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const p of properties) {
    const type = p.completeWhenResourceExists;
    if (type === undefined || seen.has(type)) continue;
    seen.add(type);
    out.push(type);
  }
  return out;
}

/** A CEL filter for one resource type; the literal is quoted and escaped. */
export function celResourceTypeFilter(resourceType: string): string {
  return `resourceType == ${JSON.stringify(resourceType)}`;
}
