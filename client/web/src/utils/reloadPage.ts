/** The little of a window that reloading needs; tests pass a stub. */
export interface ReloadableWindow {
  location: { reload: () => void };
}

/**
 * Reload the console's page. The error states offer this instead of leaving the viewer at a
 * dead end: a page that could not load one of its code files stays broken until the browser
 * loads the page again, and the browser keeps the sign-in.
 */
export function reloadPage(target: ReloadableWindow = window): void {
  target.location.reload();
}
