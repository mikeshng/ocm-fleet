// When the cluster list is polled, and when polling has stopped.
//
// Polling runs only while a cluster is in a transient state and the last reads have succeeded.
// After three failed silent reads the page stops asking, so a backend that is down is not asked
// every five seconds for ever; the page says so, and a successful read (the notice's Refresh, or
// any other action that reads the list) starts it again.
export const FAILED_POLL_LIMIT = 3;

export function shouldPoll(
  hasTransient: boolean,
  silentFailCount: number,
): boolean {
  return hasTransient && silentFailCount < FAILED_POLL_LIMIT;
}

export function pollingPaused(
  hasTransient: boolean,
  silentFailCount: number,
): boolean {
  return hasTransient && silentFailCount >= FAILED_POLL_LIMIT;
}
