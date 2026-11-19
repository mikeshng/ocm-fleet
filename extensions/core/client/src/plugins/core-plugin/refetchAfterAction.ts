// Reads a record again after an action changed it, and reports when the read fails.
//
// The action's own answer is not the page's state: the record is read again, and only that read
// says what the page should show. When it fails, the action did happen, so the page keeps what it
// has; the person is told that the read failed and that a reload shows the new state, instead of
// being left with a row that looks current and is not.
export async function refetchAfterAction(
  refresh: () => Promise<boolean>,
  reportFailure: (message: string) => void,
  failureMessage: string,
): Promise<void> {
  if (!(await refresh())) reportFailure(failureMessage);
}
