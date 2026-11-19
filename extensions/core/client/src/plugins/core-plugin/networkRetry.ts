// Runs a read and, when it fails before reaching the server, runs it once more.
//
// A fetch that never reached the server rejects with a TypeError ("Failed to fetch" and the
// like); nothing was processed, so one more attempt cannot change what the server holds. An
// answer from the server -- including a refusal -- is not a network failure: it is returned (or
// thrown) as it is, because asking again would get the same answer.
export async function withNetworkRetry<T>(
  run: () => Promise<T>,
  delayMs = 500,
): Promise<T> {
  try {
    return await run();
  } catch (error) {
    if (!(error instanceof TypeError)) throw error;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    return run();
  }
}
