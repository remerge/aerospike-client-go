# Verification

## Baseline

- Fork commit: `3d8d71dae197fe189ad98574a1c7418b1523d85f`
- Bidder pin: `v8.0.0-20260109100617-3d8d71dae197`
- Initial host: Darwin arm64
- Prototype benchmark host: Native Linux arm64 container with six pinned CPUs

## Current Evidence

- All 52 packages compile with the new API.
- The context coordinator passed 100 normal repetitions and 20 race-enabled repetitions.
- Focused real-server context tests passed 20 normal repetitions and 5 race-enabled repetitions.
- A Linux `netem` test with 100 ms network delay passed 20 of 20 repetitions with a 5 ms context deadline.
- Canceled calls returned in approximately 5 to 7 ms and subsequent reads succeeded.
- The context-aware read measured 63.86 us per operation versus 71.07 us for the current goroutine wrapper at the median.
- The context-aware path allocated 1,121 bytes and 21 objects per operation versus 1,137 bytes and 21 objects for the wrapper.
- A repeated delayed-I/O run exposed and then verified a fix for context error identity being hidden by `MAX_RETRIES_EXCEEDED`.
- Iterative execution has regression coverage for selecting the earlier context deadline.

## Pending Gates

- Run the full Aerospike integration suite in its expected service environment.
- Add equivalent context support for the write commands userdb wraps.
- Run the complete bidder candidate on native Linux amd64 before adoption.

## Decision

Continue this narrow candidate.
It cleared the 5% performance threshold and preserved cancellation and pool-safety invariants.
This candidate is published as a draft PR and has not been merged or deployed.
