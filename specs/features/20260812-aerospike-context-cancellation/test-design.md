# Test Design

## Deterministic Tests

- Cancel before send and during buffer preparation and verify the clean connection returns to the pool.
- Cancel a context while a blocked read or final response read is in socket I/O and verify the read wakes promptly.
- Verify interruption only wins while socket I/O is active and a late callback after completion leaves the connection reusable.
- Stop the callback after successful work, cancel the context, and verify the connection remains usable.
- Force a deadline race at the retry limit and verify the returned error still matches the context cause.
- Verify iterative execution selects the earlier context deadline when a command supplies a context.
- Resize command buffers across multiple allocations and verify cleanup releases every borrowed buffer.
- Run callback race tests under the race detector.
- Verify a refreshed connection clears interruption state.

## Real Aerospike Tests

- Compare `GetLazy` and `GetLazyContext` for hit and miss reads.
- Use a canceled context and verify `errors.Is(err, context.Canceled)`.
- Use delayed I/O with a short context deadline and verify `errors.Is(err, context.DeadlineExceeded)`.
- Verify a successful read after cancellation uses a healthy connection.
- Benchmark current wrapper, direct policy deadline, and `GetLazyContext` with the same records and concurrency.

## Acceptance

All deterministic and real-server tests pass normally and under the race detector.
Cancellation never returns an interrupted connection to the pool.
The successful context-aware path is materially cheaper than the wrapper after real command work is included.
