# Test Design

## Deterministic Tests

- Cancel a context while socket I/O is blocked and verify the I/O wakes promptly.
- Stop the callback after successful work, cancel the context, and verify the connection remains usable.
- Run callback race tests under the race detector.
- Verify a refreshed connection clears interruption state.

## Real Aerospike Tests

- Compare `GetLazy` and `GetLazyContext` for hit, miss, and selected-bin reads.
- Use a canceled context and verify `errors.Is(err, context.Canceled)`.
- Verify a successful read after cancellation uses a healthy connection.
- Benchmark current wrapper, direct policy deadline, and `GetLazyContext` with the same records and concurrency.

## Acceptance

All deterministic and real-server tests pass normally and under the race detector.
Cancellation never returns an interrupted connection to the pool.
The successful context-aware path is materially cheaper than the wrapper after real command work is included.
