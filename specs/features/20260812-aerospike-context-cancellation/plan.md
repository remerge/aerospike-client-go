# Implementation Plan

1. Add context state to single-command execution.
2. Use the earlier policy or context deadline for the command.
3. Register a cancellation callback only while a connection is owned by the command.
4. Coordinate callback completion before closing or pooling the connection.
5. Add the `GetLazyContext` public API.
6. Validate deterministic races and real Aerospike behavior.
7. Benchmark against the production-pinned wrapper.
