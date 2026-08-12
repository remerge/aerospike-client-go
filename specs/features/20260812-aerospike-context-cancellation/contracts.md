# Contracts

## Public API

```go
func (clnt *Client) GetLazyContext(
    ctx context.Context,
    policy *BasePolicy,
    key *Key,
    binNames ...string,
) (*Record, error)
```

Passing a nil context behaves like `context.Background()`.
The earlier of the policy deadline and the context deadline bounds network I/O.
Context-aware reads continue to use the existing retry policy until the context ends.
A context error is wrapped in an Aerospike timeout error and remains discoverable through `errors.Is`.
Cancellation is checked immediately before and after connection acquisition.
Connection acquisition itself is not interruptible, while subsequent blocked socket I/O is interrupted by cancellation.

## Connection Contract

The cancellation callback may set the active socket deadline to the current time.
The callback must not close the connection because closing releases connection-owned buffers.
The command goroutine waits for the callback and then closes the interrupted connection.
A completed command stops its callback before returning the connection to the pool.
Temporary command buffers borrowed during resize or compression are released before close or reuse.

## Compatibility Contract

Existing non-context APIs are unchanged.
Successful `GetLazyContext` records must match `GetLazy` records for the same policy, key, and bin selection.
Iterative command execution uses the same policy-and-context deadline selection when a future command supplies a context.
