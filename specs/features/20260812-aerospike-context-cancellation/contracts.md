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
A context error is wrapped in an Aerospike timeout error and remains discoverable through `errors.Is`.

## Connection Contract

The cancellation callback may set the active socket deadline to the current time.
The callback must not close the connection because closing releases connection-owned buffers.
The command goroutine waits for the callback and then closes the interrupted connection.
A completed command stops its callback before returning the connection to the pool.

## Compatibility Contract

Existing non-context APIs are unchanged.
Successful `GetLazyContext` records must match `GetLazy` records for the same policy, key, and bin selection.
