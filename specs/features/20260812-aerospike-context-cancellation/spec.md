# Aerospike Context Cancellation

## Problem

Synchronous record commands do not accept `context.Context`.
Callers that require cancellation currently run each command in another goroutine and stop waiting when the context ends.
The command continues using the connection and its buffers after the caller returns.

## Proposed Behavior

`GetLazyContext` accepts a context and otherwise preserves `GetLazy` behavior.
The earlier of the Aerospike policy deadline and the context deadline bounds network I/O.
Context-aware reads keep the existing retry policy until the context ends.
Explicit context cancellation interrupts blocked network I/O.
An interrupted connection is closed and never returned to the pool.
The command method does not return until its cancellation callback has either been stopped or completed.

## Non-Goals

This prototype does not add context variants for every command.
This prototype does not change existing `GetLazy` semantics.
This prototype does not authorize a fork release or bidder dependency update.
