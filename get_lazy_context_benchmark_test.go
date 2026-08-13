package aerospike_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func BenchmarkGetLazyContextPaths(b *testing.B) {
	key, err := as.NewKey(*namespace, "context-benchmark", randString(50))
	if err != nil {
		b.Fatal(err)
	}
	writePolicy := as.NewWritePolicy(0, 0)
	if err := client.Put(writePolicy, key, as.BinMap{"value": "benchmark", "count": 42}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_, _ = client.Delete(writePolicy, key)
	})

	policy := as.NewPolicy()
	policy.MaxRetries = 0
	policy.TotalTimeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	b.Cleanup(cancel)

	b.Run("current_wrapper", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			done := make(chan struct{})
			var record *as.Record
			var operationErr error
			go func() {
				record, operationErr = client.GetLazy(policy, key)
				close(done)
			}()
			select {
			case <-ctx.Done():
				b.Fatal(ctx.Err())
			case <-done:
			}
			if operationErr != nil {
				b.Fatal(operationErr)
			}
			runtime.KeepAlive(record)
		}
	})

	b.Run("direct_policy_deadline", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			record, err := client.GetLazy(policy, key)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(record)
		}
	})

	b.Run("context_aware", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			record, err := client.GetLazyContext(ctx, policy, key)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(record)
		}
	})
}
