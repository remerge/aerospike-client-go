package aerospike_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func TestGetLazyContextMatchesGetLazy(t *testing.T) {
	key, keyErr := as.NewKey(*namespace, "context-cancellation", randString(50))
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	expectedMetadata := map[interface{}]interface{}{
		"name":  "expected",
		"count": 42,
		"nested": map[interface{}]interface{}{
			"ok":   true,
			"list": []interface{}{1, "two", false},
		},
	}
	writePolicy := as.NewWritePolicy(0, 0)
	if err := client.Put(writePolicy, key, as.BinMap{"value": "expected", "count": 42, "metadata": expectedMetadata}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.Delete(writePolicy, key)
	})

	policy := as.NewPolicy()
	policy.MaxRetries = 0
	policy.TotalTimeout = time.Second

	want, getErr := client.GetLazy(policy, key)
	if getErr != nil {
		t.Fatal(getErr)
	}
	got, contextErr := client.GetLazyContext(context.Background(), policy, key)
	if contextErr != nil {
		t.Fatal(contextErr)
	}
	if got.Bins["value"] != want.Bins["value"] || got.Bins["count"] != want.Bins["count"] {
		t.Fatalf("record mismatch: got=%v want=%v", got.Bins, want.Bins)
	}

	wantMetadata := unpackLazyMap(t, want.Bins["metadata"])
	gotMetadata := unpackLazyMap(t, got.Bins["metadata"])
	if !reflect.DeepEqual(mapToIfcMap(wantMetadata), mapToIfcMap(expectedMetadata)) {
		t.Fatalf("GetLazy metadata mismatch: got=%#v want=%#v", wantMetadata, expectedMetadata)
	}
	if !reflect.DeepEqual(mapToIfcMap(gotMetadata), mapToIfcMap(expectedMetadata)) {
		t.Fatalf("GetLazyContext metadata mismatch: got=%#v want=%#v", gotMetadata, expectedMetadata)
	}
}

func TestGetLazyContextReturnsContextErrorAndClientRemainsHealthy(t *testing.T) {
	key, keyErr := as.NewKey(*namespace, "context-cancellation", randString(50))
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	writePolicy := as.NewWritePolicy(0, 0)
	if err := client.Put(writePolicy, key, as.BinMap{"value": "healthy"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.Delete(writePolicy, key)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.GetLazyContext(ctx, nil, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}

	record, getErr := client.GetLazyContext(context.Background(), nil, key)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if record.Bins["value"] != "healthy" {
		t.Fatalf("unexpected record after cancellation: %v", record.Bins)
	}
}

func TestGetLazyContextMissMatchesGetLazy(t *testing.T) {
	key, keyErr := as.NewKey(*namespace, "context-cancellation", randString(50))
	if keyErr != nil {
		t.Fatal(keyErr)
	}

	_, wantErr := client.GetLazy(nil, key)
	_, gotErr := client.GetLazyContext(context.Background(), nil, key)
	if !errors.Is(wantErr, as.ErrKeyNotFound) || !errors.Is(gotErr, as.ErrKeyNotFound) {
		t.Fatalf("miss mismatch: got=%v want=%v", gotErr, wantErr)
	}
}

func TestGetLazyContextInterruptsDelayedInFlightIO(t *testing.T) {
	if os.Getenv("AEROSPIKE_CONTEXT_DELAY_TEST") != "1" {
		t.Skip("requires an externally delayed Aerospike network")
	}

	key, keyErr := as.NewKey(*namespace, "context-cancellation", randString(50))
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.TotalTimeout = 2 * time.Second
	if err := client.Put(writePolicy, key, as.BinMap{"value": "delayed"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.Delete(writePolicy, key)
	})

	policy := as.NewPolicy()
	policy.MaxRetries = 0
	policy.TotalTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	began := time.Now()
	_, getErr := client.GetLazyContext(ctx, policy, key)
	elapsed := time.Since(began)
	if !errors.Is(getErr, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", getErr)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("cancellation took %s, expected at most 50ms", elapsed)
	}
	t.Logf("in-flight cancellation returned in %s", elapsed)

	record, getErr := client.GetLazyContext(context.Background(), policy, key)
	if getErr != nil {
		t.Fatalf("client was unhealthy after interrupted command: %v", getErr)
	}
	if record.Bins["value"] != "delayed" {
		t.Fatalf("unexpected record after interrupted command: %v", record.Bins)
	}
}

func unpackLazyMap(t *testing.T, value interface{}) interface{} {
	t.Helper()

	method := reflect.ValueOf(value).MethodByName("UnpackMap")
	if !method.IsValid() {
		t.Fatalf("expected lazy map unpacker, got %T", value)
	}

	results := method.Call(nil)
	if len(results) != 2 {
		t.Fatalf("unexpected UnpackMap result count: %d", len(results))
	}
	if !results[1].IsNil() {
		err, _ := results[1].Interface().(error)
		t.Fatalf("failed to unpack lazy map: %v", err)
	}

	return results[0].Interface()
}
