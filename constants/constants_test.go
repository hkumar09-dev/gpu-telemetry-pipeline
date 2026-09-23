package constants

import "testing"

func TestConstants(t *testing.T) {
	if MAX_RETRY_ATTEMPTS <= 0 {
		t.Fatal("retries")
	}
	if INITIAL_BACKOFF <= 0 || MAX_BACKOFF <= INITIAL_BACKOFF {
		t.Fatal("backoff")
	}
	if TOPIC == "" {
		t.Fatal("topic")
	}
	if PersistInterval <= 0 || MaxStoreBytes <= 0 {
		t.Fatal("store")
	}
}

func TestErrors(t *testing.T) {
	for _, err := range []error{ErrBackpressure, ErrTimeout, ErrClosed, ErrUnknownMsg, ErrNotOwner} {
		if err == nil || err.Error() == "" {
			t.Fatalf("empty error %+v", err)
		}
	}
}
