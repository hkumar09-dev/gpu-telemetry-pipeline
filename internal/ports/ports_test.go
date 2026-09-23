package ports

import (
	"testing"
	"time"
)

func TestDeliveryAndTimeWindowTypes(t *testing.T) {
	d := Delivery{ID: "1", Topic: "t", Key: "k", Payload: []byte("x")}
	if d.ID != "1" || d.Topic != "t" || len(d.Payload) != 1 {
		t.Fatalf("%+v", d)
	}
	if time.Now().IsZero() {
		t.Fatal("clock")
	}
}
