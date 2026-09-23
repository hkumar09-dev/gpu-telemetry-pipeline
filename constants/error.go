package constants

import "errors"

var (
	ErrBackpressure   = errors.New("partition at capacity")
	ErrTimeout        = errors.New("consume timeout")
	ErrClosed         = errors.New("broker closed")
	ErrUnknownMsg     = errors.New("unknown delivery")
	ErrNotOwner       = errors.New("delivery not owned by consumer")
	ErrUnavailable    = errors.New("unavailable")
	ErrUnknownGPU     = errors.New("gpu not found")
	ErrInvalidTime    = errors.New("invalid time filter")
	ErrInvalidPayload = errors.New("invalid telemetry")
)
