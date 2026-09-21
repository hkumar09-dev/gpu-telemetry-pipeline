package constants

import "errors"

var (
	ErrBackpressure = errors.New("partition at capacity")
	ErrTimeout      = errors.New("consume timeout")
	ErrClosed       = errors.New("broker closed")
	ErrUnknownMsg   = errors.New("unknown delivery")
	ErrNotOwner     = errors.New("delivery not owned by consumer")
)
