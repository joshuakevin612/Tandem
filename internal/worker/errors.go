package worker

import "errors"

// ErrPermanent matches (via errors.Is) any error wrapped by Permanent.
var ErrPermanent = errors.New("permanent failure")

type permanentError struct{ err error }

func (e *permanentError) Error() string        { return e.err.Error() }
func (e *permanentError) Unwrap() error        { return e.err }
func (e *permanentError) Is(target error) bool { return target == ErrPermanent }

// Permanent marks err as non-retryable: the job goes straight to dead.
// Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// IsPermanent reports whether err (or anything it wraps) was marked Permanent.
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }

// Cancellation causes for a handler context; see context.Cause.
var (
	// ErrLeaseLost: the heartbeat found the job no longer owned by this worker.
	ErrLeaseLost = errors.New("worker: lease lost")
	// ErrLeaseUncertain: heartbeats kept failing for a full lease, so ownership
	// can no longer be assumed.
	ErrLeaseUncertain = errors.New("worker: heartbeat failing, lease ownership unknown")
)
