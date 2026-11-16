package domain

// DeliveryCompletionEvent records the terminal outcome of a single delivery.
// Operation is the delivery cycle the completion belongs to; a workflow
// waiting on one operation must discard completions of the other, since
// a put and a removal of the same target share a delivery ID and can
// both be referenced at a single generation.
type DeliveryCompletionEvent struct {
	DeliveryID DeliveryID
	Generation Generation
	Operation  DeliveryOperation
	Result     DeliveryResult
}

// DeliveryAckedEvent records that a delivery has been acknowledged by
// the addon -- i.e. it has transitioned out of [DeliveryStatePending].
// This is signaled to the workflow so it knows the addon received the
// work and can stop the ack retry loop for this delivery. Operation
// identifies the cycle the acknowledgement belongs to, for the same
// reason as [DeliveryCompletionEvent].
type DeliveryAckedEvent struct {
	DeliveryID DeliveryID
	Generation Generation
	Operation  DeliveryOperation
}

// FulfillmentEvent is the signal delivered to a running reconciliation
// workflow. Exactly one field is non-nil per event instance.
type FulfillmentEvent struct {
	DeliveryAcked     *DeliveryAckedEvent
	DeliveryCompleted *DeliveryCompletionEvent
}
