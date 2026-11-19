package domain

import "time"

// DeliveryCompletion is a completion that a delivery reported but whose
// fulfillment workflow has not been told yet.
//
// A delivery's terminal state and this record are written in one
// transaction (see [DeliveryReporter.ReportResult]), so a completion can
// never be lost to a crash between the state change and the signal. A
// relay reads outstanding records, signals the completion, and removes
// the record on success; a fulfillment whose workflow instance is no
// longer running counts as delivered, because no workflow can observe
// the signal (see [ErrFulfillmentNotRunning]).
//
// Only the fields the workflow reads are kept: the delivered state and
// the message explaining it. Structured outputs (provisioned targets and
// produced secrets) were committed with the delivery itself; they are
// not repeated here.
type DeliveryCompletion struct {
	DeliveryID    DeliveryID
	FulfillmentID FulfillmentID
	Generation    Generation
	Operation     DeliveryOperation
	State         DeliveryState
	Message       string
	CreatedAt     time.Time
}

// CompletionEvent returns the fulfillment event that reports this
// completion to the delivery's workflow.
func (c DeliveryCompletion) CompletionEvent() FulfillmentEvent {
	return FulfillmentEvent{DeliveryCompleted: &DeliveryCompletionEvent{
		DeliveryID: c.DeliveryID,
		Generation: c.Generation,
		Operation:  c.Operation,
		Result: DeliveryResult{
			State:     c.State,
			Message:   c.Message,
			Operation: c.Operation,
		},
	}}
}
