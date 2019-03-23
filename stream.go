package stream

import (
	"context"
	"time"
)

// Provider used to build a Stream object
type Provider interface {
	GetStreamFor(group string) Stream
}

// Stream provides access to a single message stream
type Stream interface {
	Read(context.Context) (*Message, error)
	Ack(context.Context, *Message) error
	Nack(context.Context, *Message) error
	Publish(context.Context, *Message) error
}

// Message stream message
type Message struct {
	ID     string
	Type   string
	Time   time.Time
	Source string
	Data   interface{}
}
