package stream

import (
	"context"
	"time"
)

// Stream used to build a Consumer object
type Stream interface {
	GetConsumer(group string) Consumer
	GetProducer(group string) Producer
}

// Consumer provides read access to a message stream
type Consumer interface {
	Read(context.Context) (*Message, error)
	Ack(context.Context, *Message) error
	Nack(context.Context, *Message) error
}

// Producer provides publish access to a message stream
type Producer interface {
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
