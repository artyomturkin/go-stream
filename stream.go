package stream

import (
	"context"

	"github.com/hashicorp/go-hclog"
)

// Stream used to build a Consumer object
type Stream interface {
	GetConsumer(ctx context.Context, group string) Consumer
	GetProducer(ctx context.Context, group string) Producer
}

// Message wraps context and data from stream
type Message struct {
	Context context.Context
	Data    interface{}
}

// Consumer provides read access to a message stream
type Consumer interface {
	Messages() <-chan Message
	Ack(context.Context) error
	Nack(context.Context) error
	Close() error
}

// Producer provides publish access to a message stream
type Producer interface {
	Publish(context.Context, interface{}) error
	Close() error
}

// Config common configuration for streams
type Config struct {
	Endpoints           []string
	Topic               string
	MaxInflightMessages int

	Logger hclog.Logger
}

type streamContextKey string
