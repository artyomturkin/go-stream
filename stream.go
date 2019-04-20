package stream

import (
	"context"
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

	Custom interface{}
}

// TrackedMessagesContextKey context key to get tracked messages from context. Tracked messages are stored in []interface{}
const TrackedMessagesContextKey = streamContextKey("TRACKED_MESSAGES")

// SetTrackers adds message trackers to context
func SetTrackers(ctx context.Context, tracker ...interface{}) context.Context {
	tracks := GetTrackers(ctx)

	for _, t := range tracker {
		tracks = append(tracks, t)
	}

	return context.WithValue(ctx, TrackedMessagesContextKey, tracks)
}

// GetTrackers returns an array of trackers
func GetTrackers(ctx context.Context) []interface{} {
	if a, ok := ctx.Value(TrackedMessagesContextKey).([]interface{}); ok {
		return a
	}
	return []interface{}{}
}

type streamContextKey string
