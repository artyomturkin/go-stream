# Stream processing for Go

Common stream abstraction and implementations for stream providers.

Importing into a project
```sh
go get guthub.com/artyomturkin/go-stream
```

## Abstractions

### Streams
Common structure and configuration for creating streams from providers, consumer and producer interfaces and message structure.

```go
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
	Close() error
}

// Producer provides publish access to a message stream
type Producer interface {
	Publish(context.Context, *Message) error
	Close() error
}

// Message stream message
type Message struct {
	ID          string      `json:"id"`
	Type        string      `json:"type"`
	Time        time.Time   `json:"time"`
	Source      string      `json:"source"`
	Data        interface{} `json:"data"`
	ContentType string      `json:"contenttype"`

	StreamMeta interface{} `json:"-"`
}
```

### Pipelines

Stream processing abstractions. Pipeline consists of one input stream, functions to modify and/or filter messages and one output stream? if any.

Example:
```go
// create pipeline
p := stream.NewPipeline("pipeline-name").From(source).Do(forward, forward).Do(forward).To(output).Build(ctx)
```

To drop message in a filter, filter func should return nil instead of message.

## Providers

### Kafka

Importing 
```sh
go get github.com/artyomturkin/go-stream/providers/kafka
```

Create a new stream:
```go
stream := kafka.New(config)
```