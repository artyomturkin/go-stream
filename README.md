# Stream abstraction for Go

Common stream abstraction and in-memory implementation for streaming data.

Importing into a project
```sh
go get guthub.com/artyomturkin/go-stream
```

## Abstractions

### Stream
Common structure and configuration for creating streams from providers, consumer and producer interfaces and message structure.

```go
// Stream used to build a Consumer object
type Stream interface {
	GetConsumer(ctx context.Context, group string) Consumer
	GetProducer(ctx context.Context, group string) Producer
}
```
```go
// Message wraps context and data from stream
type Message struct {
	Context context.Context
	Data    interface{}
}
```
```go
// Consumer provides read access to a message stream
type Consumer interface {
	Messages() <-chan Message
	Ack(context.Context) error
	Nack(context.Context) error
	Close() error
	Errors() <-chan error
}
```
```go
// Producer provides publish access to a message stream
type Producer interface {
	Publish(context.Context, interface{}) error
	Close() error
	Errors() <-chan error
}
```
```go
// Config common configuration for streams
type Config struct {
	Endpoints           []string
	Topic               string
	MaxInflightMessages int
	Custom              interface{}
}
```

### Helper funcs

Functions to set and get tracking information from context
```go
// SetTrackers adds message trackers to context
func SetTrackers(ctx context.Context, tracker ...interface{}) context.Context
```
```go
// GetTrackers returns an array of trackers
func GetTrackers(ctx context.Context) []interface{}
```
