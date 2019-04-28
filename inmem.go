package stream

import (
	"context"
	"sync"
)

// Ensure that InmemStream implements Stream interface
var _ Stream = InmemStream{}

// InmemStream create inmemory stream.
// Created consumer will output elements defined in Message, Acks and Nacks will be stored in Acks and Nacks fields.
// Created producer will store published messages in Message.
type InmemStream struct {
	Messages []interface{}

	Acks  []context.Context
	Nacks []context.Context
}

// GetConsumer create new consumer from inmemory stream
func (i InmemStream) GetConsumer(ctx context.Context, _ string) Consumer {
	return &consumerProducer{
		is:   i,
		ctx:  ctx,
		done: make(chan struct{}),
	}
}

// GetProducer create new producer from inmemory stream
func (i InmemStream) GetProducer(ctx context.Context, _ string) Producer {
	return &consumerProducer{
		is:   i,
		ctx:  ctx,
		done: make(chan struct{}),
	}
}

/////////////   Internal implementation   /////////////

type consumerProducer struct {
	sync.Mutex

	is   InmemStream
	ctx  context.Context
	done chan struct{}
}

var _ Consumer = &consumerProducer{}
var _ Producer = &consumerProducer{}

func (t *consumerProducer) Close() error {
	return nil
}

func (t *consumerProducer) Ack(ctx context.Context) error {
	t.Lock()
	defer t.Unlock()

	t.is.Acks = append(t.is.Acks, ctx)
	return nil
}

func (t *consumerProducer) Nack(ctx context.Context) error {
	t.Lock()
	defer t.Unlock()

	t.is.Nacks = append(t.is.Nacks, ctx)
	return nil
}

func (t *consumerProducer) Messages() <-chan Message {
	ch := make(chan Message)
	go func() {
		for i, m := range t.is.Messages {
			ch <- Message{Data: m, Context: SetTrackers(t.ctx, i)}
		}
		close(ch)
		close(t.done)
	}()
	return ch
}

func (t *consumerProducer) Publish(_ context.Context, m interface{}) error {
	t.Lock()
	defer t.Unlock()

	t.is.Messages = append(t.is.Messages, m)
	return nil
}

func (t *consumerProducer) Errors() <-chan error {
	return make(chan error)
}

func (t *consumerProducer) Done() <-chan struct{} {
	return t.done
}
