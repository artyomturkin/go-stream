package stream_test

import (
	"context"
	"errors"
	"sync"

	stream "github.com/artyomturkin/go-stream"
)

type TestStream struct {
	AckResults        map[*stream.Message]error
	PubResults        map[*stream.Message]error
	Messages          []TestMessage
	PublishedMessages []*stream.Message

	WaitForCancel bool
}

var _ stream.Stream = &TestStream{}

func (t *TestStream) GetConsumer(_ string) stream.Consumer {
	return &testStreamConsumerProducer{
		acks:     t.AckResults,
		pubs:     t.PubResults,
		msgs:     t.Messages,
		provider: t,
	}
}

func (t *TestStream) GetProducer(_ string) stream.Producer {
	return &testStreamConsumerProducer{
		acks:     t.AckResults,
		pubs:     t.PubResults,
		msgs:     t.Messages,
		provider: t,
	}
}

type TestMessage struct {
	Message *stream.Message
	Error   error
}

type testStreamConsumerProducer struct {
	sync.Mutex

	acks map[*stream.Message]error
	msgs []TestMessage
	pubs map[*stream.Message]error

	pos      int
	provider *TestStream
}

var _ stream.Consumer = &testStreamConsumerProducer{}

func (t *testStreamConsumerProducer) Ack(_ context.Context, m *stream.Message) error {
	if err, ok := t.acks[m]; ok {
		return err
	}

	// return nil if no error explicitly set, for convinience
	return nil
}

func (t *testStreamConsumerProducer) Nack(_ context.Context, m *stream.Message) error {
	if err, ok := t.acks[m]; ok {
		return err
	}

	// return nil if no error explicitly set, for convinience
	return nil
}

func (t *testStreamConsumerProducer) Read(ctx context.Context) (*stream.Message, error) {
	t.Lock()
	defer t.Unlock()

	if t.pos < len(t.msgs) {
		m := t.msgs[t.pos]
		t.pos++
		return m.Message, m.Error
	}

	if t.provider.WaitForCancel {
		<-ctx.Done()
	}

	return nil, ErrorNoNewMessages
}

func (t *testStreamConsumerProducer) Publish(_ context.Context, m *stream.Message) error {
	t.Lock()
	defer t.Unlock()

	t.provider.PublishedMessages = append(t.provider.PublishedMessages, m)

	if err, ok := t.pubs[m]; ok {
		return err
	}

	// return nil if no error explicitly set, for convinience
	return nil
}

var (
	// ErrorNoNewMessages no more messages can be read from a stream
	ErrorNoNewMessages = errors.New("no more messages in stream")
)
