package stream_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	stream "github.com/artyomturkin/go-stream"
)

type TestStream struct {
	AckResults        map[*stream.Message]error
	PubResults        map[*stream.Message]error
	Messages          []TestMessage
	PublishedMessages []*stream.Message

	BlockAfterAllMessagesRead bool
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

func (t *testStreamConsumerProducer) Close() error {
	return nil
}

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

	if t.provider.BlockAfterAllMessagesRead {
		<-ctx.Done()
		return nil, ErrorStreamCanceled
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
	ErrorNoNewMessages  = errors.New("no more messages in stream")
	ErrorStreamCanceled = errors.New("stream canceled")
)

func TestUnmarshalMessage_Raw_Success(t *testing.T) {
	ti := time.Now()
	msg := map[string]interface{}{
		"id":   "identity",
		"time": ti,
		"msg":  "hello world",
	}

	d, _ := json.Marshal(msg)

	c := &stream.WireConfig{
		UnmarshalF: stream.WrapUnmarshalFromBytes(json.Unmarshal),
		IDField:    "id",
		TimeField:  "time",
		RawData:    true,
		Source:     "test",
		Type:       "test",
	}

	m, err := stream.UnmarshalMessage(d, c)
	if err != nil {
		t.Fatalf("failed to unamrshal msg: %v", err)
	}

	if m.ID != "identity" {
		t.Error("ID header does not expected value")
	}
}

func TestUnmarshalMessage_Message_Success(t *testing.T) {
	ti := time.Now()
	msg := &stream.Message{
		Data: map[string]interface{}{
			"time": ti,
			"msg":  "hello world",
		},
		ID:     "identity",
		Time:   ti,
		Source: "test",
		Type:   "test",
	}

	d, _ := json.Marshal(msg)

	c := &stream.WireConfig{
		UnmarshalF: stream.WrapUnmarshalFromBytes(json.Unmarshal),
	}

	m, err := stream.UnmarshalMessage(d, c)
	if err != nil {
		t.Fatalf("failed to unamrshal msg: %v", err)
	}

	if m.ID != "identity" {
		t.Error("ID header does not expected value")
	}
}

func TestUnmarshalMessage_Raw_Fail(t *testing.T) {
	msg := map[string]interface{}{
		"msg": "hello world",
	}

	d, _ := json.Marshal(msg)

	c := &stream.WireConfig{
		UnmarshalF: stream.WrapUnmarshalFromBytes(json.Unmarshal),
		IDField:    "id",
		TimeField:  "time",
		RawData:    true,
		Source:     "test",
		Type:       "test",
	}

	_, err := stream.UnmarshalMessage(d, c)
	if err == nil {
		t.Fatalf("expected an error")
	}

	if _, ok := err.(stream.ErrorMessageFormat); !ok {
		t.Errorf("Expected format error, instead got: %+v", err)
	}
}

func TestUnmarshalMessage_Message_Fail(t *testing.T) {
	ti := time.Now()
	msg := &stream.Message{
		Data: map[string]interface{}{
			"time": ti,
			"msg":  "hello world",
		},
		ID:     "identity",
		Time:   ti,
		Source: "test",
	}

	d, _ := json.Marshal(msg)

	c := &stream.WireConfig{
		UnmarshalF: stream.WrapUnmarshalFromBytes(json.Unmarshal),
	}

	_, err := stream.UnmarshalMessage(d, c)
	if err == nil {
		t.Fatalf("expected an error")
	}

	if _, ok := err.(stream.ErrorMessageFormat); !ok {
		t.Errorf("Expected format error, instead got: %+v", err)
	}
}

func TestMarshal_Raw_Success(t *testing.T) {
	ti := time.Now()

	m := &stream.Message{
		Data: map[string]interface{}{
			"time": ti,
			"msg":  "hello world",
		},
		ID:     "identity",
		Time:   ti,
		Source: "test",
		Type:   "test",
	}

	wc := &stream.WireConfig{
		MarshalF:    stream.WrapMarshalIntoBytes(json.Marshal),
		ContentType: "application/json",
		RawData:     true,
	}

	d, _ := stream.MarshalMessage(m, wc)

	var r map[string]interface{}
	json.Unmarshal(d.([]byte), &r)

	if r["msg"] != "hello world" {
		t.Errorf("unexpected msg: %v", r)
	}
}

func TestMarshal_Message_Success(t *testing.T) {
	ti := time.Now()

	m := &stream.Message{
		Data: map[string]interface{}{
			"time": ti,
			"msg":  "hello world",
		},
		ID:     "identity",
		Time:   ti,
		Source: "test",
		Type:   "test",
	}

	wc := &stream.WireConfig{
		MarshalF:    stream.WrapMarshalIntoBytes(json.Marshal),
		ContentType: "application/json",
	}

	d, _ := stream.MarshalMessage(m, wc)

	var r map[string]interface{}
	json.Unmarshal(d.([]byte), &r)

	if r["id"] != "identity" {
		t.Errorf("unexpected msg: %v", r)
	}
}
