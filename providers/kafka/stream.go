package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/artyomturkin/go-stream"
	k "github.com/segmentio/kafka-go"
	"golang.org/x/sync/semaphore"
)

type streamController struct {
	s                  *semaphore.Weighted
	uc                 *stream.WireConfig
	breakOnFormatError bool
	brokers            []string
	topic              string
}

func (s *streamController) GetConsumer(group string) stream.Consumer {
	r := k.NewReader(k.ReaderConfig{
		Brokers:       s.brokers,
		Topic:         s.topic,
		GroupID:       group,
		QueueCapacity: 1,
		RetentionTime: 7 * 24 * time.Hour,
	})
	return &consumer{
		s:                  s.s,
		r:                  r,
		uc:                 s.uc,
		breakOnFormatError: s.breakOnFormatError,
	}
}

func (s *streamController) GetProducer(group string) stream.Producer {
	return &producer{}
}

type consumer struct {
	s                  *semaphore.Weighted
	r                  *k.Reader
	uc                 *stream.WireConfig
	breakOnFormatError bool
}

func (c *consumer) Read(ctx context.Context) (*stream.Message, error) {
	err := c.s.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}

	for {
		m, err := c.r.FetchMessage(ctx)
		if err != nil {
			return nil, err
		}

		msg, err := stream.UnmarshalMessage(m.Value, c.uc)

		if err == nil {
			msg.StreamMeta = m
			return msg, nil
		}

		if c.breakOnFormatError {
			return nil, err
		}
	}
}
func (c *consumer) Ack(ctx context.Context, m *stream.Message) error {
	c.s.Release(1)
	if msg, ok := m.StreamMeta.(k.Message); ok {
		return c.r.CommitMessages(ctx, msg)
	}

	return errors.New("StreamMeta is not formated correctly")
}

func (c *consumer) Nack(context.Context, *stream.Message) error {
	c.s.Release(1)
	return nil
}

func (c *consumer) Close() error {
	return c.r.Close()
}

type producer struct {
	w  *k.Writer
	uc *stream.WireConfig
}

func (p *producer) Publish(ctx context.Context, m *stream.Message) error {
	data, err := stream.MarshalMessage(m, p.uc)
	if err != nil {
		return err
	}

	return p.w.WriteMessages(ctx, k.Message{Value: data})
}
