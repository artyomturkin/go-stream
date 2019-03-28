package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
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

// New create new stream from config
func New(c stream.Config) stream.Stream {
	return &streamController{
		s:                  semaphore.NewWeighted(int64(c.MaxInflightMessages)),
		uc:                 c.WireConfig,
		breakOnFormatError: c.ForwardUnmarshalErrors,
		brokers:            c.Endpoints,
		topic:              c.Topic,
	}
}

// Ensure that streamController implements stream.Stream interface
var _ stream.Stream = (*streamController)(nil)

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
	w := k.NewWriter(k.WriterConfig{
		Brokers: s.brokers,
		Topic:   s.topic,
	})
	return &producer{
		w:  w,
		uc: s.uc,
	}
}

type consumer struct {
	s                  *semaphore.Weighted
	r                  *k.Reader
	uc                 *stream.WireConfig
	breakOnFormatError bool
}

func (c *consumer) Read(ctx context.Context) (*stream.Message, error) {
	log.Printf("[KAFKA] Read - acquire thread")
	err := c.s.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}

	log.Printf("[KAFKA] Read - thread acquired")

	for {
		log.Printf("[KAFKA] Read - read message")

		m, err := c.r.FetchMessage(ctx)
		if err != nil {
			log.Printf("[KAFKA] Read - read error: %v", err)
			return nil, err
		}

		log.Printf("[KAFKA] Read - partition %d", m.Partition)
		msg, err := stream.UnmarshalMessage(m.Value, c.uc)

		if err == nil {
			log.Printf("[KAFKA] Read - forward message: %v", msg)
			msg.StreamMeta = m
			return msg, nil
		}

		log.Printf("[KAFKA] Read - unmarshaling failed: %v", err)
		if c.breakOnFormatError {
			return nil, err
		}

		log.Printf("[KAFKA] Read - skipping")
		err = c.r.CommitMessages(ctx, m)
		if err != nil {
			log.Printf("[KAFKA] Read - skipping failed: %v", err)
			return nil, fmt.Errorf("failed to commit skipped message")
		}
	}
}
func (c *consumer) Ack(ctx context.Context, m *stream.Message) error {
	log.Printf("[KAFKA] [%s] Ack - release thread", m.ID)
	defer c.s.Release(1)
	if msg, ok := m.StreamMeta.(k.Message); ok {
		err := c.r.CommitMessages(ctx, msg)
		if err != nil {
			log.Printf("[KAFKA] [%s] Ack - error: %v", m.ID, err)
			return err
		}
		log.Printf("[KAFKA] [%s] Ack - success", m.ID)
		return nil
	}

	log.Printf("[KAFKA] [%s] Ack - StreamMeta is not formated correctly", m.ID)
	return errors.New("StreamMeta is not formated correctly")
}

func (c *consumer) Nack(context.Context, *stream.Message) error {
	log.Printf("[KAFKA] Nack - release thread")
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
		log.Printf("[KAFKA] [%s] Publish - failed to marshal message: %v", m.ID, err)
		return err
	}

	log.Printf("[KAFKA] [%s] Publish - message: %1000s", m.ID, string(data.([]byte)))
	err = p.w.WriteMessages(ctx, k.Message{Value: data.([]byte)})
	if err != nil {
		log.Printf("[KAFKA] [%s] Publish - failed: %v", m.ID, err)
		return err
	}

	log.Printf("[KAFKA] [%s] Publish - success", m.ID)
	return nil
}

func (p *producer) Close() error {
	return p.w.Close()
}
