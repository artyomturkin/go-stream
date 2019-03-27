package stream

import (
	"context"
	"errors"
	"fmt"
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

// ErrorMessageFormat message does not match expected format
type ErrorMessageFormat struct {
	internal error
}

// Error return error text
func (e ErrorMessageFormat) Error() string {
	return fmt.Sprintf("Message is formatted incorrectly: %v", e.internal)
}

// Internal get internal error
func (e ErrorMessageFormat) Internal() error {
	return e.internal
}

// NewErrorMessageFormat create new error
func NewErrorMessageFormat(internal error) ErrorMessageFormat {
	return ErrorMessageFormat{
		internal: internal,
	}
}

// Config common configuration for streams
type Config struct {
	Endpoints           []string
	Topic               string
	MaxInflightMessages int

	WireConfig             *WireConfig
	ForwardUnmarshalErrors bool
}

// WireConfig configuration for message unmarshaling
type WireConfig struct {
	UnmarshalF  func(interface{}, interface{}) error
	MarshalF    func(interface{}) (interface{}, error)
	RawData     bool
	ContentType string

	IDField   string
	TimeField string
	Type      string
	Source    string
}

// UnmarshalMessage convert byte array to message, extracting headers if needed
func UnmarshalMessage(data interface{}, c *WireConfig) (*Message, error) {
	var msg Message

	if !c.RawData {
		err := c.UnmarshalF(data, &msg)
		if err != nil {
			return nil, NewErrorMessageFormat(err)
		}
	} else {
		var d map[string]interface{}
		err := c.UnmarshalF(data, &d)
		if err != nil {
			return nil, NewErrorMessageFormat(err)
		}

		var id string
		var t time.Time

		vErr := ""
		if s, ok := d[c.IDField].(string); !ok {
			vErr = "failed to get ID;"
		} else {
			id = s
		}

		if s, ok := d[c.TimeField].(string); !ok {
			vErr += "failed to get Time"
		} else if ti, err := time.Parse(time.RFC3339, s); err != nil {
			vErr += err.Error()
		} else {
			t = ti
		}

		if vErr != "" {
			return nil, NewErrorMessageFormat(errors.New(vErr))
		}

		msg = Message{
			Type:   c.Type,
			Source: c.Source,
			Time:   t,
			ID:     id,
			Data:   d,
		}
	}

	if (msg.Time == time.Time{} || msg.Source == "" || msg.Type == "" || msg.ID == "") {
		return nil, NewErrorMessageFormat(errors.New("not all message headers set"))
	}

	return &msg, nil
}

// MarshalMessage Convert message to bytes
func MarshalMessage(m *Message, wc *WireConfig) (interface{}, error) {
	var d interface{}
	if wc.RawData {
		d = m.Data
	} else {
		m.ContentType = wc.ContentType
		d = m
	}

	data, err := wc.MarshalF(d)
	if err != nil {
		return nil, err
	}

	return data, nil
}
