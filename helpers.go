package stream

import (
	"fmt"
)

// WrapUnmarshalFromBytes convert unmarshaling func to required signature
func WrapUnmarshalFromBytes(f func([]byte, interface{}) error) func(interface{}, interface{}) error {
	return func(data, output interface{}) error {
		if d, ok := data.([]byte); ok {
			return f(d, output)
		}
		return fmt.Errorf("input must be type of []byte instead of %T", data)
	}
}

// WrapMarshalIntoBytes convert marshaling func to required signature
func WrapMarshalIntoBytes(f func(interface{}) ([]byte, error)) func(interface{}) (interface{}, error) {
	return func(data interface{}) (interface{}, error) {
		return f(data)
	}
}
