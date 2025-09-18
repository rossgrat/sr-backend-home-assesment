package packer

import (
	"context"
	"errors"
	k "sr-backend-home-assessment/internal/kafka"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func Test_ProcessMessage(t *testing.T) {
	cases := []struct {
		name        string
		setupReader func(kafka.Message) k.Reader
		setupWriter func(kafka.Message) k.Writer
		inputMsg    kafka.Message
		expectedErr error
	}{
		{
			name: "valid message",
			inputMsg: kafka.Message{
				Key:   []byte("device123"),
				Value: []byte("payload"),
			},
			setupReader: func(msg kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(msg, nil)
				return r
			},
			setupWriter: func(msg kafka.Message) k.Writer {
				w := k.NewMockWriter(t)
				w.EXPECT().WriteMessages(
					mock.Anything,
					[]kafka.Message{msg},
				).Return(nil)
				return w
			},
			expectedErr: nil,
		},
		{
			name: "reader failed",
			inputMsg: kafka.Message{
				Key:   []byte("device123"),
				Value: []byte("payload"),
			},
			setupReader: func(msg kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(msg, errors.New("failed to read"))
				return r
			},
			setupWriter: func(msg kafka.Message) k.Writer {
				return k.NewMockWriter(t)
			},
			expectedErr: ErrReadMessage,
		},
		{
			name: "writer failed",
			inputMsg: kafka.Message{
				Key:   []byte("device123"),
				Value: []byte("payload"),
			},
			setupReader: func(msg kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(msg, nil)
				return r
			},
			setupWriter: func(msg kafka.Message) k.Writer {
				w := k.NewMockWriter(t)
				w.EXPECT().WriteMessages(
					mock.Anything,
					[]kafka.Message{msg},
				).Return(errors.New("failed to write"))
				return w
			},
			expectedErr: ErrWriteMessage,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			packer := &Packer{
				reader: tt.setupReader(tt.inputMsg),
				writer: tt.setupWriter(tt.inputMsg),
			}
			err := packer.ProcessMessage(context.Background())
			assert.ErrorIs(t, err, tt.expectedErr)
		})
	}
}
