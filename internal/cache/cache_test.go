package cache

import (
	"context"
	"encoding/json"
	"errors"
	k "sr-backend-home-assessment/internal/kafka"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_ReadMessage(t *testing.T) {
	cases := []struct {
		name          string
		setupReader   func(kafka.Message) k.Reader
		inputDeviceID string
		outputMessage func(string) kafka.Message
		expectedError error
		expectedDone  bool
		expectedState DeviceState
	}{
		{
			name: "happy path - read timeout",
			setupReader: func(outputMessage kafka.Message) k.Reader {
				reader := k.NewMockReader(t)
				reader.EXPECT().ReadMessage(mock.Anything).Return(
					outputMessage,
					context.DeadlineExceeded,
				)
				return reader
			},
			inputDeviceID: "test123",
			outputMessage: func(deviceID string) kafka.Message {
				return kafka.Message{}
			},
			expectedError: nil,
			expectedDone:  true,
			expectedState: DeviceState{},
		},
		{
			name: "happy path - lag is zero",
			setupReader: func(outputMessage kafka.Message) k.Reader {
				reader := k.NewMockReader(t)
				reader.EXPECT().ReadMessage(mock.Anything).Return(
					outputMessage,
					nil,
				)
				reader.EXPECT().Lag().Return(int64(0))
				return reader
			},
			inputDeviceID: "test123",
			outputMessage: func(deviceID string) kafka.Message {
				record := k.StructuredConnectRecord{
					Payload: k.DeviceEvent{
						DeviceID:  deviceID,
						Timestamp: 12,
						EventType: "on",
					},
				}
				recordBytes, _ := json.Marshal(record)
				msg := kafka.Message{
					Key:   []byte(deviceID),
					Value: recordBytes,
				}
				return msg
			},
			expectedError: nil,
			expectedDone:  true,
			expectedState: DeviceState{
				LastTimestampSeen: 12,
				LastEvent:         "on",
			},
		},
		{
			name: "json unmarshal failed",
			setupReader: func(outputMessage kafka.Message) k.Reader {
				reader := k.NewMockReader(t)
				reader.EXPECT().ReadMessage(mock.Anything).Return(
					outputMessage,
					nil,
				)
				return reader
			},
			inputDeviceID: "test123",
			outputMessage: func(deviceID string) kafka.Message {
				msg := kafka.Message{
					Key:   []byte(deviceID),
					Value: []byte("not-a-json"),
				}
				return msg
			},
			expectedError: ErrParseMessage,
			expectedDone:  false,
			expectedState: DeviceState{},
		},
		{
			name: "read message failed",
			setupReader: func(outputMessage kafka.Message) k.Reader {
				reader := k.NewMockReader(t)
				reader.EXPECT().ReadMessage(mock.Anything).Return(
					outputMessage,
					errors.New("failed"),
				)
				return reader
			},
			outputMessage: func(deviceID string) kafka.Message {
				return kafka.Message{}
			},
			expectedError: ErrReadMessage,
			expectedDone:  false,
			expectedState: DeviceState{},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cache := &StateCache{
				reader: tt.setupReader(tt.outputMessage(tt.inputDeviceID)),
				store:  make(map[string]DeviceState),
			}
			done, err := cache.ReadMessage(context.Background())
			assert.ErrorIs(t, err, tt.expectedError)
			assert.Equal(t, tt.expectedDone, done)

			state, _ := cache.Get(tt.inputDeviceID)
			assert.Equal(t, tt.expectedState, state)
		})
	}
}
