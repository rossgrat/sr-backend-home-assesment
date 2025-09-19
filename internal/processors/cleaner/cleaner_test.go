package cleaner

import (
	"context"
	"encoding/json"
	"errors"
	"sr-backend-home-assessment/internal/cache"
	k "sr-backend-home-assessment/internal/kafka"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func Test_ProcessMessage(t *testing.T) {
	cases := []struct {
		name          string
		inputMessage  func(string) kafka.Message
		inputDeviceID string
		setupCache    func(string) deviceCache
		setupReader   func(kafka.Message) k.Reader
		setupWriter   func(string) k.Writer
		expectedErr   error
	}{
		{
			name: "valid message",
			inputMessage: func(deviceID string) kafka.Message {
				event := k.DeviceEvent{
					DeviceID:  deviceID,
					EventType: "device_enter",
					Timestamp: 1,
				}
				data, _ := json.Marshal(event)
				return kafka.Message{
					Key:   []byte(event.DeviceID),
					Value: data,
				}
			},
			inputDeviceID: "device123",
			setupCache: func(deviceID string) deviceCache {
				c := NewMockdeviceCache(t)
				c.EXPECT().Get(deviceID).Return(cache.DeviceState{
					LastEvent:         "device_exit",
					LastTimestampSeen: 0,
				}, true)
				c.EXPECT().Set(deviceID, cache.DeviceState{
					LastEvent:         "device_enter",
					LastTimestampSeen: 1,
				})
				return c
			},
			setupReader: func(inputMessage kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(inputMessage, nil)
				return r
			},
			setupWriter: func(deviceID string) k.Writer {
				w := k.NewMockWriter(t)
				record := k.StructuredConnectRecord{
					Schema: k.StructuredSchema,
					Payload: k.DeviceEvent{
						DeviceID:  deviceID,
						EventType: "device_enter",
						Timestamp: 1,
					},
				}
				recordBytes, _ := json.Marshal(record)
				w.EXPECT().WriteMessages(
					mock.Anything,
					[]kafka.Message{
						{
							Key:   []byte(deviceID),
							Value: recordBytes,
						},
					},
				).Return(nil)
				return w
			},
			expectedErr: nil,
		},
		{
			name: "reader failed",
			inputMessage: func(deviceID string) kafka.Message {
				event := k.DeviceEvent{
					DeviceID:  deviceID,
					EventType: "device_enter",
					Timestamp: 1,
				}
				data, _ := json.Marshal(event)
				return kafka.Message{
					Key:   []byte(event.DeviceID),
					Value: data,
				}
			},
			inputDeviceID: "device123",
			setupCache: func(deviceID string) deviceCache {
				return NewMockdeviceCache(t)
			},
			setupReader: func(inputMessage kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(inputMessage, errors.New("failed"))
				return r
			},
			setupWriter: func(deviceID string) k.Writer {
				return k.NewMockWriter(t)
			},
			expectedErr: ErrReadMessage,
		},
		{
			name: "invalid message JSON",
			inputMessage: func(deviceID string) kafka.Message {
				return kafka.Message{
					Key:   []byte(deviceID),
					Value: []byte("invalid-json"),
				}
			},
			inputDeviceID: "device123",
			setupCache: func(deviceID string) deviceCache {
				return NewMockdeviceCache(t)
			},
			setupReader: func(inputMessage kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(inputMessage, nil)
				return r
			},
			setupWriter: func(deviceID string) k.Writer {
				return k.NewMockWriter(t)
			},
			expectedErr: ErrJSONParse,
		},
		{
			name: "writer failed",
			inputMessage: func(deviceID string) kafka.Message {
				event := k.DeviceEvent{
					DeviceID:  deviceID,
					EventType: "device_enter",
					Timestamp: 1,
				}
				data, _ := json.Marshal(event)
				return kafka.Message{
					Key:   []byte(event.DeviceID),
					Value: data,
				}
			},
			inputDeviceID: "device123",
			setupCache: func(deviceID string) deviceCache {
				c := NewMockdeviceCache(t)
				c.EXPECT().Get(deviceID).Return(cache.DeviceState{
					LastEvent:         "device_exit",
					LastTimestampSeen: 0,
				}, true)
				return c
			},
			setupReader: func(inputMessage kafka.Message) k.Reader {
				r := k.NewMockReader(t)
				r.EXPECT().ReadMessage(mock.Anything).Return(inputMessage, nil)
				return r
			},
			setupWriter: func(deviceID string) k.Writer {
				w := k.NewMockWriter(t)
				record := k.StructuredConnectRecord{
					Schema: k.StructuredSchema,
					Payload: k.DeviceEvent{
						DeviceID:  deviceID,
						EventType: "device_enter",
						Timestamp: 1,
					},
				}
				recordBytes, _ := json.Marshal(record)
				w.EXPECT().WriteMessages(
					mock.Anything,
					[]kafka.Message{
						{
							Key:   []byte(deviceID),
							Value: recordBytes,
						},
					},
				).Return(errors.New("failed"))
				return w
			},
			expectedErr: ErrWriteMessage,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cleaner := &Cleaner{
				cache:  tt.setupCache(tt.inputDeviceID),
				reader: tt.setupReader(tt.inputMessage(tt.inputDeviceID)),
				writer: tt.setupWriter(tt.inputDeviceID),
			}
			err := cleaner.ProcessMessage(context.Background())
			assert.ErrorIs(t, err, tt.expectedErr)
		})
	}
}

func Test_validateEvent(t *testing.T) {
	cases := []struct {
		name        string
		inputEvent  k.DeviceEvent
		setupCache  func(string) deviceCache
		expectedErr error
	}{
		{
			name: "valid event",
			inputEvent: k.DeviceEvent{
				DeviceID:  "device123",
				EventType: "device_enter",
				Timestamp: 1,
			},
			setupCache: func(deviceID string) deviceCache {
				c := NewMockdeviceCache(t)
				c.EXPECT().Get(deviceID).Return(cache.DeviceState{
					LastEvent:         "device_exit",
					LastTimestampSeen: 0,
				}, true)
				return c
			},
			expectedErr: nil,
		},
		{
			name: "invalid event",
			inputEvent: k.DeviceEvent{
				DeviceID:  "device123",
				EventType: "heartbeat",
				Timestamp: 1,
			},
			setupCache: func(deviceID string) deviceCache {
				return NewMockdeviceCache(t)
			},
			expectedErr: ErrInvalidEvent,
		},
		{
			name: "duplicate event",
			inputEvent: k.DeviceEvent{
				DeviceID:  "device123",
				EventType: "device_enter",
				Timestamp: 1,
			},
			setupCache: func(deviceID string) deviceCache {
				c := NewMockdeviceCache(t)
				c.EXPECT().Get(deviceID).Return(cache.DeviceState{
					LastEvent:         "device_enter",
					LastTimestampSeen: 0,
				}, true)
				return c
			},
			expectedErr: ErrDuplicateEvent,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cleaner := &Cleaner{
				cache: tt.setupCache(tt.inputEvent.DeviceID),
			}
			err := cleaner.validateEvent(tt.inputEvent)
			assert.ErrorIs(t, err, tt.expectedErr)
		})
	}

}
