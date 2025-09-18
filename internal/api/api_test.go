package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sr-backend-home-assessment/internal/db"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/mock"
)

func Test_GetDeviceTimeline(t *testing.T) {
	cases := []struct {
		name           string
		setupDB        func(string, string, string) repository
		inputDeviceID  string
		inputStartTime string
		inputEndTime   string
		expectedStatus int
	}{
		{
			name: "valid request",
			setupDB: func(inputDeviceID, inputStartTime, inputEndTime string) repository {
				inputStartTimeRFC, _ := time.Parse(time.RFC3339, inputStartTime)
				inputStartTimeMilli := inputStartTimeRFC.UnixMilli()
				inputEndTimeRFC, _ := time.Parse(time.RFC3339, inputEndTime)
				inputEndTimeMilli := inputEndTimeRFC.UnixMilli()
				mockRepo := &Mockrepository{}
				mockRepo.EXPECT().LoadEventsBetween(
					mock.Anything,
					inputDeviceID,
					inputStartTimeMilli,
					inputEndTimeMilli,
				).Return([]db.DeviceEvent{}, nil)
				return mockRepo
			},
			inputDeviceID:  "device123",
			inputStartTime: "2023-10-01T00:00:00Z",
			inputEndTime:   "2023-10-02T00:00:00Z",
			expectedStatus: http.StatusOK,
		},
		{
			name: "invalid start time",
			setupDB: func(inputDeviceID, inputStartTime, inputEndTime string) repository {
				return &Mockrepository{}
			},
			inputDeviceID:  "device123",
			inputStartTime: "bad",
			inputEndTime:   "2023-10-02T00:00:00Z",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "invalid end time",
			setupDB: func(inputDeviceID, inputStartTime, inputEndTime string) repository {
				return &Mockrepository{}
			},
			inputDeviceID:  "device123",
			inputStartTime: "2023-10-01T00:00:00Z",
			inputEndTime:   "bad",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "database error",
			setupDB: func(inputDeviceID, inputStartTime, inputEndTime string) repository {
				inputStartTimeRFC, _ := time.Parse(time.RFC3339, inputStartTime)
				inputStartTimeMilli := inputStartTimeRFC.UnixMilli()
				inputEndTimeRFC, _ := time.Parse(time.RFC3339, inputEndTime)
				inputEndTimeMilli := inputEndTimeRFC.UnixMilli()
				mockRepo := &Mockrepository{}
				mockRepo.EXPECT().LoadEventsBetween(
					mock.Anything,
					inputDeviceID,
					inputStartTimeMilli,
					inputEndTimeMilli,
				).Return(nil, errors.New("database error"))
				return mockRepo
			},
			inputDeviceID:  "device123",
			inputStartTime: "2023-10-01T00:00:00Z",
			inputEndTime:   "2023-10-02T00:00:00Z",
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			api := New(Config{
				DB: tt.setupDB(tt.inputDeviceID, tt.inputStartTime, tt.inputEndTime),
			})

			req := httptest.NewRequest(http.MethodGet, "https://test.com/timeline/"+tt.inputDeviceID, nil)
			ctx := chi.NewRouteContext()
			ctx.URLParams.Add("device_id", tt.inputDeviceID)
			req.URL.RawQuery = "start=" + tt.inputStartTime + "&end=" + tt.inputEndTime
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, ctx))

			w := &httptest.ResponseRecorder{}
			api.GetDeviceTimeline(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}
		})
	}

}

func Test_CreateDeviceTimeline(t *testing.T) {

	cases := []struct {
		name           string
		setupDB        func([]db.DeviceEvent) repository
		payload        func() string
		expectedStatus int
	}{
		{
			name: "happy path",
			setupDB: func(events []db.DeviceEvent) repository {
				mockRepo := &Mockrepository{}
				mockRepo.EXPECT().CreateTimeline(
					mock.Anything,
					events,
				).Return(nil)
				return mockRepo
			},
			payload: func() string {
				req := CreateDeviceEventsRequest{
					Events: []DeviceEvent{
						{DeviceID: "device123", EventType: "on", Timestamp: "2023-10-01T00:00:00Z"},
					},
				}
				data, _ := json.Marshal(req)
				return string(data)
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name: "invalid request body",
			setupDB: func(events []db.DeviceEvent) repository {
				return &Mockrepository{}
			},
			payload:        func() string { return `not-a-json` },
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "invalid event timestamp",
			setupDB: func(events []db.DeviceEvent) repository {
				return &Mockrepository{}
			},
			payload: func() string {
				req := CreateDeviceEventsRequest{
					Events: []DeviceEvent{
						{DeviceID: "device123", EventType: "on", Timestamp: "not-a-timestamp"},
					},
				}
				data, _ := json.Marshal(req)
				return string(data)
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "database error",
			setupDB: func(events []db.DeviceEvent) repository {
				mockRepo := &Mockrepository{}
				mockRepo.EXPECT().CreateTimeline(
					mock.Anything,
					events,
				).Return(errors.New("database error"))
				return mockRepo
			},
			payload: func() string {
				req := CreateDeviceEventsRequest{
					Events: []DeviceEvent{
						{DeviceID: "device123", EventType: "on", Timestamp: "2023-10-01T00:00:00Z"},
					},
				}
				data, _ := json.Marshal(req)
				return string(data)
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var req CreateDeviceEventsRequest
			json.Unmarshal([]byte(tt.payload()), &req)
			dbEvents, _ := convertEventsToDB(req.Events)
			api := New(Config{
				DB: tt.setupDB(dbEvents),
			})

			reqBody := bytes.NewBufferString(tt.payload())
			r := httptest.NewRequest(http.MethodPost, "https://test.com/timeline", reqBody)
			w := &httptest.ResponseRecorder{}
			api.CreateDeviceTimeline(w, r)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}
		})
	}
}
