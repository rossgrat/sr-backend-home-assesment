package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sr-backend-home-assessment/internal/db"
	"time"

	"github.com/go-chi/chi/v5"
)

var (
	ErrInvalidTimestamp = fmt.Errorf("invalid timestamp")
)

type repository interface {
	CreateTimeline(context.Context, []db.DeviceEvent) error
	LoadEventsBetween(context.Context, string, int64, int64) ([]db.DeviceEvent, error)
}

type API struct {
	DB repository
}

type Config struct {
	DB repository
}

func New(cfg Config) *API {
	return &API{DB: cfg.DB}
}

func (a *API) GetDeviceTimeline(w http.ResponseWriter, r *http.Request) {
	deviceID := chi.URLParam(r, "device_id")
	startStr, err := url.QueryUnescape(r.URL.Query().Get("start"))
	if err != nil {
		http.Error(w, "invalid start query param", http.StatusBadRequest)
		return
	}
	endStr, err := url.QueryUnescape(r.URL.Query().Get("end"))
	if err != nil {
		http.Error(w, "invalid end query param", http.StatusBadRequest)
		return
	}

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, "invalid start timestamp", http.StatusBadRequest)
		return
	}
	startTimeUnix := startTime.UnixMilli()
	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		http.Error(w, "invalid end timestamp", http.StatusBadRequest)
		return
	}
	endTimeUnix := endTime.UnixMilli()

	events, err := a.DB.LoadEventsBetween(r.Context(), deviceID, startTimeUnix, endTimeUnix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var resp GetDeviceTimelineResponse
	for _, event := range events {
		resp.Events = append(resp.Events, DeviceEvent{
			DeviceID:  event.DeviceID,
			EventType: event.EventType,
			Timestamp: time.UnixMilli(event.Timestamp).Format(time.RFC3339),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (a *API) CreateDeviceTimeline(w http.ResponseWriter, r *http.Request) {
	var timeline CreateDeviceEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&timeline); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	dbEvents, err := convertEventsToDB(timeline.Events)
	if err != nil {
		http.Error(w, "invalid data in request body", http.StatusBadRequest)
		return
	}

	if err := a.DB.CreateTimeline(r.Context(), dbEvents); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func convertEventsToDB(events []DeviceEvent) ([]db.DeviceEvent, error) {
	const fn = "convertEventsToDB"
	dbEvents := make([]db.DeviceEvent, 0, len(events))
	for _, event := range events {
		parsedTime, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			return []db.DeviceEvent{}, fmt.Errorf("%s:%w:%w", fn, ErrInvalidTimestamp, err)
		}
		dbEvents = append(dbEvents, db.DeviceEvent{
			DeviceID:  event.DeviceID,
			EventType: event.EventType,
			Timestamp: parsedTime.UnixMilli(),
		})
	}
	return dbEvents, nil
}
