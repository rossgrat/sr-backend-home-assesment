package api

import (
	"encoding/json"
	"net/http"
	"sr-backend-home-assessment/internal/db"
	"time"

	"github.com/go-chi/chi/v5"
)

type API struct {
	DB *db.DB
}

type Config struct {
	DB *db.DB
}

func New(cfg Config) *API {
	return &API{DB: cfg.DB}
}

func (a *API) GetDeviceTimeline(w http.ResponseWriter, r *http.Request) {
	deviceID := chi.URLParam(r, "device_id")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

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
			Timestamp: time.Unix(event.Timestamp, 0).Format(time.RFC3339),
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

	dbEvents := make([]db.DeviceEvent, 0, len(timeline.Events))
	for _, event := range timeline.Events {
		parsedTime, err := time.Parse(time.RFC3339, event.Timestamp)
		if err != nil {
			http.Error(w, "invalid event timestamp", http.StatusBadRequest)
			return
		}
		dbEvents = append(dbEvents, db.DeviceEvent{
			DeviceID:  event.DeviceID,
			EventType: event.EventType,
			Timestamp: parsedTime.UnixMilli(),
		})
	}

	if err := a.DB.CreateTimeline(r.Context(), dbEvents); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}
