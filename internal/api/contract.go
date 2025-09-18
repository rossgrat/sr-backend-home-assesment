package api

type DeviceEvent struct {
	DeviceID  string `json:"deviceID"`
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
}

type CreateDeviceEventsRequest struct {
	Events []DeviceEvent `json:"events"`
}

type GetDeviceTimelineResponse struct {
	Events []DeviceEvent `json:"events"`
}
