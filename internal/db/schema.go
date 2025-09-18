package db

type DeviceEvent struct {
	DeviceID  string `json:"device_id"`
	EventType string `json:"event_type"`
	Timestamp int64  `json:"timestamp"`
}
