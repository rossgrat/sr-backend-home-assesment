package worker

const (
	DeviceEnter  = "device_enter"
	DeviceExit   = "device_exit"
	Heartbeat    = "heartbeat"
	StatusUpdate = "status_update"
)

type UnstructuredConnectRecord struct {
	Payload DeviceEvent `json:"payload"`
}

type StructuredConnectRecord struct {
	Schema  Schema      `json:"schema"`
	Payload DeviceEvent `json:"payload"`
}

type DeviceEvent struct {
	Timestamp int64  `json:"timestamp"`
	DeviceID  string `json:"device_id"`
	EventType string `json:"event_type"`
}

type Schema struct {
	Type     string  `json:"type"`
	Name     string  `json:"name"`
	Fields   []Field `json:"fields"`
	Optional bool    `json:"optional"`
}

type Field struct {
	Field string `json:"field"`
	Type  string `json:"type"`
}

var StructuredSchema = Schema{
	Type:     "struct",
	Name:     "DeviceUpdate",
	Optional: false,
	Fields: []Field{
		{Field: "timestamp", Type: "int64"},
		{Field: "device_id", Type: "string"},
		{Field: "event_type", Type: "string"},
	},
}
