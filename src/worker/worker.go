package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type ConnectRecord struct {
	Schema  Schema                 `json:"schema"`
	Payload map[string]interface{} `json:"payload"`
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

func RunWorker(ctx context.Context) {
	// Kafka setup
	brokers := []string{"kafka:29092"}
	groupID := "device-events-cleaner"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   "device-events",
	})
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   "device-events-cleaned",
	})

	fmt.Println("Worker started. Listening for device-events...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("Error reading message:", err)
			break
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(m.Value, &payload); err != nil {
			fmt.Println("Error parsing JSON:", err)
			continue
		}

		schema := Schema{
			Type:     "struct",
			Name:     "DeviceUpdate",
			Optional: false,
			Fields: []Field{
				{Field: "timestamp", Type: "int"},
				{Field: "device_id", Type: "string"},
				{Field: "event_type", Type: "string"},
			},
		}
		record := ConnectRecord{
			Schema:  schema,
			Payload: payload,
		}
		out, err := json.Marshal(record)
		if err != nil {
			fmt.Println("Error marshaling record:", err)
			continue
		}
		err = writer.WriteMessages(ctx, kafka.Message{Value: out})
		if err != nil {
			fmt.Println("Error writing cleaned message:", err)
		} else {
			fmt.Println("Published cleaned message to device-events-cleaned")
		}
	}
	reader.Close()
	writer.Close()
}
