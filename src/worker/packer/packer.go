package packer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func RunPacker(ctx context.Context) {
	brokers := []string{"kafka:29092"}
	groupID := "device-events-packer"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   "device-events-cleaned",
	})
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   "device-events-cleaned-compacted",
	})

	fmt.Println("Worker started. Listening for device-events...")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("Error reading message:", err)
			break
		}
		err = writer.WriteMessages(ctx, kafka.Message{Key: m.Key, Value: m.Value})
		if err != nil {
			fmt.Println("Error moving message:", err)
		} else {
			fmt.Println("Moved message to device-events-cleaned-compacted")
		}
	}
	reader.Close()
	writer.Close()
}
