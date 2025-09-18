package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

// AI Generated
// Steps:
// 1. Read events from provided/events.json
// 2. Publish events to Kafka topic device-events
// 3. Wait a minute for consumer to process events
// 4. Call timeline API for each device_id found in events.json with start=0 and end=current time
// 5. Print the timelines returned by the API
// 6. Compare the results against provided/events_expected.json

func main() {
	// Publish all events to topic in events.json

	// Load events from events.json (newline-delimited JSON)
	file, err := os.Open("../../provided/events.json")
	if err != nil {
		panic(fmt.Errorf("failed to open events.json: %w", err))
	}
	defer file.Close()

	// Set up Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "device-events",
	})
	defer writer.Close()

	scanner := bufio.NewScanner(file)
	deviceIDs := make(map[string]struct{})

	// Collect all Kafka messages first
	// Declare messages slice explicitly
	var messages []kafka.Message
	for scanner.Scan() {
		line := scanner.Text()
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			fmt.Printf("failed to decode event: %v\n", err)
			continue
		}
		// Track device_id
		if id, ok := event["device_id"].(string); ok {
			deviceIDs[id] = struct{}{}
		}
		value, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("failed to marshal event: %v\n", err)
			continue
		}
		msg := kafka.Message{
			Value: value,
		}
		// Ensure the result of append is used
		messages = append(messages, msg)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("error reading events.json: %v\n", err)
	}

	//Send all messages in one batch
	if err := writer.WriteMessages(context.TODO(), messages...); err != nil {
		fmt.Printf("failed to write messages: %v\n", err)
	} else {
		fmt.Printf("Published %d events to Kafka topic 'device-events'\n", len(messages))
	}
	fmt.Printf("Unique device IDs: %v\n", deviceIDs)

	// Allow some time for the consumer to process events
	time.Sleep(1 * time.Minute)

	// Call timeline API for each device
	timelineMap := make(map[string]interface{})

	for deviceID := range deviceIDs {
		// Add start and end timestamps in RFC3339 format to the URL
		startTime := time.Unix(0, 0).Format(time.RFC3339)
		endTime := time.Now().Format(time.RFC3339)
		url := fmt.Sprintf("http://localhost:8080/timeline/%s?start=%s&end=%s", deviceID, url.QueryEscape(startTime), url.QueryEscape(endTime))
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("Error fetching timeline for %s: %v\n", deviceID, err)
			continue
		}
		defer resp.Body.Close()

		// Check HTTP status code
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body) // Read body even on HTTP error
			fmt.Printf("Error fetching timeline for %s: HTTP %d\n", deviceID, resp.StatusCode)
			fmt.Printf("Raw response: %s\n", string(body)) // Print raw response body
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Error reading response for %s: %v\n", deviceID, err)
			continue
		}

		var timeline []interface{}
		if err := json.Unmarshal(body, &timeline); err != nil {
			fmt.Printf("Error unmarshalling timeline for %s: %v\n", deviceID, err)
			fmt.Printf("Raw response: %s\n", string(body)) // Print raw response body on unmarshalling error
			continue
		}

		timelineMap[deviceID] = timeline
	}

	// Print the timelines with indented JSON
	if prettyJSON, err := json.MarshalIndent(timelineMap, "", "  "); err != nil {
		fmt.Printf("Error marshalling timelines: %v\n", err)
	} else {
		fmt.Printf("Timelines:\n%s\n", string(prettyJSON))
	}

	// Check result against expected

	// Load expected events from events_expected.json
	expectedFile, err := os.Open("./events_expected.json")
	if err != nil {
		panic(fmt.Errorf("failed to open events_expected.json: %w", err))
	}
	defer expectedFile.Close()

	expectedScanner := bufio.NewScanner(expectedFile)
	expectedEvents := make(map[string][]map[string]interface{})
	for expectedScanner.Scan() {
		line := expectedScanner.Text()
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			fmt.Printf("failed to decode expected event: %v\n", err)
			continue
		}
		if id, ok := event["device_id"].(string); ok {
			expectedEvents[id] = append(expectedEvents[id], event)
		}
	}
	if err := expectedScanner.Err(); err != nil {
		fmt.Printf("error reading events_expected.json: %v\n", err)
	}

	// Convert timestamp in expected events from Unix milliseconds to RFC3339
	for deviceID, events := range expectedEvents {
		for i, event := range events {
			if timestamp, ok := event["timestamp"].(float64); ok {
				event["timestamp"] = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).Format(time.RFC3339)
				expectedEvents[deviceID][i] = event
			}
		}
	}

	if prettyJSON, err := json.MarshalIndent(expectedEvents, "", "  "); err != nil {
		fmt.Printf("Error marshalling timelines: %v\n", err)
	} else {
		fmt.Printf("EXPECTED:\n%s\n", string(prettyJSON))
	}

	// Verify that all expected events are present in the timeline and no additional events exist
	for deviceID, timeline := range timelineMap {
		expected, exists := expectedEvents[deviceID]
		if !exists {
			fmt.Printf("No expected events found for device %s\n", deviceID)
			continue
		}

		receivedEvents, ok := timeline.([]interface{})
		if !ok {
			fmt.Printf("Invalid timeline format for device %s\n", deviceID)
			continue
		}

		// Convert received events to []map[string]interface{}
		receivedMaps := make([]map[string]interface{}, len(receivedEvents))
		for i, event := range receivedEvents {
			receivedEvent, ok := event.(map[string]interface{})
			if !ok {
				fmt.Printf("Invalid event format in timeline for device %s\n", deviceID)
				continue
			}
			receivedMaps[i] = receivedEvent
		}

		// Sort both received and expected events
		sortEvents(receivedMaps)
		sortEvents(expected)

		// Compare sorted events
		if len(receivedMaps) != len(expected) {
			fmt.Printf("Event count mismatch for device %s: expected %d, got %d\n", deviceID, len(expected), len(receivedMaps))
			continue
		}
		for i, expectedEvent := range expected {
			for key, value := range expectedEvent {
				if receivedMaps[i][key] != value {
					fmt.Printf("Event mismatch for device %s at index %d\n", deviceID, i)
					fmt.Printf("Expected: %v\n", expectedEvent)
					fmt.Printf("Received: %v\n", receivedMaps[i])
					break
				}
			}
		}
	}

	fmt.Println("E2E test completed")
}

// sortEvents sorts a slice of events (maps) by their timestamp key, using time comparisons
func sortEvents(events []map[string]interface{}) {
	sort.Slice(events, func(i, j int) bool {
		iTime, err1 := time.Parse(time.RFC3339, events[i]["timestamp"].(string))
		jTime, err2 := time.Parse(time.RFC3339, events[j]["timestamp"].(string))
		if err1 == nil && err2 == nil {
			return iTime.Before(jTime)
		}
		return false
	})
}
