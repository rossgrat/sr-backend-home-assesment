package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type DeviceEvent struct {
	DeviceID  string `json:"deviceID"`
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
}

func main() {
	baseURL := "http://localhost:8080"

	// 1. POST /timeline
	events := []DeviceEvent{
		{
			DeviceID:  "device123",
			EventType: "on",
			Timestamp: time.Now().Format(time.RFC3339),
		},
		{
			DeviceID:  "device123",
			EventType: "off",
			Timestamp: time.Now().Add(10 * time.Second).Format(time.RFC3339),
		},
	}
	payload, _ := json.Marshal(struct {
		Events []DeviceEvent `json:"events"`
	}{Events: events})
	fmt.Println("Payload:", string(payload))
	resp, err := http.Post(baseURL+"/timeline", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Println("POST /timeline status:", resp.Status)
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("POST response body:", string(body))
	}

	// 2. GET /timeline/:device_id?start=...&end=...
	start := time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	end := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
	getURL := fmt.Sprintf("%s/timeline/device123?start=%s&end=%s", baseURL, start, end)
	resp, err = http.Get(getURL)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var result struct {
		Events []DeviceEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		panic(err)
	}
	fmt.Println("GET /timeline/device123 result:", result.Events)
}
