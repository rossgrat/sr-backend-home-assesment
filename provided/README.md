# Device Events Assessment

## Overview

In this assessment, you will build a stream processing pipeline for devices that:
- **Consumes** device events from a Kafka topic named `device-events`
- **Validates** event transitions (enter/exit) for each device
- **Stores** valid events in a timeline service
- **Provides** an HTTP API to access the device timeline

The system processes real-time events from devices, including types such as `device_enter`, `device_exit`, and others, to build a presence timeline system.

## Requirements

### Kafka Consumer
- Listen to the `device-events` topic
- Filter and process only `device_enter` and `device_exit` events
- Enforce valid transitions:
  - No two consecutive `device_enter` events for the same device
  - No two consecutive `device_exit` events for the same device
- Forward valid events to the Timeline service

### Timeline Service (HTTP API)
Required routes:
- `POST /timeline`: Accept and store device events
- `GET /timeline/:device_id`: Retrieve ordered list of events for a device

## What's Provided

- **Docker Compose File:**
  - `docker-compose-kafka.yml` – Sets up a Kafka cluster for event streaming

- **Infrastructure Setup Script:**
  - `events_producer.sh` – Shell script that starts the Kafka containers and launches the data producer

- **Data Producer:**
  - `data-producer.py` – Python script that reads device events from `events.json` and publishes them to the `device-events` Kafka topic
  
- **Sample Data:**
  - `events.json` – Sample file containing device events (one JSON object per line) for testing

## Prerequisites

- **Docker** – Ensure you have Docker installed and running
- **Python3** – Ensure Python3 is installed

## Getting Started

1. Make the script executable:
   ```bash
   chmod +x events_producer.sh
   ```

2. Start the infrastructure:
   ```bash
   ./events_producer.sh
   ```

## Implementation Requirements

1. Choose a database for the Timeline service (any local database of your choice)
2. Implement the Kafka consumer following the validation rules
3. Implement the Timeline service with the required HTTP endpoints
4. You may use any libraries or frameworks you prefer

## Submission Guidelines

Please include in your submission:
1. Source code for both the consumer and Timeline service
2. Documentation of your design choices and assumptions
3. Detailed deployment and running instructions
4. Description of any additional configurations or services added

## Notes

- All messages must be consumed in order to maintain the correct timeline sequence for each device
- Messages should be processed in the order they are received to ensure accurate device state transitions
- You can use the Kafka console consumer to verify events are being published correctly:
  ```bash
  docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic device-events --from-beginning
  ```

## Questions?

Feel free to reach out with any questions regarding the setup or requirements!
