# Ubiety Senior Backend Home Assessment
Hi Ubiety team, thanks so much for considering me for this role! This repo is the solution source code for the provided project spec.

## Running Locally
You'll need `make` installed if you don't have it already. To start this application and all of its dependencies, run `make up`. To stop everything, run `make down`.

## Quick Callouts
I wanted to make a quick callout that I changed up some of the provided source files.
1. `docker-compose-kafka.yml` -> `docker-compose.yml`
    - Removed the `wurstmeister` Zookeeper and Kafka images in favor of the official Confluent images. The `wurstmeister` images don't appear to have builds for ARM64, which is what I am using, so the project would not startup.
    - Added other dependencies, these will be described in the Dependencies section.
    - Moved the `device-events` Kafka topic instantiation to a one-shot `kafka-init-topics` container.
2. `data-producer.py` -> `provided/data-producer.py`
    - No changes
3. `events.json` -> `provided/events.json`
    - No changes
4. `events_producer.sh` -> `provided/events_producer.sh`
    - Removed the `docker compose up` commands
5. `README.md` -> `provided/README.md`
    - No changes
6. `requirements.txt` -> `provided/requirements.txt`
    - No Changes
    
## Overview
To get us started, here is an overview of how this system is structured.
![alt text](architecture.png "Architecture")

1. Sensors write their events to the `device-events` Kafka topic (provided).
2. A worker (the Cleaner) in the `main` application consumes the events published to `device-events`, performs the validation outlined in the project spec, adds a schema for the Postgres Kafka Connecter (more on that in the next step), and publishes the event to `device_events_cleaned`.
3. A Kafka Postgres Connector consumes events from `device_events_cleaned` and writes them directly to the TimescaleDB instance. The connector is present in the `kafka-connect` Docker Compose container, and the `kafka-connect-init` one-shot container sets the correct config on startup. The actual connecter executable, along with the config are present in `kafka-connect`. In order for the Kafka Connector to work, JSON schema is required in the Kafka messages.
    - Side Note: Although the spec requires a RESTful `POST /timeline` endpoint, I did not want to introduce synchronicity into the asynchronous pipeline, which is why I used the Kafka connector.
4. The Postgres Kafka Connector writes to TimescaleDB, a Postgres database with a plugin that optimizes the database for time series data. We only really need a database optimized for time series data in a larger scale application. Any database can be used for this toy project.
5. A RESTful server provides the required endpoints to create timeline events and read timelines as specified by the spec.

### Caching
There is also a local cache in the Cleaner that stores the previous state of all devices. In order to populate this cache, a consumer for the `device_events_cleaned_compacted` topic will run on startup until all messages on the compacted topic have been consumed. This prevents the Cleaner from losing the device state if it crashes.

The `device_events_cleaned_compacted` topic is published to by the Packer, which consumes from the `device_events_cleaned` topic along with the Kafka Connector.

## Assumptions
- One consumer per consumer group per partition for all Kafka topics
    - `sync.Map` for a local cache is not necessary 
- One processor (Goroutine) per consumer
- Timestamps are reliable


## Future Work
- OTEL, logs, traces, metrics (especially throughput and response time percentiles)
- Scale up Kafka partitions, it is hard to move data to a different partition, can never downscale partitions
- Partition on device ID?
- All Kafka handlers at present are susceptible to the poison pill problem, this needs fixing
- waitForBrokers should be expanded to all workers, brokers may not be the same
- Broker strings should be checked to work with multiple brokers
- Scale up brokers
- DB migrator needs to handle rollbacks
- Local cache should be moved to a distributed cache (Redis)
- Handle unreliable clocks