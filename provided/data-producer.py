#!/usr/bin/env python3
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pytz
from tzlocal import get_localzone

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "device-events"
DATA_FILE = "events.json"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)


def load_events(file_path):
    """Load events.json, convert Unix millisecond timestamp to datetime."""
    events = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            # Convert Unix milliseconds to datetime
            event["_ts"] = datetime.fromtimestamp(event["timestamp"] / 1000.0)
            events.append(event)
    return events


if __name__ == "__main__":
    events = load_events(DATA_FILE)
    if not events:
        print(f"No events found in {DATA_FILE}. Exiting.")
        exit(1)

    # Compute the earliest original timestamp
    t_min = min(e["_ts"] for e in events)

    # Use the machine's local timezone
    local_tz = get_localzone()

    # Determine the start of the next full minute (second=0) in local timezone
    now = datetime.now(local_tz)
    t0 = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    wait_secs = (t0 - now).total_seconds()
    print(f"⏱  Waiting until {t0.isoformat()} to start production…")
    if wait_secs > 0:
        time.sleep(wait_secs)

    print("▶️  Starting real-time production…")
    for e in events:
        # Calculate the relative delay from t_min
        delta: timedelta = e["_ts"] - t_min
        send_time = t0 + delta

        # Sleep until the correct real-time moment
        now = datetime.now(local_tz)
        sleep_secs = (send_time - now).total_seconds()
        if sleep_secs > 0:
            time.sleep(sleep_secs)

        # Update timestamp to current Unix milliseconds
        current_time = int(time.time() * 1000)
        e["timestamp"] = current_time
        del e["_ts"]

        producer.send(TOPIC, value=e)
        print(
            f"→ Sent device={e['device_id']} event={e['event_type']} @ {datetime.fromtimestamp(current_time/1000.0).isoformat()}"
        )

    producer.flush()
    producer.close()
    print("✅ All events sent. Exiting.")
