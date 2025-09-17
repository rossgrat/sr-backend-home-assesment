#!/bin/bash

# Create and activate Python virtual environment
python3 -m venv venv
source venv/bin/activate

echo "Starting data producer to generate order events..."
pip3 install -r requirements.txt
python3 data-producer.py

# Deactivate virtual environment
deactivate

echo "Infrastructure setup complete. Check your Docker containers and logs for activity."
