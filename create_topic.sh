#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: ./create_topic.sh <TOPIC_ID>"
  # Usage Example ./create_topic.sh destination-1c19dc7e-73e2-4af3-93f5-a1cb322e5add-stg
  exit 1
fi

TOPIC_ID=$1
echo "Creating topic $TOPIC_ID .."
gcloud pubsub topics create $TOPIC_ID
