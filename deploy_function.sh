#!/bin/bash

# Check arguments
# ToDo: Support more arguments, maybe use named arguments?
if [ $# -ne 2 ]; then
  echo "Usage: ./deploy.sh <FUNCTION_NAME> <PUBSUB_TOPIC_ID>"
  # Usage Example ./deploy.sh  manyoni-er-dispatcher-stg destination-1c19dc7e-73e2-4af3-93f5-a1cb322e5add-stg
  exit 1
fi
FUNCTION_NAME=$1
PUBSUB_TOPIC_ID=$2
REGION="us-central1"
MIN_INSTANCES=1
MAX_INSTANCES=2
CPU=1
CONCURRENCY=4
echo "Deploying function triggered by topic $PUBSUB_TOPIC_ID .."
gcloud beta functions deploy $FUNCTION_NAME \
    --gen2 \
    --region=$REGION \
    --vpc-connector=cdip-cloudrun-connector \
    --source=. \
    --runtime=python38 \
    --entry-point=main \
    --memory=256Mi \
    --trigger-event="google.cloud.pubsub.topic.v1.messagePublished" \
    --trigger-resource=$PUBSUB_TOPIC_ID \
    --env-vars-file=.env.yaml \
    --min-instances=$MIN_INSTANCES \
    --max-instances=$MAX_INSTANCES \
    --concurrency=$CONCURRENCY \
    --cpu=$CPU \
    --retry
