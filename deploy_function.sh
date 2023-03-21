#!/bin/bash

# Check arguments
if [ $# -ne 3 ]; then
  echo "Usage: ./deploy.sh <FUNCTION_NAME> <PUBSUB_TOPIC_ID> <SERVICE_ACCOUNT>"
  # Usage Example ./deploy.sh  er-dispatcher-1c19dc7e-73e2-4af3-93f5-a1cb322e5add-stg destination-1c19dc7e-73e2-4af3-93f5-a1cb322e5add-stg "service-account-a1234@cdip-dev-78ca.iam.gserviceaccount.com"
  exit 1
fi
FUNCTION_NAME=$1
PUBSUB_TOPIC_ID=$2
SERVICE_ACCOUNT=$3
# Extra settings. # ToDo: get this settings from the outbound config?
REGION="us-central1"
MIN_INSTANCES=1
MAX_INSTANCES=2
CPU=1
CONCURRENCY=4

echo "Deploying function triggered by topic $PUBSUB_TOPIC_ID .."
gcloud beta functions deploy $FUNCTION_NAME \
    --gen2 \
    --service-account=$SERVICE_ACCOUNT \
    --region=$REGION \
    --vpc-connector=cdip-dev-cloudrun-connect \
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
