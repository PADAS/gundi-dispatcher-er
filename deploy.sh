PUBSUB_TOPIC=$1
echo "Deploying function triggered by topic $PUBSUB_TOPIC .."
gcloud functions deploy er-positions-dispatcher \
    --gen2 \
    --vpc-connector=cdip-cloudrun-connector \
    --source=. \
    --runtime=python38 \
    --entry-point=main \
    --region=us-central1 \
    --memory=256Mi \
    --trigger-event="google.cloud.pubsub.topic.v1.messagePublished" \
    --trigger-resource=$PUBSUB_TOPIC \
    --env-vars-file=.env.yaml