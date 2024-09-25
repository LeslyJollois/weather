#!/bin/bash

# Variables
ENV="stg"
PROJECT_ID="weather-436309"
CLUSTER_REGION="europe-west1-b"
CLUSTER_NAME="$ENV-weather"
DEPOSIT_NAME="$ENV-go-lead-event-subscription"
IMAGE_REGION="europe-west1"
IMAGE_NAME="$ENV-go-lead_event_subscription"
CONTAINER_NAME="$ENV-go-lead-event-subscription"
DEPLOYMENT_NAME="$ENV-go-lead-event-subscription"
NAMESPACE="default"

# Generate a timestamp
TIMESTAMP=$(date +%Y%m%d%H%M%S)

# 1. Authenticate to the GCP Kubernetes cluster
echo "Authenticating to Google Cloud..."
# gcloud auth login
gcloud config set project $PROJECT_ID
gcloud container clusters get-credentials $CLUSTER_NAME --region $CLUSTER_REGION

# 2. Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_REGION-docker.pkg.dev/$PROJECT_ID/$DEPOSIT_NAME/$IMAGE_NAME:$TIMESTAMP .

# 3. Push the image to Google Container Registry
echo "Pushing Docker image to Google Container Registry..."
docker push $IMAGE_REGION-docker.pkg.dev/$PROJECT_ID/$DEPOSIT_NAME/$IMAGE_NAME:$TIMESTAMP

# 4. Update the Kubernetes deployment
echo "Updating Kubernetes deployment..."
kubectl apply -f deployment.yaml
kubectl set image deployment/$DEPLOYMENT_NAME $CONTAINER_NAME=$IMAGE_REGION-docker.pkg.dev/$PROJECT_ID/$DEPOSIT_NAME/$IMAGE_NAME:$TIMESTAMP --namespace=$NAMESPACE

# 5. Confirm the update
echo "Deployment updated. Verifying the rollout status..."
kubectl rollout status deployment/$DEPLOYMENT_NAME --namespace=$NAMESPACE

echo "Deployment of $IMAGE_NAME complete."
