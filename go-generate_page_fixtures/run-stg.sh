#!/bin/bash

# Variables
ENV="stg"
PROJECT_ID="weather-436309"
CLUSTER_REGION="europe-west1-b"
CLUSTER_NAME="$ENV-weather"
DEPOSIT_NAME="$ENV-go-generate-page-fixtures"
IMAGE_REGION="europe-west1"
IMAGE_NAME="$ENV-go-generate_page_fixtures"

# 1. Authenticate to the GCP Kubernetes cluster
echo "Authenticating to Google Cloud..."
# gcloud auth login
gcloud config set project $PROJECT_ID
gcloud container clusters get-credentials $CLUSTER_NAME --region $CLUSTER_REGION

# 2. Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_REGION-docker.pkg.dev/$PROJECT_ID/$DEPOSIT_NAME/$IMAGE_NAME:latest .

# 3. Push the image to Google Container Registry
echo "Pushing Docker image to Google Container Registry..."
docker push $IMAGE_REGION-docker.pkg.dev/$PROJECT_ID/$DEPOSIT_NAME/$IMAGE_NAME:latest

# 4. Update the Kubernetes job
echo "Executing Kubernetes job..."
kubectl delete job stg-go-generate-page-fixtures --ignore-not-found
kubectl apply -f job.yaml

echo "Job $IMAGE_NAME executed."
