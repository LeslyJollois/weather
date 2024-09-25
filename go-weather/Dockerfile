# Step 1: Build the application
FROM golang:1.23.1 AS builder

# Define the target platform (Linux)
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

# Set the working directory
WORKDIR /app

# Copy the application files
COPY ./src .
COPY ./assets ./assets

# Install dependencies and build the application
RUN go mod download
RUN go build -o weather .

# Step 2: Create the final image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy the executable from the build stage
COPY --from=builder /app/weather .
COPY --from=builder /app/.env.stg ./.env
COPY --from=builder /app/gcp-service-account.json .
COPY --from=builder /app/assets ./assets

# Make the binary executable
RUN chmod +x ./weather

# Command to run the application
CMD ["./weather"]
