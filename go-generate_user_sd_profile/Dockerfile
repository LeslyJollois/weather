# Step 1: Build the application
FROM golang:1.23.1 AS builder

# Define the target platform (Linux)
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

# Set the working directory
WORKDIR /app

# Copy the application files
COPY ./src .

# Install dependencies and build the application
RUN go mod download
RUN go build -o generate_user_sd_profile .

# Step 2: Create the final image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy the executable from the build stage
COPY --from=builder /app/generate_user_sd_profile .
COPY --from=builder /app/.env.stg ./.env
COPY --from=builder /app/gcp-service-account.json .

# Make the binary executable
RUN chmod +x ./generate_user_sd_profile

# Command to run the application
CMD ["./generate_user_sd_profile"]
