## Build container
FROM golang:1.15-buster as builder
WORKDIR /app

# Copy go dependencies and download them
COPY go.* ./
RUN go mod download

# Copy all other non-ignored files and build server
COPY . ./
RUN go build -mod=readonly -v -o server

## Production container
FROM debian:buster-slim
RUN set -x \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy secrets and server, and execute it
COPY .env .env
COPY --from=builder /app/server /app/server
CMD ["/app/server", "-keywords", "bruno covas"]
