## Build container
FROM golang:1.15-buster as builder
WORKDIR /app

# Copy go dependencies and download them
COPY go.* ./
RUN go mod download

# Copy all other non-ignored files and build binaries
COPY . ./
RUN go build -mod=readonly -v -o bin/fetch-tweets ./fetch

## Production container
FROM debian:buster-slim
RUN set -x \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy secrets and server, and execute it
COPY .env .env
COPY google-application-credentials.json google-application-credentials.json
COPY --from=builder /app/fetch-tweets /app/fetch-tweets
CMD [ \
    "/app/fetch-tweets", \
    "-keywords", \
        "bruno covas,\
         russomano,\
         joyce hasselmann,\
         jilmar tatto,\
         guilherme boulos,\
         boulos,\
         andrea matarazzo,\
         arthur do val,\
         mamaefalei,\
         marina helou,\
         orlando silva,\
         levy fidelix,\
         filipe sabará,\
         márcio frança,\
         vera lúcia,\
         eleições sp,\
         prefeitura sp",\
     "-languages", ""]

