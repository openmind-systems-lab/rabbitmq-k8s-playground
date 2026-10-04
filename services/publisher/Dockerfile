# --- STAGE 1: Build ---
FROM golang:1.25-alpine AS builder

# Install git (required to resolve Go modules)
RUN apk add --no-cache git

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/main .

# --- STAGE 2: Run ---
FROM alpine:latest

RUN apk --no-cache add ca-certificates \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app app

WORKDIR /home/app
COPY --from=builder /app/main .
RUN chmod +x ./main

USER 10001:10001

CMD ["./main"]
