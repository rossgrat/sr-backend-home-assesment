# syntax=docker/dockerfile:1
FROM golang:1.25.1-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod tidy && go build -o worker main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/worker /app/worker
CMD ["/app/worker"]
