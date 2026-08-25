FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o broker ./cmd/broker

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/broker .

EXPOSE 8080 8081 9080

CMD ["./broker"]