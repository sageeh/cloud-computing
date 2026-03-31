FROM golang:1.26-alpine3.23
WORKDIR /app

COPY . ./app

RUN go mod init producer
RUN apk update --no-cache && \
    apk add git --no-cache && \
    apk upgrade --no-cache

RUN go install github.com/air-verse/air@latest
RUN go get github.com/segmentio/kafka-go
RUN go get github.com/lib/pq
RUN go mod download
CMD [ "air", "main.go" ]
