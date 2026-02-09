# -------- build stage --------
FROM golang:alpine AS builder

RUN apk add --no-cache git
WORKDIR /app

# cache deps
COPY go.mod go.sum ./
RUN go mod download

# copy source
COPY . .

# build binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -v -o /go/bin/app .

# -------- final stage --------
FROM alpine:latest

RUN apk --no-cache add ca-certificates
COPY --from=builder /go/bin/app /app

EXPOSE 3000
ENTRYPOINT ["/app"]
LABEL Name=bethos Version=0.0.1
