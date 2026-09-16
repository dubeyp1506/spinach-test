FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/api ./cmd/api \
 && CGO_ENABLED=0 go build -o /app/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -o /app/seed ./cmd/seed

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/api /app/worker /app/seed ./
EXPOSE 8080
CMD ["/app/api"]
