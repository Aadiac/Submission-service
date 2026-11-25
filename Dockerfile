FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /api ./cmd/api

FROM alpine:3.20
WORKDIR /app
COPY --from=build /api /app/api
EXPOSE 8080
ENTRYPOINT ["/app/api"]
