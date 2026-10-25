FROM golang:1.26.9 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tasker ./cmd/tasker

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/tasker /tasker

WORKDIR /data
EXPOSE 8080

# Mount a volume at /data to persist the SQLite database across containers.
ENTRYPOINT ["/tasker"]
