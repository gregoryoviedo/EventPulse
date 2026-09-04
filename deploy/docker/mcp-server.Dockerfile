# syntax=docker/dockerfile:1
FROM golang:1.25 AS build
WORKDIR /src
COPY pkg/ ./pkg/
COPY services/mcp-server/ ./services/mcp-server/
WORKDIR /src/services/mcp-server
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
