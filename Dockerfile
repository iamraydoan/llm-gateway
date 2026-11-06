FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/llm-gateway .
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/llm-gateway /app/llm-gateway
EXPOSE 8080
ENTRYPOINT ["/app/llm-gateway"]
CMD ["-config", "/app/config.yaml"]
