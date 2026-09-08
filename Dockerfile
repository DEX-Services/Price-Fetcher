# Render deploys this via runtime: docker in render.yaml.
# Builds the cmd/fetcher entrypoint into a small static binary.
#
# This avoids Render's native Go builder, which (a) tries to `go build` the whole
# module from the repo root where there are no .go files, and (b) compiles with a
# fixed Go version (here 1.27) rather than the module's go.mod.

# ---- Build stage ----
FROM golang:1.22-alpine AS builder
WORKDIR /src

# Cache module downloads first (separate layer) so rebuilds are fast.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# -tags netgo: static binary, no cgo DNS issues on the slim runtime.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -tags netgo \
    -ldflags '-s -w' \
    -o /out/price-fetcher \
    ./cmd/fetcher

# ---- Runtime stage ----
# Alpine base keeps it tiny; ca-certificates are REQUIRED because the fetcher
# dials Binance over wss:// and Aiven Redis over rediss:// (both TLS).
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
# Run as a non-root user for better container hygiene.
RUN addgroup -S app && adduser -S -G app app
COPY --from=builder /out/price-fetcher /usr/local/bin/price-fetcher
USER app
# /healthz lives here; Render's health check targets this port.
EXPOSE 8083
CMD ["price-fetcher"]