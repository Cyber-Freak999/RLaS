# Multi-stage build for the rate-limiter.
#
# The build context is the repo root: rate-limiter depends on the shared
# rlas/redis module (go.mod `replace rlas/redis => ../redis`), so both module
# trees must be present to compile.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY redis/ ./redis/
COPY rate-limiter/ ./rate-limiter/
WORKDIR /src/rate-limiter
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/rate-limiter ./cmd/server

FROM alpine:3.20
# wget (busybox) powers the compose healthcheck; ca-certificates for any TLS.
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 -h /nonexistent -s /sbin/nologin app
COPY --from=build /out/rate-limiter /usr/local/bin/rate-limiter
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rate-limiter"]
