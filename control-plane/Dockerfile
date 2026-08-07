# Multi-stage build for the control-plane.
#
# The build context is the repo root: control-plane depends on the shared
# rlas/redis module (go.mod `replace rlas/redis => ../redis`), so both module
# trees must be present to compile.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY redis/ ./redis/
COPY control-plane/ ./control-plane/
WORKDIR /src/control-plane
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/control-plane ./cmd/server

FROM alpine:3.20
# wget (busybox) powers the compose healthcheck; ca-certificates for any TLS.
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 -h /nonexistent -s /sbin/nologin app
COPY --from=build /out/control-plane /usr/local/bin/control-plane
USER app
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/control-plane"]
