#!/bin/sh
# Entry point for the Redis Sentinel nodes. Redis has no notion of an
# "optional password": the config files are committed unauthenticated, and if
# REDIS_PASSWORD is set we append the auth directives to a merged copy at
# startup instead of templating the committed configs.
#
# Usage: entrypoint.sh <role> <base-config> <binary>
#   role in {master, replica, sentinel}

set -eu

role="$1"
base="$2"
binary="$3"

# Sentinel rewrites its config file to persist its identity (myid), the
# observed topology, and failover epochs across restarts, so it cannot run
# from the read-only committed file. Run it from a writable copy under /data,
# seeding it from the committed config only on first run — clobbering it on
# every restart would make each sentinel look brand-new (new myid) and trigger
# spurious failover churn.
if [ "$role" = "sentinel" ]; then
  writable="/data/sentinel.conf"
  if [ ! -f "$writable" ]; then
    cp "$base" "$writable"
  fi
  base="$writable"
fi

if [ -n "${REDIS_PASSWORD:-}" ]; then
  merged="/usr/local/etc/redis/redis-merged-${role}.conf"
  cp "$base" "$merged"
  printf 'requirepass %s\n' "$REDIS_PASSWORD" >> "$merged"
  case "$role" in
    master | replica)
      printf 'masterauth %s\n' "$REDIS_PASSWORD" >> "$merged"
      ;;
    sentinel)
      printf 'sentinel auth-pass mymaster %s\n' "$REDIS_PASSWORD" >> "$merged"
      ;;
  esac
  exec "$binary" "$merged"
fi

exec "$binary" "$base"
