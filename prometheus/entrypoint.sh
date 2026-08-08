#!/bin/sh
set -eu

# Prometheus does not expand environment variables inside prometheus.yml, so
# the scrape interval (a .env knob per AGENTS.md) is interpolated here into a
# writable copy before Prometheus loads it. /prometheus is the TSDB volume,
# writable by the image's nobody user.
sed "s/\${PROMETHEUS_SCRAPE_INTERVAL}/${PROMETHEUS_SCRAPE_INTERVAL:-10s}/g" \
  /etc/prometheus/prometheus.yml > /prometheus/prometheus.yml

exec /bin/prometheus \
  --config.file=/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus
