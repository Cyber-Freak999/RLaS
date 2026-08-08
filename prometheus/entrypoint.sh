#!/bin/sh
set -eu

# Prometheus does not expand environment variables inside prometheus.yml, so
# the scrape interval (a .env knob per AGENTS.md) is interpolated here into a
# writable copy before Prometheus loads it. /prometheus is the TSDB volume,
# writable by the image's nobody user.
#
# Two configs share this path: the compose default (DNS discovery of the 3
# replicas, prometheus.yml) and the Render variant (explicit HTTPS targets
# from env, prometheus.render.yml). Setting RATE_LIMITER_TARGETS selects the
# Render variant; the compose stack leaves it unset and keeps its behavior.
if [ "${RATE_LIMITER_TARGETS:-}" != "" ]; then
  sed -e "s/\${PROMETHEUS_SCRAPE_INTERVAL}/${PROMETHEUS_SCRAPE_INTERVAL:-10s}/g" \
      -e "s#__RATE_LIMITER_TARGETS__#${RATE_LIMITER_TARGETS}#g" \
      -e "s#__CONTROL_PLANE_TARGET__#${CONTROL_PLANE_TARGET:-control-plane.onrender.com}#g" \
      /etc/prometheus/prometheus.render.yml > /prometheus/prometheus.yml
else
  sed "s/\${PROMETHEUS_SCRAPE_INTERVAL}/${PROMETHEUS_SCRAPE_INTERVAL:-10s}/g" \
    /etc/prometheus/prometheus.yml > /prometheus/prometheus.yml
fi

exec /bin/prometheus \
  --config.file=/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus
