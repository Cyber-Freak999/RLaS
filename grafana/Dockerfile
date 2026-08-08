# Custom Grafana for the RLaS demo.
#
# Why this exists: since the PostgreSQL datasource was split out of core
# (12.x/13.x era), the official grafana/grafana images ship its frontend as an
# *unbuilt source tree* (`"version": "%VERSION%"`, no gpx_ backend binary), so
# every TimescaleDB query fails with "Plugin not registered". Pinning older
# images (11.5.x, 11.6.x) does not help — their later patch builds have the
# same defect. The released plugin is only available from the grafana.com
# catalog, so we install it here, baked into the image, rather than relying on
# a runtime download.
FROM grafana/grafana:latest

# Install the released PostgreSQL datasource plugin (needs Grafana >= 12.3,
# satisfied by the :latest base above). Grafana 13 removed the standalone
# `grafana-cli`; the CLI is now `grafana cli`. The base image runs as uid 472,
# so create the plugins dir as root first, then install as the grafana user.
#
# Default plugins dir (/var/lib/grafana/plugins) is overlain by the
# compose/deploy grafana-data volume at runtime, so install into the image
# filesystem and point GF_PLUGINS_DIR there. The grafana user (uid 472) only
# needs to read it.
USER root
RUN mkdir -p /usr/share/grafana/plugins && chown 472:472 /usr/share/grafana/plugins
# The official image bundles the postgres datasource frontend as an *unbuilt
# source tree* under public/app; it would shadow the real plugin as a
# "duplicate" and Grafana would skip loading our backend-enabled copy. Remove
# it so the catalog plugin below is the only registrant.
RUN rm -rf /usr/share/grafana/public/app/plugins/datasource/grafana-postgresql-datasource
USER 472
RUN /usr/share/grafana/bin/grafana cli --pluginsDir /usr/share/grafana/plugins plugins install grafana-postgresql-datasource 13.0.1

ENV GF_PLUGINS_DIR=/usr/share/grafana/plugins
