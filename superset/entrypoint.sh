#!/usr/bin/env bash
set -e

: "${ADMIN_USER:=admin}"
: "${ADMIN_PASSWORD:=admin}"
: "${ADMIN_EMAIL:=admin@example.com}"

mkdir -p /app/superset_home
export SUPERSET_HOME=/app/superset_home

echo ">>> Upgrading Superset metadata DB..."
superset db upgrade

echo ">>> Creating admin user ($ADMIN_USER) if missing..."
superset fab create-admin \
    --username "$ADMIN_USER" \
    --firstname Admin \
    --lastname User \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" || true

echo ">>> Running superset init..."
superset init

echo ""
echo "============================================================"
echo " Superset is starting. Login: $ADMIN_USER / $ADMIN_PASSWORD"
echo ""
echo " Next steps:"
echo "   1. Settings -> Database Connections -> + DATABASE"
echo "      Choose Trino. Example SQLAlchemy URI:"
echo "      trino://<user>:<password>@<host>:443/<catalog>"
echo "   2. Build dashboards manually (see superset/sql/ for prep queries)."
echo "============================================================"
echo ""

echo ">>> Starting Superset webserver on :8088..."
exec gunicorn \
    --bind "0.0.0.0:8088" \
    --access-logfile - \
    --error-logfile - \
    --workers 2 \
    --worker-class gthread \
    --threads 20 \
    --timeout 120 \
    "superset.app:create_app()"
