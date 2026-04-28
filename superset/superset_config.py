import os

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "CHANGE_ME_TO_A_LONG_RANDOM_STRING_IN_PRODUCTION",
)

# Use a local SQLite metadata DB by default (fine for single-user demo).
# For production override SQLALCHEMY_DATABASE_URI via env.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db",
)

# Disable CSRF on API endpoints used by chart queries (Trino long-running ok).
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log"]

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "EMBEDDED_SUPERSET": False,
}

# Dashboard refresh is driven by the dashboard metadata itself (30s for this project).
SUPERSET_WEBSERVER_TIMEOUT = 120
